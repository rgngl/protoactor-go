package cluster

import (
	"fmt"
	"time"

	"github.com/asynkron/gofun/set"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/extensions"
	"github.com/asynkron/protoactor-go/log"
	"github.com/asynkron/protoactor-go/remote"
)

var extensionID = extensions.NextExtensionID()

type Cluster struct {
	ActorSystem    *actor.ActorSystem
	Config         *Config
	Gossip         *Gossiper
	Remote         *remote.Remote
	PidCache       *PidCacheValue
	MemberList     *MemberList
	IdentityLookup IdentityLookup
	kinds          map[string]*ActivatedKind
	context        Context
}

var _ extensions.Extension = &Cluster{}

func New(actorSystem *actor.ActorSystem, config *Config) *Cluster {
	c := &Cluster{
		ActorSystem: actorSystem,
		Config:      config,
		kinds:       map[string]*ActivatedKind{},
	}
	actorSystem.Extensions.Register(c)

	c.context = config.ClusterContextProducer(c)
	c.PidCache = NewPidCache()
	c.MemberList = NewMemberList(c)
	c.subscribeToTopologyEvents()

	actorSystem.Extensions.Register(c)

	var err error
	c.Gossip, err = newGossiper(c)

	if err != nil {
		panic(err)
	}

	return c
}

func (c *Cluster) subscribeToTopologyEvents() {
	c.ActorSystem.EventStream.Subscribe(func(evt interface{}) {
		if clusterTopology, ok := evt.(*ClusterTopology); ok {
			for _, member := range clusterTopology.Left {
				c.PidCache.RemoveByMember(member)
			}
		}
	})
}

func (c *Cluster) ExtensionID() extensions.ExtensionID {
	return extensionID
}

//goland:noinspection GoUnusedExportedFunction
func GetCluster(actorSystem *actor.ActorSystem) *Cluster {
	c := actorSystem.Extensions.Get(extensionID)

	return c.(*Cluster)
}

func (c *Cluster) GetBlockedMembers() set.Set[string] {
	return c.Remote.BlockList().BlockedMembers()
}

func (c *Cluster) StartMember() {
	cfg := c.Config
	c.Remote = remote.NewRemote(c.ActorSystem, c.Config.RemoteConfig)

	c.initKinds()

	// TODO: make it possible to become a cluster even if remoting is already started
	c.Remote.Start()

	address := c.ActorSystem.Address()
	plog.Info("Starting Proto.Actor cluster member", log.String("id", c.ActorSystem.ID), log.String("address", address))

	c.IdentityLookup = cfg.IdentityLookup
	c.IdentityLookup.Setup(c, c.GetClusterKinds(), false)

	// TODO: Disable Gossip for now until API changes are done
	// gossiper must be started whenever any topology events starts flowing
	if err := c.Gossip.StartGossiping(); err != nil {
		panic(err)
	}
	c.MemberList.InitializeTopologyConsensus()

	if err := cfg.ClusterProvider.StartMember(c); err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)
}

func (c *Cluster) GetClusterKinds() []string {
	keys := make([]string, 0, len(c.kinds))
	for k := range c.kinds {
		keys = append(keys, k)
	}

	return keys
}

func (c *Cluster) StartClient() {
	cfg := c.Config
	c.Remote = remote.NewRemote(c.ActorSystem, c.Config.RemoteConfig)

	c.Remote.Start()

	address := c.ActorSystem.Address()
	plog.Info("Starting Proto.Actor cluster-client", log.String("address", address))

	c.IdentityLookup = cfg.IdentityLookup
	c.IdentityLookup.Setup(c, c.GetClusterKinds(), true)

	if err := cfg.ClusterProvider.StartClient(c); err != nil {
		panic(err)
	}
}

func (c *Cluster) Shutdown(graceful bool) {
	c.ActorSystem.Shutdown()

	if graceful {
		_ = c.Config.ClusterProvider.Shutdown(graceful)
		c.IdentityLookup.Shutdown()
		// This is to wait ownership transferring complete.
		time.Sleep(time.Millisecond * 2000)
		c.MemberList.stopMemberList()
		c.Gossip.Shutdown()
	}

	c.Remote.Shutdown(graceful)

	address := c.ActorSystem.Address()
	plog.Info("Stopped Proto.Actor cluster", log.String("address", address))
}

func (c *Cluster) Get(identity string, kind string) *actor.PID {
	return c.IdentityLookup.Get(NewClusterIdentity(identity, kind))
}

func (c *Cluster) Request(identity string, kind string, message interface{}) (interface{}, error) {
	return c.context.Request(identity, kind, message)
}

func (c *Cluster) GetClusterKind(kind string) *ActivatedKind {
	k, ok := c.kinds[kind]
	if !ok {
		plog.Error("Invalid kind", log.String("kind", kind))

		return nil
	}

	return k
}

func (c *Cluster) TryGetClusterKind(kind string) (*ActivatedKind, bool) {
	k, ok := c.kinds[kind]

	return k, ok
}

func (c *Cluster) initKinds() {
	for name, kind := range c.Config.Kinds {
		c.kinds[name] = kind.Build(c)
	}
}

// Call is a wrap of context.RequestFuture with retries.
func (c *Cluster) Call(name string, kind string, msg interface{}, opts ...GrainCallOption) (interface{}, error) {
	callConfig := DefaultGrainCallConfig(c)
	for _, o := range opts {
		o(callConfig)
	}

	_context := callConfig.Context
	if _context == nil {
		_context = c.ActorSystem.Root
	}

	var lastError error

	for i := 0; i < callConfig.RetryCount; i++ {
		pid := c.Get(name, kind)

		if pid == nil {
			return nil, remote.ErrUnknownError
		}

		timeout := callConfig.Timeout
		_resp, err := _context.RequestFuture(pid, msg, timeout).Result()
		if err != nil {
			var msgStr string
			if pm, ok := msg.(proto.Message); ok {
				msgStr = protojson.Format(pm)
			}

			plog.Error("cluster.RequestFuture failed", log.Error(err), log.PID("pid", pid),
				log.String("name", name), log.String("kind", kind),
				log.PID("sender", _context.Sender()),
				log.String("msg_str", msgStr), log.String("msg_type", fmt.Sprintf("%T", msg)),
			)
			lastError = err

			switch err {
			case actor.ErrTimeout, remote.ErrTimeout:
				callConfig.RetryAction(i)

				id := ClusterIdentity{Kind: kind, Identity: name}
				c.PidCache.Remove(id.Identity, id.Kind)

				continue
			case actor.ErrDeadLetter, remote.ErrDeadLetter:
				callConfig.RetryAction(i)

				id := ClusterIdentity{Kind: kind, Identity: name}
				c.PidCache.Remove(id.Identity, id.Kind)

				continue
			default:
				return nil, err
			}
		}

		return _resp, nil
	}

	return nil, lastError
}
