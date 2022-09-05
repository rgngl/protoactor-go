package remote

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var (
	ErrNotProtoMessage = errors.New("msg is not proto.Message")
)

type protoSerializer struct{}

func newProtoSerializer() *protoSerializer {
	return &protoSerializer{}
}

func (p *protoSerializer) Serialize(msg interface{}) ([]byte, error) {
	if message, ok := msg.(proto.Message); ok {
		bytes, err := proto.Marshal(message)
		if err != nil {
			return nil, err
		}

		return bytes, nil
	}
	return nil, fmt.Errorf("%w: %T", ErrNotProtoMessage, msg)
}

func (p *protoSerializer) Deserialize(typeName string, bytes []byte) (interface{}, error) {
	n, _ := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeName))

	pm := n.New().Interface()

	err := proto.Unmarshal(bytes, pm)
	return pm, err
}

func (protoSerializer) GetTypeName(msg interface{}) (string, error) {
	if message, ok := msg.(proto.Message); ok {
		typeName := proto.MessageName(message)

		return string(typeName), nil
	}
	return "", fmt.Errorf("%w: %T", ErrNotProtoMessage, msg)
}
