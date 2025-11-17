package util

import (
	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// DeepCopy 深拷贝
func DeepCopy[T comparable, A any, M ~map[T]A | ~[]map[T]A](m M) M {
	sm, err := sonic.MarshalString(m)
	if err != nil {
		return nil
	}
	var rm M
	err = sonic.UnmarshalString(sm, &rm)
	if err != nil {
		return nil
	}
	return rm
}

func MapToAnyPb(m map[string]interface{}) (*anypb.Any, error) {
	// 1. 将 map[string]interface{} 转换为 structpb.Struct
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil, err
	}
	// 2. 将 structpb.Struct 转换为 anypb.Any
	anyValue, err := anypb.New(s)
	if err != nil {
		return nil, err
	}
	return anyValue, nil
}
