// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package codec provides a codec for gRPC that uses the vtproto encoding.
package codec

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

type vtprotoCodec struct{}

func (vtprotoCodec) Marshal(v any) (mem.BufferSlice, error) {
	size, err := getSize(v)
	if err != nil {
		return nil, err
	}

	if mem.IsBelowBufferPoolingThreshold(size) {
		buf, err := marshal(v)
		if err != nil {
			return nil, err
		}

		return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
	}

	pool := mem.DefaultBufferPool()

	buf := pool.Get(size)
	if err := marshalAppend(*buf, v); err != nil {
		pool.Put(buf)

		return nil, err
	}

	return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil
}

func getSize(v any) (int, error) {
	switch v := v.(type) {
	case vtprotoMessage:
		return v.SizeVT(), nil
	case gproto.Message:
		return gproto.Size(v), nil
	case protoadapt.MessageV1:
		return gproto.Size(protoadapt.MessageV2Of(v)), nil
	default:
		return -1, fmt.Errorf("failed to get size, message is %T, must satisfy the vtprotoMessage, proto.Message or protoadapt.MessageV1 ", v)
	}
}

func marshal(v any) ([]byte, error) {
	switch v := v.(type) {
	case vtprotoMessage:
		return v.MarshalVT()
	case gproto.Message:
		return gproto.Marshal(v)
	case protoadapt.MessageV1:
		return gproto.Marshal(protoadapt.MessageV2Of(v))
	default:
		return nil, fmt.Errorf("failed to marshal, message is %T, must satisfy the vtprotoMessage, proto.Message or protoadapt.MessageV1 ", v)
	}
}

func marshalAppend(dst []byte, v any) error {
	takeErr := func(_ any, e error) error { return e }

	switch v := v.(type) {
	case vtprotoMessage:
		return takeErr(v.MarshalToSizedBufferVT(dst))
	case gproto.Message:
		return takeErr((gproto.MarshalOptions{}).MarshalAppend(dst[:0], v))
	case protoadapt.MessageV1:
		return takeErr((gproto.MarshalOptions{}).MarshalAppend(dst[:0], protoadapt.MessageV2Of(v)))
	default:
		return fmt.Errorf("failed to marshal-append, message is %T, must satisfy the vtprotoMessage, proto.Message or protoadapt.MessageV1 ", v)
	}
}

func (vtprotoCodec) Unmarshal(data mem.BufferSlice, v any) error {
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buf.Free()

	switch v := v.(type) {
	case vtprotoMessage:
		return v.UnmarshalVT(buf.ReadOnlyData())
	case gproto.Message:
		return gproto.Unmarshal(buf.ReadOnlyData(), v)
	case protoadapt.MessageV1:
		return gproto.Unmarshal(buf.ReadOnlyData(), protoadapt.MessageV2Of(v))
	default:
		return fmt.Errorf("failed to unmarshal, message is %T, must satisfy the vtprotoMessage, proto.Message or protoadapt.MessageV1", v)
	}
}

func (vtprotoCodec) Name() string { return proto.Name }

type vtprotoMessage interface {
	MarshalToSizedBufferVT([]byte) (int, error)
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
	SizeVT() int
}

func init() { encoding.RegisterCodecV2(vtprotoCodec{}) }
