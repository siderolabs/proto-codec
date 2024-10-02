// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package codec_test

import (
	"strings"
	"testing"

	vtwrapperspb "github.com/planetscale/vtprotobuf/types/known/wrapperspb"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func testProtobuf[T any](t testing.TB, what *T, check func(testing.TB, *T)) {
	c := encoding.GetCodecV2("proto")

	out, err := c.Marshal(what)
	if err != nil {
		t.Fatal(err)
	}

	defer out.Free()

	var result T

	err = c.Unmarshal(out, &result)
	if err != nil {
		t.Fatal(err)
	}

	check(t, &result)
}

type testData struct {
	length int
	allocs int64
}

func TestProtobuf(t *testing.T) {
	tests := map[string]testData{
		"short string": {
			length: 42,
			allocs: 5,
		},
		"long string": {
			length: 10240,
			allocs: allocsCount,
		},
	}

	for name, d := range tests {
		if !t.Run(name, func(t *testing.T) {
			str := generateString(d.length)
			value := wrapperspb.String(str)
			c := check(str)

			testProtobuf(t, value, c)

			res := testing.Benchmark(benchmarkProtobuf(func(t testing.TB) { testProtobuf(t, value, c) }))

			if allocs := res.AllocsPerOp(); d.allocs != allocs {
				t.Fatalf("unexpected number of allocations: expected %d != actual %d", d.allocs, allocs)
			}
		}) {
			break
		}
	}
}

func BenchmarkProtobuf(b *testing.B) {
	str := generateString(42)
	value := wrapperspb.String(str)
	c := check(str)

	benchmarkProtobuf(func(t testing.TB) { testProtobuf(t, value, c) })(b)
}

func check(expected string) func(t testing.TB, what *wrapperspb.StringValue) {
	return func(t testing.TB, what *wrapperspb.StringValue) {
		if expected != what.GetValue() {
			t.Fatal("strings are not equal", expected, what.GetValue())
		}
	}
}

func TestVTProtobuf(t *testing.T) {
	tests := map[string]testData{
		"short string": {
			length: 42,
			allocs: 5,
		},
		"long string": {
			length: 10240,
			allocs: allocsCount,
		},
	}

	for name, d := range tests {
		if !t.Run(name, func(t *testing.T) {
			str := generateString(d.length)
			value := (*vtwrapperspb.StringValue)(wrapperspb.String(str))
			c := checkVT(str)

			testProtobuf(t, value, c)

			res := testing.Benchmark(benchmarkProtobuf(func(t testing.TB) { testProtobuf(t, value, c) }))

			if allocs := res.AllocsPerOp(); d.allocs != allocs {
				t.Fatalf("unexpected number of allocations: expected %d != actual %d", d.allocs, allocs)
			}
		}) {
			break
		}
	}
}

func BenchmarkVTProtobuf(b *testing.B) {
	str := generateString(10240)
	value := (*vtwrapperspb.StringValue)(wrapperspb.String(str))
	c := checkVT(str)

	benchmarkProtobuf(func(t testing.TB) { testProtobuf(t, value, c) })(b)
}

func checkVT(expected string) func(t testing.TB, what *vtwrapperspb.StringValue) {
	return func(t testing.TB, what *vtwrapperspb.StringValue) {
		if expected != what.Value {
			t.Fatal("strings are not equal", expected, what.Value)
		}
	}
}

func TestOldProtobuf(t *testing.T) {
	tests := map[string]testData{
		"short string": {
			length: 42,
			allocs: 8,
		},
		"long string": {
			length: 10240,
			allocs: 10,
		},
	}

	for name, d := range tests {
		if !t.Run(name, func(t *testing.T) {
			str := generateString(d.length)
			value := wrapperspb.String(str)
			c := check(str)

			testProtobufOld(t, value, c)

			res := testing.Benchmark(benchmarkProtobuf(func(t testing.TB) { testProtobufOld(t, value, c) }))

			if allocs := res.AllocsPerOp(); d.allocs != allocs {
				t.Fatalf("unexpected number of allocations: expected %d != actual %d", d.allocs, allocs)
			}
		}) {
			break
		}
	}
}

func BenchmarkOldProtobuf(b *testing.B) {
	str := generateString(42)
	value := wrapperspb.String(str)
	c := check(str)

	benchmarkProtobuf(func(t testing.TB) { testProtobufOld(t, value, c) })(b)
}

func testProtobufOld(t testing.TB, what *wrapperspb.StringValue, check func(testing.TB, *wrapperspb.StringValue)) {
	c := encoding.GetCodecV2("proto")

	out, err := c.Marshal((*oldProto)(what))
	if err != nil {
		t.Fatal(err)
	}

	rssult := &wrapperspb.StringValue{}

	err = c.Unmarshal(out, (*oldProto)(rssult))
	if err != nil {
		t.Fatal(err)
	}

	check(t, rssult)
}

// Because all protobuf types provide V2 version of methods, we need to manually cast it down to V1.
type oldProto wrapperspb.StringValue

func (x *oldProto) Reset() { (*wrapperspb.StringValue)(x).Reset() }

func (x *oldProto) String() string { return messageString((*wrapperspb.StringValue)(x)) }
func (*oldProto) ProtoMessage()    {}
func messageString(m protoreflect.ProtoMessage) string {
	return protoimpl.X.MessageStringOf(m)
}

func generateString(length int) string {
	var builder strings.Builder

	for i := range length {
		builder.WriteRune(rune('a' + i%26))
	}

	return builder.String()
}

func benchmarkProtobuf(fn func(t testing.TB)) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			fn(b)
		}
	}
}

func TestGoGoProtobuf(t *testing.T) {
	tests := map[string]testData{
		"short string": {
			length: 42,
			allocs: 5,
		},
		"long string": {
			length: 10240,
			allocs: allocsCount,
		},
	}

	for name, d := range tests {
		if !t.Run(name, func(t *testing.T) {
			str := generateString(d.length)
			value := (*gogoProto)(wrapperspb.String(str))
			c := checkGogo(str)

			testProtobuf(t, value, c)

			res := testing.Benchmark(benchmarkProtobuf(func(t testing.TB) { testProtobuf(t, value, c) }))

			if allocs := res.AllocsPerOp(); d.allocs != allocs {
				t.Fatalf("unexpected number of allocations: expected %d != actual %d", d.allocs, allocs)
			}
		}) {
			break
		}
	}
}

func BenchmarkGoGoProtobuf(b *testing.B) {
	str := generateString(10240)
	value := (*gogoProto)(wrapperspb.String(str))
	c := checkGogo(str)

	benchmarkProtobuf(func(t testing.TB) { testProtobuf(t, value, c) })(b)
}

func checkGogo(expected string) func(t testing.TB, what *gogoProto) {
	return func(t testing.TB, what *gogoProto) {
		if expected != what.Value {
			t.Fatal("strings are not equal", expected, what.Value)
		}
	}
}

// Let's pretend our vt proto is actually gogo proto.
type gogoProto vtwrapperspb.StringValue

func (x *gogoProto) MarshalToSizedBuffer(b []byte) (int, error) {
	return (*vtwrapperspb.StringValue)(x).MarshalToSizedBufferVT(b)
}

func (x *gogoProto) Marshal() ([]byte, error) {
	return (*vtwrapperspb.StringValue)(x).MarshalVT()
}

func (x *gogoProto) Unmarshal(dest []byte) error {
	return (*vtwrapperspb.StringValue)(x).UnmarshalVT(dest)
}

func (x *gogoProto) Size() int      { return (*vtwrapperspb.StringValue)(x).SizeVT() }
func (x *gogoProto) Reset()         { (*wrapperspb.StringValue)(x).Reset() }
func (x *gogoProto) String() string { return messageString((*wrapperspb.StringValue)(x)) }
func (*gogoProto) ProtoMessage()    {}
