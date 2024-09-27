// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package codec_test

import (
	"testing"
	"time"

	vtimestamppb "github.com/planetscale/vtprotobuf/types/known/timestamppb"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtobuf(t *testing.T) {
	expected := time.Now().UTC()

	testProtobuf(t, timestamppb.New(expected), check(expected))
	res := testing.Benchmark(BenchmarkProtobuf)

	if allocs := res.AllocsPerOp(); allocs != 4 {
		t.Fatalf("unexpected number of allocations: %d", allocs)
	}
}

func BenchmarkProtobuf(b *testing.B) {
	expected := time.Now().UTC()
	ts := timestamppb.New(expected)
	c := check(expected)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		testProtobuf(b, ts, c)
	}
}

func check(expected time.Time) func(t testing.TB, what *timestamppb.Timestamp) {
	return func(t testing.TB, what *timestamppb.Timestamp) {
		if !expected.Equal(what.AsTime()) {
			t.Fatal("timestamps are not equal")
		}
	}
}

func TestVTProtobuf(t *testing.T) {
	expected := time.Now().UTC()

	testProtobuf(t, (*vtimestamppb.Timestamp)(timestamppb.New(expected)), checkVT(expected))

	res := testing.Benchmark(BenchmarkVTProtobuf)

	if allocs := res.AllocsPerOp(); allocs != 4 {
		t.Fatalf("unexpected number of allocations: %d", allocs)
	}
}

func BenchmarkVTProtobuf(b *testing.B) {
	expected := time.Now().UTC()
	ts := (*vtimestamppb.Timestamp)(timestamppb.New(expected))
	c := checkVT(expected)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		testProtobuf(b, ts, c)
	}
}

func checkVT(expected time.Time) func(t testing.TB, what *vtimestamppb.Timestamp) {
	return func(t testing.TB, what *vtimestamppb.Timestamp) {
		if !expected.Equal((*timestamppb.Timestamp)(what).AsTime()) {
			t.Fatal("timestamps are not equal", expected, (*timestamppb.Timestamp)(what).AsTime())
		}
	}
}

func testProtobuf[T any](t testing.TB, what *T, check func(testing.TB, *T)) {
	c := encoding.GetCodecV2("proto")

	out, err := c.Marshal(what)
	if err != nil {
		t.Fatal(err)
	}

	var result T

	err = c.Unmarshal(out, &result)
	if err != nil {
		t.Fatal(err)
	}

	check(t, &result)
}
