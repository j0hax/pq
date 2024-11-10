package persistent

import (
	"reflect"
	"testing"
)

func TestRingBuffer_AddAndGet(t *testing.T) {
	ringBuffer := New[int](5, "/tmp/ringtest.prb")
	ringBuffer.Enqueue(0)
	ringBuffer.Enqueue(1, 2)
	ringBuffer.Enqueue(3, 4)

	for i := range 5 {
		r := ringBuffer.Dequeue()
		if i != r {
			t.Errorf("Expected %d, got %d\n", i, r)
		}
	}

	ringBuffer.Enqueue(7)
	ringBuffer.Enqueue(8)

	results := make([]int, 0, 2)
	expect := []int{7, 8}

	for range 2 {
		results = append(results, ringBuffer.Dequeue())
	}

	if !reflect.DeepEqual(results, expect) {
		t.Errorf("Expected %v, got %v\n", expect, results)
	}
}

func Test_Save(t *testing.T) {
	ringBuffer := New[int](5, "/tmp/ringtest.prb")
	ringBuffer.Enqueue(0)
	ringBuffer.Enqueue(1, 2)
	ringBuffer.Enqueue(3, 4)

	err := ringBuffer.Save()
	if err != nil {
		t.Error(err)
	}

	// Test restore
	ringBuffer, err = Open[int]("/tmp/ringtest.prb")
	if err != nil {
		t.Error(err)
	}
}
