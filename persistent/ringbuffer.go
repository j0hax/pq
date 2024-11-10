package persistent

import "sync"
import "encoding/gob"
import "os"
import "compress/gzip"

// RingBuffer is a circular buffer which can be saved to disk.
type RingBuffer[T any] struct {
	Filename string
	mutex    sync.RWMutex
	size     int
	data     *bufferState[T]
}

// bufferState contains the actual interal buffer data which must be persisted.
type bufferState[T any] struct {
	Contents   []T
	ReadIndex  int
	WriteIndex int
}

func New[T any](capacity int, persistencePath string) *RingBuffer[T] {
	return &RingBuffer[T]{
		Filename: persistencePath,
		size:     capacity,
		data: &bufferState[T]{
			Contents: make([]T, capacity),
		},
	}
}

// Save serializes the slice of objects in the Queue.
func (q *RingBuffer[T]) Save() error {
	fi, err := os.Create(q.Filename)
	if err != nil {
		return err
	}
	defer fi.Close()

	fz := gzip.NewWriter(fi)
	defer fz.Close()

	encoder := gob.NewEncoder(fz)

	q.mutex.RLock()
	defer q.mutex.RUnlock()

	err = encoder.Encode(q.data)

	return err
}

// Open creates a persistent queue file
func Open[T any](path string) (*RingBuffer[T], error) {
	// TODO: handle errors that arise if the file does not exist.
	fi, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	fz, err := gzip.NewReader(fi)
	if err != nil {
		return nil, err
	}
	defer fz.Close()

	decoder := gob.NewDecoder(fz)

	q := &RingBuffer[T]{}

	err = decoder.Decode(&q.data)
	if err != nil {
		return nil, err
	}

	q.size = len(q.data.Contents)

	return q, err
}

// increment adds position + 1 % q.size.
func (q *RingBuffer[T]) increment(position *int) {
	*position = (*position + 1) % q.size
}

// Enqueue adds items to the Queue
func (q *RingBuffer[T]) Enqueue(items ...T) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Iteravely add each item passed
	for _, item := range items {
		q.data.Contents[q.data.WriteIndex] = item
		q.increment(&q.data.WriteIndex)
	}

	go q.Save()
}

// Dequeue removes one item from the Queue
func (q *RingBuffer[T]) Dequeue() T {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	// Grab the item from the index, then update the index
	temp := q.data.Contents[q.data.ReadIndex]
	q.increment(&q.data.ReadIndex)
	go q.Save()
	return temp
}

// Peek returns a copy of the next item without dequeuing it.
func (q *RingBuffer[T]) Peek() T {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.data.Contents[q.data.ReadIndex]
}
