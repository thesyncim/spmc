// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spmc

import (
	"sync/atomic"
	"unsafe"
)

// poolDequeue is a lock-free fixed-size single-producer,
// multi-consumer queue. The single producer can both push and pop
// from the head, and consumers can pop from the tail.
//
// It has the added feature that it nils out unused slots to avoid
// unnecessary retention of objects. This is important for sync.Pool,
// but not typically a property considered in the literature.
type poolDequeue[T any] struct {
	// headGenTail packs together a 21-bit head index, a 21-bit
	// ABA guard for the head, and a 21-bit tail index.
	//
	// The ABA guard is necessary because head is non-monotonic.
	// Without this, a popHead followed by a pushHead would leave
	// this as the same value, so a popTail could falsely think
	// that the head didn't change.
	//
	// The head index is stored in the most-significant bits so
	// that we can atomically add to it and the overflow is
	// harmless.
	headGenTail uint64

	// vals stores the interface{} values of this dequeue. The
	// size of this must be a power of 2.
	//
	// vals[i].typ is nil if the slot is empty and non-nil
	// otherwise. A slot is still in use until *both* the tail
	// index has moved beyond it and typ has been set to nil. This
	// is set to nil atomically by the consumer and read
	// atomically by the producer.
	vals []*T
}

const dequeueBits = 21

// dequeueLimit is the maximum size of a poolDequeue.
//
// This is half of 1<<dequeueBits because detecting fullness depends
// on wrapping around the ring buffer without wrapping around the
// index.
const dequeueLimit = (1 << dequeueBits) / 2

// dequeueNil is used in poolDeqeue to represent interface{}(nil).
// Since we use nil to represent empty slots, we need a sentinel value
// to represent nil.
type dequeueNil *struct{}

func (d *poolDequeue[T]) unpack(ptrs uint64) (head, gen, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32(ptrs >> (2 * dequeueBits) & mask)
	gen = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (d *poolDequeue[T]) pack(head, gen, tail uint32) uint64 {
	const mask = 1<<dequeueBits - 1
	return (uint64(head) << (2 * dequeueBits)) |
		(uint64(gen&mask) << dequeueBits) |
		uint64(tail&mask)
}

// pushHead adds val at the head of the queue. It returns false if the
// queue is full. It must only be called by a single producer.
func (d *poolDequeue[T]) pushHead(val T) bool {
	ptrs := atomic.LoadUint64(&d.headGenTail)
	head, _, tail := d.unpack(ptrs)
	if tail+uint32(len(d.vals)) == head {
		// Queue is full.
		return false
	}
	slot := &d.vals[head&uint32(len(d.vals)-1)]

	// Check if the head slot has been released by popTail.

	typ := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(slot)))
	if typ != nil {
		// Another goroutine is still cleaning up the tail, so
		// the queue is actually still full.
		return false
	}

	/*// The head slot is free, so we own it.
	if val == T(nil) {
		//val = dequeueNil(nil)
	}*/
	*slot = &val

	// Increment head. This passes ownership of slot to popTail.
	// We don't increment gen here; only one of pushHead and
	// popHead needs to do that and its more convenient to do in
	// popHead.
	atomic.AddUint64(&d.headGenTail, 1<<(2*dequeueBits))
	return true
}

// popHead removes and returns the element at the head of the queue.
// It returns false if the queue is empty. It must only be called by a
// single producer.
func (d *poolDequeue[T]) popHead() (T, bool) {
	var slot **T
	for {
		ptrs := atomic.LoadUint64(&d.headGenTail)
		head, gen, tail := d.unpack(ptrs)
		if tail == head {
			// Queue is empty.
			return zero[T](), false
		}

		// Confirm tail and decrement head. We do this before
		// reading the value to take back ownership of this
		// slot.
		head--
		ptrs2 := d.pack(head, gen+1, tail)
		if atomic.CompareAndSwapUint64(&d.headGenTail, ptrs, ptrs2) {
			// We successfully took back slot.
			slot = &d.vals[head&uint32(len(d.vals)-1)]
			break
		}
	}

	val := *slot
	// Zero the slot. Unlike popTail, this isn't racing with
	// pushHead, so we don't need to be careful here.
	*slot = nil
	return *val, true
}

// popTail removes and returns the element at the tail of the queue.
// It returns false if the queue is empty. It may be called by any
// number of consumers.
func (d *poolDequeue[T]) popTail() (T, bool) {
	var slot **T
	for {
		ptrs := atomic.LoadUint64(&d.headGenTail)
		head, gen, tail := d.unpack(ptrs)
		if tail == head {
			// Queue is empty.
			return zero[T](), false
		}

		// Increment tail and check that head hasn't changed
		// by confirming both head and gen. If this succeeds,
		// then we own the slot at tail.
		ptrs2 := d.pack(head, gen, tail+1)
		if atomic.CompareAndSwapUint64(&d.headGenTail, ptrs, ptrs2) {
			// Success.
			slot = &d.vals[tail&uint32(len(d.vals)-1)]
			break
		}
	}

	// We now own slot.
	val := *slot

	// Tell pushHead that we're done with this slot. Zeroing the
	// slot is also important so we don't leave behind references
	// that could keep this object live longer than necessary.
	//
	// We write to val first and then publish that we're done with
	// this slot by atomically writing to typ.

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(slot)), nil)
	// At this point pushHead owns the slot.

	return *val, true
}

func unsafeAddr[T any](addr *T) *unsafe.Pointer {
	p := unsafe.Pointer(addr)
	return &p
}

// poolChain is a dynamically-sized version of poolDequeue.
//
// This is implemented as a doubly-linked list queue of poolDequeues
// where each dequeue is double the size of the previous one. Once a
// dequeue fills up, this allocates a new one and only ever pushes to
// the latest dequeue. Pops happen from the other end of the list and
// once a dequeue is exhausted, it gets removed from the list.
type poolChain[T any] struct {
	// head is the poolDequeue to push to. This is only accessed
	// by the producer, so doesn't need to be synchronized.
	head *poolChainElt[T]

	// tail is the poolDequeue to popTail from. This is accessed
	// by consumers, so reads and writes must be atomic.
	tail *poolChainElt[T]
}

type poolChainElt[T any] struct {
	poolDequeue[T]

	// next and prev link to the adjacent poolChainElts in this
	// poolChain.
	//
	// next is written atomically by the producer and read
	// atomically by the consumer. It only transitions from nil to
	// non-nil.
	//
	// prev is written atomically by the consumer and read
	// atomically by the producer. It only transitions from
	// non-nil to nil.
	next, prev *poolChainElt[T]
}

func storePoolChainElt[T any](pp **poolChainElt[T], v *poolChainElt[T]) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(pp)), unsafe.Pointer(v))
}

func loadPoolChainElt[T any](pp **poolChainElt[T]) *poolChainElt[T] {
	return (*poolChainElt[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(pp))))
}

func (c *poolChain[T]) pushHead(val T) {
	d := c.head
	if d == nil {
		// Initialize the chain.
		const initSize = 8 // Must be a power of 2
		d = new(poolChainElt[T])
		d.vals = make([]*T, initSize)
		c.head = d
		storePoolChainElt(&c.tail, d)
	}

	if d.pushHead(val) {
		return
	}

	// The current dequeue is full. Allocate a new one of twice
	// the size.
	newSize := len(d.vals) * 2
	if newSize >= dequeueLimit {
		// Can't make it any bigger.
		newSize = dequeueLimit
	}

	d2 := &poolChainElt[T]{prev: d}
	d2.vals = make([]*T, newSize)
	c.head = d2
	storePoolChainElt(&d.next, d2)
	d2.pushHead(val)
}

func zero[T any]() T {
	var x T
	return x
}

func (c *poolChain[T]) popHead() (T, bool) {
	d := c.head
	for d != nil {
		if val, ok := d.popHead(); ok {
			return val, ok
		}
		// There may still be unconsumed elements in the
		// previous dequeue, so try backing up.
		d = loadPoolChainElt[T](&d.prev)
	}
	return zero[T](), false
}

func (c *poolChain[T]) popTail() (T, bool) {
	d := loadPoolChainElt[T](&c.tail)
	if d == nil {
		return zero[T](), false
	}

	for {
		// It's important that we load the next pointer
		// *before* popping the tail. In general, d may be
		// transiently empty, but if next is non-nil before
		// the pop and the pop fails, then d is permanently
		// empty, which is the only condition under which it's
		// safe to drop d from the chain.
		d2 := loadPoolChainElt[T](&d.next)

		if val, ok := d.popTail(); ok {
			return val, ok
		}

		if d2 == nil {
			// This is the only dequeue. It's empty right
			// now, but could be pushed to in the future.
			return zero[T](), false
		}

		// The tail of the chain has been drained, so move on
		// to the next dequeue. Try to drop it from the chain
		// so the next pop doesn't have to look at the empty
		// dequeue again.
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&c.tail)), unsafe.Pointer(d), unsafe.Pointer(d2)) {
			// We won the race. Clear the prev pointer so
			// the garbage collector can collect the empty
			// dequeue and so popHead doesn't back up
			// further than necessary.
			storePoolChainElt[T](&d2.prev, nil)
		}
		d = d2
	}
}

func NewPoolChain[T any]() PoolDequeue[T] {
	return new(poolChain[T])
}

func (c *poolChain[T]) PushHead(val T) bool {
	c.pushHead(val)
	return true
}

func (c *poolChain[T]) PopHead() (T, bool) {
	return c.popHead()
}

func (c *poolChain[T]) PopTail() (T, bool) {
	return c.popTail()
}

// poolDequeue testing.
type PoolDequeue[T any] interface {
	PushHead(val T) bool
	PopHead() (T, bool)
	PopTail() (T, bool)
}

func NewPoolDequeue[T any](n int) PoolDequeue[T] {
	return &poolDequeue[T]{
		vals: make([]*T, n),
	}
}

func (d *poolDequeue[T]) PushHead(val T) bool {
	return d.pushHead(val)
}

func (d *poolDequeue[T]) PopHead() (T, bool) {
	return d.popHead()
}

func (d *poolDequeue[T]) PopTail() (T, bool) {
	return d.popTail()
}
