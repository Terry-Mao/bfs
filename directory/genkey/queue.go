package genkey

import "container/list"

// thread safe queue
type Queue struct {
        sem chan int
        list *list.List
}

// NewQueue
func NewQueue() *Queue {
        sem := make(chan int, 1)
        list := list.New()
        return &Queue{sem, list}
}

// Size size of queue
func(q *Queue) Size() int {
        return q.list.Len()
}

// Put new element into queue
func (q *Queue) Push(val int64) int64 {
        q.sem <- 1
        e := q.list.PushFront(val)
        <-q.sem
        return e
}

// Get a element from queue
func (q *Queue) Pop() int64 {
        q.sem <-1
        e := q.list.Back()
        q.list.Remove(e)
        <-q.sem
        return e
}
