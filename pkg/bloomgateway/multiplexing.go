package bloomgateway

import (
	"math"
	"time"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/util/loser"
	"github.com/oklog/ulid"
)

type Iter[T any] struct {
	items []T
	idx   int
	ident int // the index that identifies the iterator
}

func (i Iter[T]) Next() bool {
	if i.idx+1 > len(i.items) {
		return false
	}
	i.idx++
	return true
}

func (i Iter[T]) At() T {
	return i.items[i.idx]
}

func (i Iter[T]) Ident() int {
	return i.ident
}

func IterFromSlice[T any](i int, items []T) Iter[T] {
	return Iter[T]{
		items: items,
		idx:   -1,
		ident: i,
	}
}

// Task is the data structure that is enqueued to the internal queue and queued by query workers
type Task struct {
	// ID is a lexcographically sortable unique identifier of the task
	ID ulid.ULID
	// Tenant is the tenant ID
	Tenant string
	// Request is the original request
	Request *logproto.FilterChunkRefRequest
	// ErrCh is a send-only channel to write an error to
	ErrCh chan<- error
	// ResCh is a send-only channel to write partial responses to
	ResCh chan<- *logproto.GroupedChunkRefs
}

// NewTask returns a new Task that can be enqueued to the task queue.
// As additional arguments, it returns a result and an error channel, as well
// as an error if the instantiation fails.
func NewTask(tenantID string, req *logproto.FilterChunkRefRequest) (Task, chan *logproto.GroupedChunkRefs, chan error, error) {
	key, err := ulid.New(ulid.Now(), nil)
	if err != nil {
		return Task{}, nil, nil, err
	}
	errCh := make(chan error, 1)
	resCh := make(chan *logproto.GroupedChunkRefs, 1)
	task := Task{
		ID:      key,
		Tenant:  tenantID,
		Request: req,
		ErrCh:   errCh,
		ResCh:   resCh,
	}
	return task, resCh, errCh, nil
}

func (t Task) WithRequest(req *logproto.FilterChunkRefRequest) Task {
	return Task{
		ID:      t.ID,
		Tenant:  t.Tenant,
		Request: req,
		ErrCh:   t.ErrCh,
		ResCh:   t.ResCh,
	}
}

type MultiplexTask struct {
	Tasks  []*FilterableChunkGroup
	Tenant string
	Day    int // replace with DayTime
}

type FilterableChunkGroup struct {
	*logproto.GroupedChunkRefs

	From    time.Time
	Through time.Time

	Filters []*logproto.LineFilterExpression

	ResCh chan<- *logproto.GroupedChunkRefs
	ErrCh chan<- error
}

type multiplexIterator struct {
	curr  FilterableChunkGroup
	tasks []Task
	errs  []error
	tree  *loser.Tree[uint64, Iter[*logproto.GroupedChunkRefs]]
}

func newMultiplexIterator(tasks []Task) *multiplexIterator {
	sequences := make([]Iter[*logproto.GroupedChunkRefs], 0, len(tasks))
	for i := range tasks {
		sequences = append(sequences, IterFromSlice(i, tasks[i].Request.Refs))
	}

	atFn := func(s Iter[*logproto.GroupedChunkRefs]) uint64 {
		return s.At().Fingerprint
	}
	lessFn := func(i, j uint64) bool {
		return i < j
	}
	closeFn := func(s Iter[*logproto.GroupedChunkRefs]) {}

	return &multiplexIterator{
		tasks: tasks,
		errs:  make([]error, 0),
		curr:  FilterableChunkGroup{},
		tree:  loser.New(sequences, math.MaxUint64, atFn, lessFn, closeFn),
	}
}

func (i *multiplexIterator) Next() bool {
	ok := i.tree.Next()
	if !ok {
		return false
	}
	next := i.tree.Winner()

	group := next.At()
	iterIndex := next.Ident()
	task := i.tasks[iterIndex]

	i.curr.Fingerprint = group.Fingerprint
	i.curr.Tenant = group.Tenant
	i.curr.Refs = group.Refs
	i.curr.From, i.curr.Through = getFromThrough(group.Refs)
	i.curr.ResCh = task.ResCh
	i.curr.ErrCh = task.ErrCh
	i.curr.Filters = task.Request.Filters
	return true
}

func (i *multiplexIterator) At() FilterableChunkGroup {
	return i.curr
}

func getFromThrough(refs []*logproto.ShortRef) (time.Time, time.Time) {
	if len(refs) == 0 {
		return time.Time{}, time.Time{}
	}
	return refs[0].From.Time(), refs[len(refs)-1].Through.Time()
}
