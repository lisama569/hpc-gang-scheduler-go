package scheduler

import (
	"errors"
	"sync"
	"testing"
)

func TestInvalidSubmissions(t *testing.T) {
	s := newTestScheduler()
	cases := []*Job{
		nil,
		{ID: "", GangID: "g", Members: 1},
		{ID: "j", GangID: "", Members: 1},
		{ID: "j", GangID: "g", Members: 0},
		{ID: "j", GangID: "g", Members: 1, Request: Resources{CPU: -1}},
	}
	for i, c := range cases {
		if err := s.Submit(c); !errors.Is(err, ErrInvalidJob) {
			t.Fatalf("case %d: want ErrInvalidJob, got %v", i, err)
		}
	}
	if err := s.AddNode(&Node{ID: ""}); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("empty node id must be invalid, got %v", err)
	}
}

func TestClosedQueueRejects(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	s.AddQueue(QueueConfig{Name: "closed", Closed: true})
	must(t, s.Submit(&Job{ID: "j", GangID: "g", Queue: "closed",
		Members: 1, Request: Resources{CPU: 1}}))
	j, _ := s.Job("j")
	if j.State != StateRejected || j.Reason != ReasonQueueClosed {
		t.Fatalf("closed queue job must be Rejected/QueueClosed, got %s/%s", j.State, j.Reason)
	}
	if r := s.Schedule(); len(r.StartedGangs) != 0 {
		t.Fatalf("closed-queue job must never start: %v", r.StartedGangs)
	}
}

func TestMissingJobAndNode(t *testing.T) {
	s := newTestScheduler()
	if _, err := s.Job("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := s.Cancel("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := s.NodeFault("nope", "e1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestSubmitDuplicateIDConcurrent(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 64}}))
	var wg sync.WaitGroup
	var successes int32
	var mu sync.Mutex
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Submit(&Job{ID: "same", GangID: "same-g", Members: 1, Request: Resources{CPU: 1}})
			mu.Lock()
			if err == nil {
				successes++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("exactly one submit with same ID must succeed, got %d", successes)
	}
}
