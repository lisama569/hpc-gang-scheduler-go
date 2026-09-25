package scheduler

import (
	"strings"
	"testing"
)

// 两台单机单槽节点；每个作业占一个槽位，一次 Schedule 最多启动两个。
func twoSlotCluster(t *testing.T) *Scheduler {
	t.Helper()
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 1}}))
	must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 1}}))
	return s
}

func TestWeightedFairAcrossQueues(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 4}}))
	s.AddQueue(QueueConfig{Name: "a", Weight: 1})
	s.AddQueue(QueueConfig{Name: "b", Weight: 1})
	for _, id := range []string{"a1", "a2", "b1", "b2"} {
		q := strings.TrimRight(id, "0123456789")
		must(t, s.Submit(&Job{ID: id, GangID: "g-" + id, User: "u", Queue: q,
			Members: 1, Priority: 1, Request: Resources{CPU: 1}}))
	}
	r := s.Schedule()
	// 等权重轮转：a,b,a,b —— 任何队列都不会连续占满全部槽位
	want := []string{"g-a1", "g-b1", "g-a2", "g-b2"}
	if len(r.StartedGangs) != 4 {
		t.Fatalf("want 4 started, got %v", r.StartedGangs)
	}
	for i := range want {
		if r.StartedGangs[i] != want[i] {
			t.Fatalf("round-robin order mismatch: want %v got %v", want, r.StartedGangs)
		}
	}
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved")
	}
}

func TestWeight2GetsDoubleService(t *testing.T) {
	// 单节点 4 槽，队列 a 权重 2、b 权重 1。
	// 动态加权轮转的“每权重服务量”应在 4 次启动后保持公平：a 拿 3 个、b 拿 1 个，
	// 且顺序由份额比较唯一确定（a,b,a,a）。
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 4}}))
	s.AddQueue(QueueConfig{Name: "a", Weight: 2})
	s.AddQueue(QueueConfig{Name: "b", Weight: 1})
	for _, id := range []string{"a1", "a2", "a3", "b1", "b2"} {
		q := string(id[0])
		must(t, s.Submit(&Job{ID: id, GangID: "g-" + id, Queue: q,
			Members: 1, Priority: 1, Request: Resources{CPU: 1}}))
	}
	r := s.Schedule()
	want := []string{"g-a1", "g-b1", "g-a2", "g-a3"}
	if len(r.StartedGangs) != 4 {
		t.Fatalf("want 4 started, got %v", r.StartedGangs)
	}
	for i := range want {
		if r.StartedGangs[i] != want[i] {
			t.Fatalf("weighted RR order mismatch: want %v got %v", want, r.StartedGangs)
		}
	}
}

func TestStarvationReservation(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 1}}))
	s.AddQueue(QueueConfig{Name: "q", Weight: 1, ReservationThreshold: 3})

	// 老作业一直要 2 CPU（资源不足），被推迟 3 次后获得预留地位
	must(t, s.Submit(&Job{ID: "old", GangID: "gold", User: "u", Queue: "q",
		Members: 1, Priority: 1, Request: Resources{CPU: 2}}))
	for i := 0; i < 3; i++ {
		s.Schedule()
	}
	old, _ := s.Job("old")
	if old.Retries != 3 {
		t.Fatalf("want 3 retries, got %d", old.Retries)
	}

	// 后来一个高优先级但只需 1 CPU 的作业；当前 1 槽空闲，能直接启动，
	// 这里改为验证：当两者都无法立即启动时，预留者在排序中领先
	must(t, s.Submit(&Job{ID: "new", GangID: "gnew", User: "u", Queue: "q",
		Members: 1, Priority: 9, Request: Resources{CPU: 2}}))
	order := s.orderedPendingGangs()
	if len(order) == 0 || order[0] != "gold" {
		t.Fatalf("starving reserved gang must be tried first, order=%v", order)
	}
}

func TestPriorityPreemptionControlled(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 4}}))
	s.AddQueue(QueueConfig{Name: "q", PreemptEnabled: true})
	must(t, s.Submit(&Job{ID: "low", GangID: "glow", Queue: "q",
		Members: 1, Priority: 1, Request: Resources{CPU: 4}}))
	s.Schedule()
	must(t, s.Submit(&Job{ID: "high", GangID: "ghigh", Queue: "q",
		Members: 1, Priority: 9, Request: Resources{CPU: 4}}))
	s.Schedule()
	high, _ := s.Job("high")
	low, _ := s.Job("low")
	if high.State != StateRunning || high.NodeIDs[0] != "n1" {
		t.Fatalf("high job should run after preemption: %s %v", high.State, high.NodeIDs)
	}
	if low.State != StatePending || low.PreemptCount != 1 {
		t.Fatalf("victim should be requeued with reserve boost: %s preempt=%d",
			low.State, low.PreemptCount)
	}
	aud := s.AuditLog()
	if len(aud) != 1 || aud[0].Kind != "preempt" || aud[0].VictimJob != "low" {
		t.Fatalf("want one preempt audit record, got %+v", aud)
	}
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved after preemption")
	}

	// 高优先级作业完成后，被抢占作业的下一次调度结论可复现地重新启动
	must(t, s.NotifyComplete("high", "c1"))
	r1 := s.Schedule()
	r2 := s.Schedule()
	if len(r1.StartedGangs) != 1 || len(r2.StartedGangs) != 0 {
		t.Fatalf("victim restart not reproducible: %v then %v", r1.StartedGangs, r2.StartedGangs)
	}
	low, _ = s.Job("low")
	if low.State != StateRunning {
		t.Fatalf("victim should be running again, got %s", low.State)
	}
}

func TestPreemptionDisabledDoesNotEvict(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 4}}))
	s.AddQueue(QueueConfig{Name: "q", PreemptEnabled: false})
	must(t, s.Submit(&Job{ID: "low", GangID: "glow", Queue: "q",
		Members: 1, Priority: 1, Request: Resources{CPU: 4}}))
	s.Schedule()
	must(t, s.Submit(&Job{ID: "high", GangID: "ghigh", Queue: "q",
		Members: 1, Priority: 9, Request: Resources{CPU: 4}}))
	r := s.Schedule()
	if _, rejected := r.Rejected["high"]; !rejected {
		t.Fatalf("without preemption enabled high job must stay deferred: %v", r)
	}
	low, _ := s.Job("low")
	if low.State != StateRunning {
		t.Fatalf("running job must not be evicted when preemption disabled")
	}
}

func TestCrossQueueNoPreemption(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 4}}))
	s.AddQueue(QueueConfig{Name: "a", PreemptEnabled: true})
	s.AddQueue(QueueConfig{Name: "b", PreemptEnabled: true})
	must(t, s.Submit(&Job{ID: "low", GangID: "glow", Queue: "a",
		Members: 1, Priority: 1, Request: Resources{CPU: 4}}))
	s.Schedule()
	must(t, s.Submit(&Job{ID: "high", GangID: "ghigh", Queue: "b",
		Members: 1, Priority: 99, Request: Resources{CPU: 4}}))
	r := s.Schedule()
	if _, rejected := r.Rejected["high"]; !rejected {
		t.Fatalf("cross-queue preemption must not happen: %v", r)
	}
}
