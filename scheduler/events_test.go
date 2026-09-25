package scheduler

import (
	"errors"
	"testing"
)

func TestNodeFaultKillsGangAndReleases(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 8}}))
	must(t, s.Submit(&Job{ID: "j", GangID: "g", Members: 2, Request: Resources{CPU: 8}}))
	s.Schedule()
	j, _ := s.Job("j")
	if j.State != StateRunning {
		t.Fatalf("setup: want running, got %s", j.State)
	}

	aff, err := s.NodeFaultAffected("n1", "fault-1")
	must(t, err)
	if len(aff) != 1 || aff[0] != "j" {
		t.Fatalf("gang on faulted node must be failed atomically, affected=%v", aff)
	}
	j, _ = s.Job("j")
	if j.State != StateFailed || len(j.NodeIDs) != 0 {
		t.Fatalf("want Failed with released nodes, got %s %v", j.State, j.NodeIDs)
	}
	// gang 跨两台节点，另一台上的份额也必须回收
	if n := s.testNode("n2"); n.Used.CPU != 0 {
		t.Fatalf("surviving node allocation must be released, used=%d", n.Used.CPU)
	}
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved after fault")
	}
}

func TestDuplicateAndLateEventsAreIdempotent(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.Submit(&Job{ID: "j", GangID: "g", Members: 1, Request: Resources{CPU: 8}}))
	s.Schedule()

	// 完成 -> 资源释放
	if err := s.NotifyComplete("j", "c1"); err != nil {
		t.Fatal(err)
	}
	usedAfter := s.testNode("n1").Used.CPU

	// 重复完成事件
	err := s.NotifyComplete("j", "c1")
	if !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("duplicate complete must be ErrDuplicateEvent, got %v", err)
	}
	// 乱序/迟到但新 eventID：作业已终态，拒绝且不回退状态
	err = s.NotifyComplete("j", "c2-late")
	if !errors.Is(err, ErrTerminalState) {
		t.Fatalf("late complete must be ErrTerminalState, got %v", err)
	}
	j, _ := s.Job("j")
	if j.State != StateCompleted {
		t.Fatalf("state must not roll back, got %s", j.State)
	}
	if s.testNode("n1").Used.CPU != usedAfter {
		t.Fatal("duplicate/late event changed ledger")
	}

	// 节点事件重复：同一 eventID 去重
	must(t, s.NodeFault("n1", "f1"))
	if err := s.NodeFault("n1", "f1"); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("duplicate fault must be ErrDuplicateEvent, got %v", err)
	}
	// 不同 eventID 但节点已是 Faulted：幂等空操作，不报错、不产生副作用
	if err := s.NodeFault("n1", "f2-again"); err != nil {
		t.Fatalf("repeated fault of same node should be no-op, got %v", err)
	}
	must(t, s.NodeRecover("n1", "r1"))
	if n := s.testNode("n1"); n.State != NodeHealthy {
		t.Fatalf("recover failed: %s", n.State)
	}
	// 恢复后可再次调度（同 gang 作业已完成，提交新作业验证节点可用）
	must(t, s.Submit(&Job{ID: "j2", GangID: "g2", Members: 1, Request: Resources{CPU: 8}}))
	r := s.Schedule()
	if len(r.StartedGangs) != 1 {
		t.Fatalf("recovered node must accept new jobs, rejected=%v", r.Rejected)
	}
}

func TestDrainDoesNotKillButRejectsNew(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 8}}))
	must(t, s.Submit(&Job{ID: "j1", GangID: "g1", Members: 1, Request: Resources{CPU: 8}}))
	s.Schedule()
	// n1 正在跑 j1，对 n1 drain：j1 继续运行
	must(t, s.NodeDrain("n1", "d1"))
	if j, _ := s.Job("j1"); j.State != StateRunning {
		t.Fatalf("drain must not kill running jobs, got %s", j.State)
	}
	// 新作业只能落在 n2（每成员 8 CPU，2 成员需要两台健康节点）-> n1 不参与
	must(t, s.Submit(&Job{ID: "j2", GangID: "g2", Members: 2, Request: Resources{CPU: 8}}))
	r := s.Schedule()
	if got := r.Rejected["j2"]; got != ReasonInsufficientCPU {
		t.Fatalf("drained node must be excluded from placement, got reason=%q", got)
	}
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved")
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 8}}))
	must(t, s.Submit(&Job{ID: "j1", GangID: "g1", Members: 2, Request: Resources{CPU: 4}}))
	must(t, s.Submit(&Job{ID: "j2", GangID: "g2", Members: 1, Request: Resources{CPU: 8}}))
	s.Schedule()
	must(t, s.NodeFault("n1", "f1"))
	if !s.CheckConservation() {
		t.Fatal("setup conservation fail")
	}

	snap := s.Snapshot()
	s2 := Restore(snap)
	if !s2.CheckConservation() {
		t.Fatal("restored scheduler ledger not conserved")
	}
	// 事件去重状态也恢复
	if err := s2.NodeFault("n1", "f1"); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("restored scheduler must remember event IDs, got %v", err)
	}
	// 在恢复出的调度器上继续调度，结论一致
	r1 := s.Schedule()
	r2 := s2.Schedule()
	if len(r1.StartedGangs) != len(r2.StartedGangs) {
		t.Fatalf("post-restore schedule diverged: %v vs %v", r1.StartedGangs, r2.StartedGangs)
	}
}
