package scheduler

import (
	"testing"
)

func newTestScheduler() *Scheduler {
	s := NewScheduler()
	return s
}

func TestGangAtomic_AllOrNothing(t *testing.T) {
	s := newTestScheduler()
	// 两台节点，各 8 CPU
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 8}}))

	// 4 成员、每成员 5 CPU：总量够(16<20? 不够) -> 应被拒绝，且任何资源都不能被占用
	must(t, s.Submit(&Job{ID: "j", GangID: "g", Queue: "q", Members: 4,
		Request: Resources{CPU: 5}}))
	r := s.Schedule()
	if len(r.StartedGangs) != 0 {
		t.Fatalf("gang should not start, got %v", r.StartedGangs)
	}
	if got := r.Rejected["j"]; got != ReasonInsufficientCPU {
		t.Fatalf("want InsufficientCPU, got %q", got)
	}
	for _, id := range []string{"n1", "n2"} {
		if n := s.testNode(id); n.Used.CPU != 0 {
			t.Fatalf("partial allocation leaked on %s: used=%d", id, n.Used.CPU)
		}
	}
	j, _ := s.Job("j")
	if j.State != StatePending {
		t.Fatalf("insufficient gang must stay Pending, got %s", j.State)
	}

	// 资源补齐：增加两台 8 CPU 节点后应原子启动，4 个成员全部落到节点
	must(t, s.AddNode(&Node{ID: "n3", Total: Resources{CPU: 8}}))
	must(t, s.AddNode(&Node{ID: "n4", Total: Resources{CPU: 8}}))
	r = s.Schedule()
	if len(r.StartedGangs) != 1 || r.StartedGangs[0] != "g" {
		t.Fatalf("gang should start, got %v", r.StartedGangs)
	}
	j, _ = s.Job("j")
	if j.State != StateRunning || len(j.NodeIDs) != 4 {
		t.Fatalf("want Running with 4 placements, got %s %v", j.State, j.NodeIDs)
	}
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved")
	}
}

func TestRejectReasonsDistinguishable(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8, Memory: 8, GPU: 1}}))

	cases := []struct {
		name string
		job  Job
		want RejectReason
	}{
		{"cpu", Job{ID: "jc", GangID: "gc", Members: 1, Request: Resources{CPU: 16}}, ReasonInsufficientCPU},
		{"mem", Job{ID: "jm", GangID: "gm", Members: 1, Request: Resources{CPU: 1, Memory: 32}}, ReasonInsufficientMem},
		{"gpu", Job{ID: "jg", GangID: "gg", Members: 2, Request: Resources{CPU: 1, GPU: 1}}, ReasonInsufficientGPU},
		{"unknown-node", Job{ID: "ju", GangID: "gu", Members: 1, Request: Resources{CPU: 1},
			Constraint: PlacementConstraint{Nodes: []string{"zzz"}}}, ReasonConstraintUnsatis},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			must(t, s.Submit(&c.job))
			r := s.Schedule()
			if got := r.Rejected[c.job.ID]; got != c.want {
				t.Fatalf("want %s got %q", c.want, got)
			}
		})
	}
}

func TestNoHealthyNode(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.NodeFault("n1", "e1"))
	must(t, s.Submit(&Job{ID: "j", GangID: "g", Members: 1, Request: Resources{CPU: 1}}))
	r := s.Schedule()
	if got := r.Rejected["j"]; got != ReasonNoHealthyNode {
		t.Fatalf("want NoHealthyNode got %q", got)
	}
}

func TestSameRackConstraint(t *testing.T) {
	s := newTestScheduler()
	// 4 台节点：r1 两台 4CPU，r2 两台 4CPU；每成员 4 CPU、共 3 成员
	for _, n := range []*Node{
		{ID: "r1a", Rack: "r1", Total: Resources{CPU: 4}},
		{ID: "r1b", Rack: "r1", Total: Resources{CPU: 4}},
		{ID: "r2a", Rack: "r2", Total: Resources{CPU: 4}},
		{ID: "r2b", Rack: "r2", Total: Resources{CPU: 4}},
	} {
		must(t, s.AddNode(n))
	}
	must(t, s.Submit(&Job{ID: "j", GangID: "g", Members: 3, Request: Resources{CPU: 4},
		Constraint: PlacementConstraint{SameRack: true}}))
	r := s.Schedule()
	if got := r.Rejected["j"]; got != ReasonConstraintUnsatis {
		t.Fatalf("no single rack holds 3 members, want constraint reject, got %q", got)
	}
	// 放宽为不要求同机架 -> 全局 4 个槽位足够，启动
	j, _ := s.Job("j")
	j.Constraint.SameRack = false
	// 通过内部映射修改（测试便捷途径）
	s.testJob("j").Constraint.SameRack = false
	r = s.Schedule()
	if len(r.StartedGangs) != 1 {
		t.Fatalf("gang should start without same-rack, rejected=%v", r.Rejected)
	}
}

func TestDeterministicPlacement(t *testing.T) {
	for trial := 0; trial < 10; trial++ {
		s := newTestScheduler()
		must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 8}}))
		must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
		must(t, s.Submit(&Job{ID: "j", GangID: "g", Members: 2, Request: Resources{CPU: 8}}))
		s.Schedule()
		j, _ := s.Job("j")
		if j.NodeIDs[0] != "n1" || j.NodeIDs[1] != "n2" {
			t.Fatalf("placement not deterministic: %v", j.NodeIDs)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
