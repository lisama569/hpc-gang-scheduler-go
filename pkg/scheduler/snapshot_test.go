package scheduler

import "testing"

// 场景：快照恢复后，用户额度、排队顺序与调度结论与未恢复时完全一致。
func TestSnapshotRestoreEquivalence(t *testing.T) {
	t.Log("业务场景: 快照恢复后用户额度与排队结论与恢复前一致")
	cfg := Config{
		Queues: []QueueConfig{
			{Name: "q1", Weight: 1, UserWeights: map[string]float64{"alice": 2, "bob": 1}},
		},
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 6}},
	}
	build := func() *Scheduler {
		s := New(cfg)
		s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 8}, Healthy: true})
		s.AddNode(Node{ID: "n2", Capacity: ResourceVec{CPU: 8}, Healthy: true})
		// alice 用满一部分额度，bob 无额度，再排几个等待的作业。
		s.Submit(job("a1", "alice", "q1", 0, 1, 4))
		s.Submit(job("a2", "alice", "q1", 0, 1, 4)) // 额度内第二个
		s.Submit(job("a3", "alice", "q1", 0, 1, 4)) // 卡在额度上
		s.Submit(job("b1", "bob", "q1", 0, 1, 4))
		s.Submit(job("b2", "bob", "q1", 0, 1, 4))
		s.RunCycle()
		return s
	}

	origin := build()
	snap := origin.Snapshot()
	restored := Restore(cfg, snap)

	// 额度占用一致。
	for _, u := range []string{"alice", "bob"} {
		if origin.Quota().Used(u) != restored.Quota().Used(u) {
			t.Fatalf("quota mismatch for %s: origin=%+v restored=%+v",
				u, origin.Quota().Used(u), restored.Quota().Used(u))
		}
	}
	// 排队顺序与推迟原因一致。
	op, rp := origin.PendingJobs("q1"), restored.PendingJobs("q1")
	if len(op) != len(rp) {
		t.Fatalf("pending length mismatch: %d vs %d", len(op), len(rp))
	}
	for i := range op {
		if op[i].ID != rp[i].ID || op[i].Defer != rp[i].Defer {
			t.Fatalf("pending[%d] mismatch: %s/%q vs %s/%q",
				i, op[i].ID, op[i].Defer, rp[i].ID, rp[i].Defer)
		}
	}
	mustInvariant(t, origin)
	mustInvariant(t, restored)

	// 恢复后继续同样的操作序列，两边结论逐步一致。
	for _, id := range []string{"a1", "b1"} {
		if err := origin.Complete(id); err != nil {
			t.Fatal(err)
		}
		if err := restored.Complete(id); err != nil {
			t.Fatal(err)
		}
	}
	origin.RunCycle()
	restored.RunCycle()
	for _, id := range []string{"a1", "a2", "a3", "b1", "b2"} {
		oj, _ := origin.Job(id)
		rj, _ := restored.Job(id)
		if oj.State != rj.State || oj.Defer != rj.Defer {
			t.Fatalf("job %s diverged after restore: %s/%q vs %s/%q",
				id, oj.State, oj.Defer, rj.State, rj.Defer)
		}
	}
	if origin.Quota().Used("alice") != restored.Quota().Used("alice") {
		t.Fatalf("quota diverged after restore: %+v vs %+v",
			origin.Quota().Used("alice"), restored.Quota().Used("alice"))
	}
	mustInvariant(t, origin)
	mustInvariant(t, restored)
	t.Logf("恢复后两边一致: alice 额度占用 %+v, a3 推迟原因 %q",
		restored.Quota().Used("alice"), mustJob(t, restored, "a3").Defer)
}

func mustJob(t *testing.T, s *Scheduler, id string) Job {
	t.Helper()
	j, ok := s.Job(id)
	if !ok {
		t.Fatalf("job %s not found", id)
	}
	return j
}
