package scheduler

import (
	"encoding/json"
	"testing"
)

// 场景：快照恢复后，用户额度、排队结论与后续调度决策与原实例完全一致。
func TestSnapshotRestoreEquivalence(t *testing.T) {
	t.Log("场景: 快照恢复一致性")
	build := func() *Scheduler {
		s := NewScheduler()
		s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
		s.AddNode(Node{ID: "n2", Capacity: cpu(4), Healthy: true})
		s.AddQueue(Queue{Name: "qa", Weight: 2})
		s.AddQueue(Queue{Name: "qb", Weight: 1})
		s.SetUserQuota("alice", cpu(4))
		s.SetUserWeight("qa", "alice", 1)
		s.SetUserWeight("qa", "bob", 2)
		mustSubmit(t, s, Job{ID: "a1", User: "alice", Queue: "qa", Request: cpu(2)})
		mustSubmit(t, s, Job{ID: "a2", User: "alice", Queue: "qa", Request: cpu(2)})
		mustSubmit(t, s, Job{ID: "a3", User: "alice", Queue: "qa", Request: cpu(2)}) // 卡额度
		mustSubmit(t, s, Job{ID: "b1", User: "bob", Queue: "qa", Request: cpu(2)})
		mustSubmit(t, s, Job{ID: "c1", User: "carol", Queue: "qb", Request: cpu(1)})
		s.RunCycle()
		return s
	}
	orig := build()
	mustValidate(t, orig)

	// 经过 JSON 序列化往返，模拟落盘恢复
	data, err := json.Marshal(orig.Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	restored := Restore(&snap)
	mustValidate(t, restored)

	// 额度占用一致
	for _, u := range []string{"alice", "bob", "carol"} {
		if got, want := restored.UserUsage(u), orig.UserUsage(u); got != want {
			t.Fatalf("user %s usage: restored=%v orig=%v", u, got, want)
		}
	}
	// 排队结论（状态+推迟原因）一致
	for _, id := range []string{"a1", "a2", "a3", "b1", "c1"} {
		stO, _ := orig.JobState(id)
		stR, _ := restored.JobState(id)
		if stO != stR {
			t.Fatalf("job %s state: restored=%s orig=%s", id, stR, stO)
		}
		rO, _ := orig.Explain(id)
		rR, _ := restored.Explain(id)
		if rO != rR {
			t.Fatalf("job %s defer reason: restored=%s orig=%s", id, rR, rO)
		}
	}
	if r, _ := restored.Explain("a3"); r != ReasonUserQuotaExceeded {
		t.Fatalf("restored a3 reason = %s, want UserQuotaExceeded", r)
	}

	// 后续调度决策一致：alice 完成一个作业后，两边应启动同一个作业
	orig.Finish("a1")
	restored.Finish("a1")
	orig.RunCycle()
	restored.RunCycle()
	for _, id := range []string{"a1", "a2", "a3", "b1", "c1"} {
		stO, _ := orig.JobState(id)
		stR, _ := restored.JobState(id)
		if stO != stR {
			t.Fatalf("after resume job %s: restored=%s orig=%s", id, stR, stO)
		}
	}
	mustValidate(t, orig)
	mustValidate(t, restored)
	t.Log("恢复前后额度、排队结论、后续调度决策完全一致")
}

// 场景：从快照恢复后节点故障，额度归还与守恒校验仍然成立。
func TestSnapshotThenNodeFailure(t *testing.T) {
	t.Log("场景: 恢复后节点故障的额度归还")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(2), Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: cpu(2), Healthy: true})
	s.SetUserQuota("alice", cpu(3)) // g 占 2，w 需要 2 -> 卡在额度
	mustSubmit(t, s, Job{ID: "g", User: "alice", Request: cpu(1), GangSize: 2})
	mustSubmit(t, s, Job{ID: "w", User: "alice", Request: cpu(2)})
	s.RunCycle()
	if r, _ := s.Explain("w"); r != ReasonUserQuotaExceeded {
		t.Fatalf("setup: w reason = %s", r)
	}
	restored := Restore(s.Snapshot())
	restored.NodeDown("n1")
	if got := restored.UserUsage("alice"); got != cpu(0) {
		t.Fatalf("usage after node down = %v, want 0", got)
	}
	restored.RunCycle()
	if st := state(t, restored, "w"); st != StateRunning {
		t.Fatalf("w should start after quota refunded, got %s", st)
	}
	mustValidate(t, restored)
	t.Log("恢复后故障回收正常，w 启动")
}
