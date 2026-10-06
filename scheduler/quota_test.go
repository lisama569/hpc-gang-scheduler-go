package scheduler

import "testing"

// 场景：用户额度边界——刚好用满，再提交一个就排队，且原因明确是用户额度。
func TestUserQuotaBoundary(t *testing.T) {
	t.Log("场景: 用户额度边界（用满即排队）")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(16), Healthy: true})
	s.SetUserQuota("alice", cpu(4))
	mustSubmit(t, s, Job{ID: "a1", User: "alice", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "a2", User: "alice", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "a3", User: "alice", Request: cpu(2)})
	s.RunCycle()
	if st := state(t, s, "a1"); st != StateRunning {
		t.Fatalf("a1 = %s", st)
	}
	if st := state(t, s, "a2"); st != StateRunning {
		t.Fatalf("a2 = %s", st)
	}
	if got := s.UserUsage("alice"); got != cpu(4) {
		t.Fatalf("alice usage = %v, want exactly 4 (满额)", got)
	}
	if st := state(t, s, "a3"); st != StatePending {
		t.Fatalf("a3 should queue on quota, got %s", st)
	}
	r, msg := s.Explain("a3")
	if r != ReasonUserQuotaExceeded {
		t.Fatalf("a3 defer reason = %s, want UserQuotaExceeded", r)
	}
	t.Logf("a3 被推迟: %s", msg)
	mustValidate(t, s)
}

// 场景：gang 作业按整组总占用计额度——不允许起一半，也不允许偷偷超发。
func TestUserQuotaGangAtomic(t *testing.T) {
	t.Log("场景: gang 作业整体计额度，不超发")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(16), Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: cpu(16), Healthy: true})
	s.SetUserQuota("alice", cpu(4))
	mustSubmit(t, s, Job{ID: "g1", User: "alice", Request: cpu(1), GangSize: 3}) // 总 3
	mustSubmit(t, s, Job{ID: "g2", User: "alice", Request: cpu(1), GangSize: 2}) // 总 2，3+2>4
	s.RunCycle()
	if st := state(t, s, "g1"); st != StateRunning {
		t.Fatalf("g1 = %s", st)
	}
	if st := state(t, s, "g2"); st != StatePending {
		t.Fatalf("g2 should queue on quota (3+2>4), got %s", st)
	}
	if r, _ := s.Explain("g2"); r != ReasonUserQuotaExceeded {
		t.Fatalf("g2 reason = %s, want UserQuotaExceeded", r)
	}
	if got := s.UserUsage("alice"); got != cpu(3) {
		t.Fatalf("alice usage = %v, want 3", got)
	}
	mustValidate(t, s)
	t.Log("g2 整组被额度拦下，无部分启动、无超发")
}

// 场景：多维额度——CPU、内存、GPU 独立限制，任一维度超限即排队。
func TestUserQuotaMultiDimension(t *testing.T) {
	t.Log("场景: 多维额度独立生效")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: Resource{CPU: 16, MemGB: 64, GPU: 4}, Healthy: true})
	s.SetUserQuota("alice", Resource{CPU: 8, MemGB: 16, GPU: 1})
	mustSubmit(t, s, Job{ID: "gpu1", User: "alice", Request: Resource{CPU: 1, MemGB: 2, GPU: 1}})
	mustSubmit(t, s, Job{ID: "gpu2", User: "alice", Request: Resource{CPU: 1, MemGB: 2, GPU: 1}}) // GPU 超
	mustSubmit(t, s, Job{ID: "mem1", User: "alice", Request: Resource{CPU: 1, MemGB: 15}})        // 内存超
	s.RunCycle()
	if st := state(t, s, "gpu1"); st != StateRunning {
		t.Fatalf("gpu1 = %s", st)
	}
	for _, id := range []string{"gpu2", "mem1"} {
		if st := state(t, s, id); st != StatePending {
			t.Fatalf("%s should queue on quota, got %s", id, st)
		}
		if r, _ := s.Explain(id); r != ReasonUserQuotaExceeded {
			t.Fatalf("%s reason = %s, want UserQuotaExceeded", id, r)
		}
	}
	mustValidate(t, s)
	t.Log("GPU 与内存维度分别触发额度限制")
}

// 场景：未配置额度的用户不受限，行为与以前一致。
func TestNoQuotaUserUnlimited(t *testing.T) {
	t.Log("场景: 无额度用户不受限")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(8), Healthy: true})
	for i := 0; i < 8; i++ {
		mustSubmit(t, s, Job{ID: "b" + itoa(i), User: "bob", Request: cpu(1)})
	}
	s.RunUntilSettled(16)
	for i := 0; i < 8; i++ {
		if st := state(t, s, "b"+itoa(i)); st != StateRunning {
			t.Fatalf("b%d should run (no quota), got %s", i, st)
		}
	}
	if got := s.UserUsage("bob"); got != cpu(8) {
		t.Fatalf("bob usage = %v, want 8", got)
	}
	mustValidate(t, s)
	t.Log("无额度用户跑满集群")
}

// 场景：四条释放路径（完成/取消/被抢占/节点故障）额度都当场归还，后继作业立刻能起。
func TestQuotaReleasePaths(t *testing.T) {
	newSched := func() *Scheduler {
		s := NewScheduler()
		s.AddNode(Node{ID: "n1", Capacity: cpu(2), Healthy: true})
		s.AddNode(Node{ID: "n2", Capacity: cpu(2), Healthy: true})
		s.SetUserQuota("alice", cpu(4))
		mustSubmit(t, s, Job{ID: "x1", User: "alice", Request: cpu(2), Preemptible: true})
		mustSubmit(t, s, Job{ID: "x2", User: "alice", Request: cpu(2)})
		mustSubmit(t, s, Job{ID: "x3", User: "alice", Request: cpu(2)}) // 卡在额度
		s.RunCycle()
		if st := state(t, s, "x3"); st != StatePending {
			t.Fatalf("setup: x3 should be pending, got %s", st)
		}
		if r, _ := s.Explain("x3"); r != ReasonUserQuotaExceeded {
			t.Fatalf("setup: x3 reason = %s", r)
		}
		return s
	}

	t.Run("正常完成释放", func(t *testing.T) {
		s := newSched()
		s.Finish("x1")
		s.RunCycle()
		if st := state(t, s, "x3"); st != StateRunning {
			t.Fatalf("x3 should start after finish, got %s", st)
		}
		if got := s.UserUsage("alice"); got != cpu(4) {
			t.Fatalf("usage = %v, want 4", got)
		}
		mustValidate(t, s)
		t.Log("完成 -> 额度归还 -> x3 启动")
	})

	t.Run("取消释放", func(t *testing.T) {
		s := newSched()
		s.Cancel("x2")
		s.RunCycle()
		if st := state(t, s, "x3"); st != StateRunning {
			t.Fatalf("x3 should start after cancel, got %s", st)
		}
		mustValidate(t, s)
		t.Log("取消 -> 额度归还 -> x3 启动")
	})

	t.Run("被抢占释放", func(t *testing.T) {
		s := newSched()
		// 集群已满（x1+x2=4CPU），高优先级作业抢占 x1，alice 额度应归还
		mustSubmit(t, s, Job{ID: "vip", User: "carol", Request: cpu(2), Priority: 10})
		s.RunCycle()
		if st := state(t, s, "vip"); st != StateRunning {
			t.Fatalf("vip should preempt x1, got %s", st)
		}
		if st := state(t, s, "x1"); st != StatePending {
			t.Fatalf("x1 should be back to pending, got %s", st)
		}
		if got := s.UserUsage("alice"); got != cpu(2) {
			t.Fatalf("alice usage after preempt = %v, want 2", got)
		}
		// alice 额度有余量但集群已满：x3 应卡在资源而不是额度
		if r, _ := s.Explain("x3"); r != ReasonInsufficientResource {
			t.Fatalf("x3 reason = %s, want InsufficientResource", r)
		}
		mustValidate(t, s)
		t.Log("抢占 -> alice 额度归还，x3 改卡在集群资源上")
	})

	t.Run("节点故障释放", func(t *testing.T) {
		s := newSched()
		s.NodeDown("n1")
		s.RunCycle()
		if got := s.UserUsage("alice"); got.CPU > 2 {
			t.Fatalf("alice usage after node down = %v, want <=2", got)
		}
		mustValidate(t, s)
		t.Log("节点故障 -> 故障作业额度归还")
	})
}

// 场景：重复/迟到的完成与故障通知不会重复还额度，更不会超发。
func TestNoDoubleRefund(t *testing.T) {
	t.Log("场景: 重复通知不重复还额度")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	s.SetUserQuota("alice", cpu(4))
	mustSubmit(t, s, Job{ID: "j1", User: "alice", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "j2", User: "alice", Request: cpu(2)})
	s.RunCycle()
	if !s.Finish("j1") {
		t.Fatal("first finish should succeed")
	}
	if s.Finish("j1") {
		t.Fatal("duplicate finish must be ignored")
	}
	if got := s.UserUsage("alice"); got != cpu(2) {
		t.Fatalf("usage = %v, want 2 (no double refund)", got)
	}
	s.Cancel("j2")
	s.Cancel("j2") // 重复取消
	if got := s.UserUsage("alice"); got != cpu(0) {
		t.Fatalf("usage = %v, want 0", got)
	}
	// 再提交两个作业，验证没有因为负额度而超发
	mustSubmit(t, s, Job{ID: "k1", User: "alice", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "k2", User: "alice", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "k3", User: "alice", Request: cpu(2)})
	s.RunCycle()
	if st := state(t, s, "k3"); st != StatePending {
		t.Fatalf("k3 should queue on quota, got %s", st)
	}
	mustValidate(t, s)
	t.Log("重复完成/取消均被忽略，额度守恒")
}
