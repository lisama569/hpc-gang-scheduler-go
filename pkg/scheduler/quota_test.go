package scheduler

import "testing"

// 场景：用户额度边界——刚好用满，再提交一个就排队，原因是 UserQuotaExceeded。
func TestUserQuotaBoundary(t *testing.T) {
	t.Log("业务场景: 用户额度刚好用满，新作业排队且原因可区分为用户额度")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{
			"alice": {CPU: 8, Mem: 16, GPU: 1},
		},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 64, Mem: 256, GPU: 8}, Healthy: true})

	// 两个作业刚好用满 alice 的全部三个维度。
	s.Submit(Job{ID: "a1", User: "alice", Queue: "q1", GangSize: 1, Request: ResourceVec{CPU: 4, Mem: 8}})
	s.Submit(Job{ID: "a2", User: "alice", Queue: "q1", GangSize: 1, Request: ResourceVec{CPU: 4, Mem: 8, GPU: 1}})
	s.RunCycle()
	for _, id := range []string{"a1", "a2"} {
		if j, _ := s.Job(id); j.State != JobRunning {
			t.Fatalf("%s should run (quota exactly full), got %s", id, j.State)
		}
	}
	if got := s.Quota().Used("alice"); got != (ResourceVec{CPU: 8, Mem: 16, GPU: 1}) {
		t.Fatalf("alice usage should equal quota, got %+v", got)
	}

	// 再提交任意维度的作业都会超额度。
	s.Submit(Job{ID: "a3", User: "alice", Queue: "q1", GangSize: 1, Request: ResourceVec{CPU: 1}})
	s.RunCycle()
	j, _ := s.Job("a3")
	if j.State != JobPending {
		t.Fatalf("a3 should be queued on quota, got %s", j.State)
	}
	if j.Defer != ReasonUserQuotaExceeded {
		t.Fatalf("a3 defer reason should be UserQuotaExceeded, got %q", j.Defer)
	}
	mustInvariant(t, s)
}

// 场景：无额度用户不受限，行为与旧版本一致。
func TestUserWithoutQuotaUnlimited(t *testing.T) {
	t.Log("业务场景: 未配置额度的用户不受限，可占满整个集群")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 1}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 10}, Healthy: true})
	for i := 0; i < 5; i++ {
		s.Submit(job(jobID("b", i), "bob", "q1", 0, 1, 2))
	}
	s.RunCycle()
	for i := 0; i < 5; i++ {
		if j, _ := s.Job(jobID("b", i)); j.State != JobRunning {
			t.Fatalf("unlimited user job %d should run, got %s", i, j.State)
		}
	}
	mustInvariant(t, s)
}

// 场景：gang 整体受额度约束——总额度不够时一个任务都不起，不偷偷超发。
func TestUserQuotaGangAtomic(t *testing.T) {
	t.Log("业务场景: gang 总额度超限时整组排队，不允许起一半或超发")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 5}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 100}, Healthy: true})
	s.Submit(job("g1", "alice", "q1", 0, 3, 2)) // 总共 6 CPU > 额度 5
	s.RunCycle()
	j, _ := s.Job("g1")
	if j.State != JobPending || j.Defer != ReasonUserQuotaExceeded {
		t.Fatalf("gang should be deferred on quota, got %s %q", j.State, j.Defer)
	}
	if got := s.Quota().Used("alice"); !got.IsZero() {
		t.Fatalf("no partial quota allocation allowed, used=%+v", got)
	}
	if got := s.totalUsed(); !got.IsZero() {
		t.Fatalf("no partial gang allocation allowed, used=%+v", got)
	}
	mustInvariant(t, s)
}

// 场景：完成释放额度——同一用户排队的作业马上补位。
func TestQuotaReleasedOnComplete(t *testing.T) {
	t.Log("业务场景: 作业完成当场归还额度，同用户排队作业立即补位")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 4}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 64}, Healthy: true})
	s.Submit(job("a1", "alice", "q1", 0, 1, 4))
	s.Submit(job("a2", "alice", "q1", 0, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("a2"); j.Defer != ReasonUserQuotaExceeded {
		t.Fatalf("a2 should be deferred on quota, got %q", j.Defer)
	}
	if err := s.Complete("a1"); err != nil {
		t.Fatal(err)
	}
	if got := s.Quota().Used("alice"); got.CPU != 0 {
		if j2, _ := s.Job("a2"); j2.State != JobRunning {
			t.Fatalf("after complete, quota=%+v a2=%s", got, j2.State)
		}
	}
	if j, _ := s.Job("a2"); j.State != JobRunning {
		t.Fatalf("a2 should be admitted right after release, got %s", j.State)
	}
	if got := s.Quota().Used("alice"); got.CPU != 4 {
		t.Fatalf("a2 should hold the quota now, used=%+v", got)
	}
	mustInvariant(t, s)
}

// 场景：取消释放额度。
func TestQuotaReleasedOnCancel(t *testing.T) {
	t.Log("业务场景: 取消运行中作业，额度当场归还")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 4}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 64}, Healthy: true})
	s.Submit(job("a1", "alice", "q1", 0, 1, 4))
	s.Submit(job("a2", "alice", "q1", 0, 1, 4))
	s.RunCycle()
	if err := s.Cancel("a1"); err != nil {
		t.Fatal(err)
	}
	if j, _ := s.Job("a2"); j.State != JobRunning {
		t.Fatalf("a2 should be admitted after cancel released quota, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：抢占释放额度——被抢占作业回到排队，其额度立即让给同用户其他作业。
func TestQuotaReleasedOnPreemption(t *testing.T) {
	t.Log("业务场景: 作业被抢占重新排队，额度当场归还并可被同用户作业使用")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 4}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("a1", "alice", "q1", 1, 1, 4)) // 低优先级，先跑
	s.Submit(job("a2", "alice", "q1", 1, 1, 4)) // 卡在额度上
	s.RunCycle()
	if j, _ := s.Job("a1"); j.State != JobRunning {
		t.Fatalf("a1 should run, got %s", j.State)
	}
	// bob 的高优先级作业抢占 a1。
	s.Submit(job("b1", "bob", "q1", 9, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("b1"); j.State != JobRunning {
		t.Fatalf("b1 should preempt a1, got %s", j.State)
	}
	if j, _ := s.Job("a1"); j.State != JobPending {
		t.Fatalf("a1 should be requeued, got %s", j.State)
	}
	// a1 被抢占后额度已归还：alice 的占用必须是 0（a2 也因集群满而未准入）。
	if got := s.Quota().Used("alice"); !got.IsZero() {
		t.Fatalf("preemption must release alice quota, got %+v", got)
	}
	mustInvariant(t, s)
}

// 场景：节点故障释放额度——整组失败，额度归还，同用户作业在其他节点补位。
func TestQuotaReleasedOnNodeFailure(t *testing.T) {
	t.Log("业务场景: 节点故障整组失败，额度归还，排队作业补位到健康节点")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 4}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true, Labels: map[string]string{"rack": "r1"}})
	s.AddNode(Node{ID: "n2", Capacity: ResourceVec{CPU: 4}, Healthy: true, Labels: map[string]string{"rack": "r2"}})
	s.Submit(Job{ID: "a1", User: "alice", Queue: "q1", GangSize: 1,
		Request: ResourceVec{CPU: 4}, NodeSelector: map[string]string{"rack": "r1"}})
	s.Submit(job("a2", "alice", "q1", 0, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("a2"); j.Defer != ReasonUserQuotaExceeded {
		t.Fatalf("a2 should wait on quota, got %q", j.Defer)
	}
	s.NodeFailed("n1")
	if j, _ := s.Job("a1"); j.State != JobFailed {
		t.Fatalf("a1 should fail with node, got %s", j.State)
	}
	if j, _ := s.Job("a2"); j.State != JobRunning {
		t.Fatalf("a2 should take released quota on healthy node, got %s", j.State)
	}
	if got := s.Quota().Used("alice"); got.CPU != 4 {
		t.Fatalf("exactly one job worth of quota should be held, got %+v", got)
	}
	mustInvariant(t, s)
}

// 场景：重复/迟到的完成与故障通知不会重复还额度，也不会超发。
func TestQuotaReleaseIdempotent(t *testing.T) {
	t.Log("业务场景: 重复完成/故障通知幂等，额度不重复归还、不超发")
	s := New(Config{
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 4}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("a1", "alice", "q1", 0, 1, 4))
	s.RunCycle()

	if err := s.Complete("a1"); err != nil {
		t.Fatal(err)
	}
	// 重复完成通知：安全且不还第二次。
	if err := s.Complete("a1"); err != nil {
		t.Fatal(err)
	}
	// 迟到的故障通知：作业已完成，不受影响。
	s.NodeFailed("n1")
	if got := s.Quota().Used("alice"); !got.IsZero() {
		t.Fatalf("duplicate notifications must not corrupt quota, got %+v", got)
	}
	mustInvariant(t, s)

	// 额度恰好 4：若被重复归还，这里就能超发进两个 4 CPU 作业。
	s.Submit(job("a2", "alice", "q1", 0, 1, 4))
	s.Submit(job("a3", "alice", "q1", 0, 1, 4))
	s.RunCycle()
	running := 0
	for _, id := range []string{"a2", "a3"} {
		if j, _ := s.Job(id); j.State == JobRunning {
			running++
		}
	}
	if running != 1 {
		t.Fatalf("quota must not be over-admitted, running=%d", running)
	}
	mustInvariant(t, s)
}
