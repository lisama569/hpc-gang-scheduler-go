package scheduler

import "testing"

// 辅助：构造一个单任务请求 cpu 的作业。
func job(id, user, queue string, prio, gang, cpu int64) Job {
	return Job{
		ID:       id,
		User:     user,
		Queue:    queue,
		Priority: int(prio),
		GangSize: int(gang),
		Request:  ResourceVec{CPU: cpu},
	}
}

func mustInvariant(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.CheckInvariants(); err != nil {
		t.Fatalf("invariant violated: %v", err)
	}
}

// 场景：排队与基本调度——提交后 RunCycle 才准入。
func TestSubmitAndSchedule(t *testing.T) {
	t.Log("业务场景: 作业提交后进入排队，RunCycle 后准入运行")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 10}, Healthy: true})
	s.Submit(job("j1", "alice", "q1", 0, 1, 4))
	if j, _ := s.Job("j1"); j.State != JobPending {
		t.Fatalf("expected pending before cycle, got %s", j.State)
	}
	s.RunCycle()
	if j, _ := s.Job("j1"); j.State != JobRunning {
		t.Fatalf("expected running after cycle, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：gang 原子启动——资源只够一部分任务时，整个 gang 排队，不允许起一半。
func TestGangAtomicity(t *testing.T) {
	t.Log("业务场景: gang 原子启动，资源不足时不允许部分启动")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("g1", "alice", "q1", 0, 3, 2)) // 需要 6 CPU，只有 4
	s.RunCycle()
	j, _ := s.Job("g1")
	if j.State != JobPending {
		t.Fatalf("gang should stay pending, got %s", j.State)
	}
	if j.Defer != ReasonInsufficientResource {
		t.Fatalf("expected InsufficientResource, got %q", j.Defer)
	}
	if got := s.totalUsed(); !got.IsZero() {
		t.Fatalf("no partial allocation allowed, used=%+v", got)
	}
	mustInvariant(t, s)
}

// 场景：gang 跨节点原子启动——单机放不下，多机拼凑可以。
func TestGangAcrossNodes(t *testing.T) {
	t.Log("业务场景: gang 跨节点原子启动")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("g1", "alice", "q1", 0, 4, 2))
	s.RunCycle()
	j, _ := s.Job("g1")
	if j.State != JobRunning || len(j.Assignments) != 4 {
		t.Fatalf("expected running with 4 assignments, got %s %v", j.State, j.Assignments)
	}
	mustInvariant(t, s)
}

// 场景：取消运行中作业，资源当场释放。
func TestCancelRunning(t *testing.T) {
	t.Log("业务场景: 取消运行中作业，资源释放，排队作业补位")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("j1", "alice", "q1", 0, 1, 4))
	s.Submit(job("j2", "bob", "q1", 0, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("j2"); j.State != JobPending {
		t.Fatalf("j2 should be pending, got %s", j.State)
	}
	if err := s.Cancel("j1"); err != nil {
		t.Fatal(err)
	}
	// Cancel 后自动补一轮调度，j2 应当补位。
	if j, _ := s.Job("j2"); j.State != JobRunning {
		t.Fatalf("j2 should be running after cancel, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：节点故障导致整组 gang 失败并回收资源。
func TestNodeFailureReclaimsGang(t *testing.T) {
	t.Log("业务场景: 节点故障，gang 整组失败，资源回收")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("g1", "alice", "q1", 0, 4, 2)) // 横跨 n1+n2
	s.RunCycle()
	if j, _ := s.Job("g1"); j.State != JobRunning {
		t.Fatalf("expected running, got %s", j.State)
	}
	s.NodeFailed("n1")
	j, _ := s.Job("g1")
	if j.State != JobFailed {
		t.Fatalf("gang should fail as a whole, got %s", j.State)
	}
	if len(j.Assignments) != 0 {
		t.Fatalf("failed gang must release all assignments, got %v", j.Assignments)
	}
	mustInvariant(t, s)
	// 重复故障通知幂等。
	s.NodeFailed("n1")
	mustInvariant(t, s)
}

// 场景：同队列内按优先级调度，高优先级先拿资源。
func TestPriorityOrder(t *testing.T) {
	t.Log("业务场景: 同用户同队列，高优先级作业先调度")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("low", "alice", "q1", 1, 1, 4))
	s.Submit(job("high", "alice", "q1", 9, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("high"); j.State != JobRunning {
		t.Fatalf("high priority should run, got %s", j.State)
	}
	if j, _ := s.Job("low"); j.State != JobPending {
		t.Fatalf("low priority should wait, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：抢占——高优先级作业抢占低优先级运行中作业，被抢占者重新排队。
func TestPreemption(t *testing.T) {
	t.Log("业务场景: 高优先级作业抢占低优先级作业，被抢占者重新排队")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true})
	s.Submit(job("low", "alice", "q1", 1, 1, 4))
	s.RunCycle()
	s.Submit(job("high", "bob", "q1", 9, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("high"); j.State != JobRunning {
		t.Fatalf("high should be running after preemption, got %s", j.State)
	}
	if j, _ := s.Job("low"); j.State != JobPending {
		t.Fatalf("low should be requeued, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：预留——其他队列不能侵占本队列未用满的保障额度。
func TestReservation(t *testing.T) {
	t.Log("业务场景: 队列预留保障，其他队列不能侵占")
	s := New(Config{Queues: []QueueConfig{
		{Name: "qa", Weight: 1, Guaranteed: ResourceVec{CPU: 6}},
		{Name: "qb", Weight: 1},
	}})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 10}, Healthy: true})
	// qb 最多只能用 10-6=4 CPU。
	s.Submit(job("b1", "bob", "qb", 0, 1, 4))
	s.Submit(job("b2", "bob", "qb", 0, 1, 4))
	s.RunCycle()
	if j, _ := s.Job("b1"); j.State != JobRunning {
		t.Fatalf("b1 should run, got %s", j.State)
	}
	if j, _ := s.Job("b2"); j.State != JobPending {
		t.Fatalf("b2 must not eat qa reservation, got %s", j.State)
	}
	// qa 的作业可以使用保障内资源。
	s.Submit(job("a1", "alice", "qa", 0, 1, 6))
	s.RunCycle()
	if j, _ := s.Job("a1"); j.State != JobRunning {
		t.Fatalf("a1 should run within guarantee, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：队列间公平——两个队列长期交替获得服务。
func TestQueueFairness(t *testing.T) {
	t.Log("业务场景: 队列间公平，长期服务份额按权重收敛")
	s := New(Config{Queues: []QueueConfig{
		{Name: "qa", Weight: 1},
		{Name: "qb", Weight: 1},
	}})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 10}, Healthy: true})
	served := map[string]int{}
	for round := 0; round < 20; round++ {
		s.Submit(job(jobID("a", round), "alice", "qa", 0, 1, 2))
		s.Submit(job(jobID("b", round), "bob", "qb", 0, 1, 2))
		s.RunCycle()
		// 完成所有运行中作业，模拟周转。
		for _, id := range []string{jobID("a", round), jobID("b", round)} {
			if j, _ := s.Job(id); j.State == JobRunning {
				served[id[:1]]++
				if err := s.Complete(id); err != nil {
					t.Fatal(err)
				}
			}
		}
		mustInvariant(t, s)
	}
	t.Logf("队列服务次数: qa=%d qb=%d", served["a"], served["b"])
	if served["a"] == 0 || served["b"] == 0 {
		t.Fatalf("both queues should be served, got %v", served)
	}
	// 等权重下长期份额应接近 1:1。
	diff := served["a"] - served["b"]
	if diff < 0 {
		diff = -diff
	}
	if diff > 2 {
		t.Fatalf("queue shares diverged: %v", served)
	}
}

// 场景：推迟原因区分——约束不满足与节点不健康。
func TestDeferReasons(t *testing.T) {
	t.Log("业务场景: 推迟原因区分（约束不满足 / 节点不健康 / 资源不足）")
	s := New(Config{})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 4}, Healthy: true, Labels: map[string]string{"gpu": "true"}})
	s.AddNode(Node{ID: "n2", Capacity: ResourceVec{CPU: 4}, Healthy: false, Labels: map[string]string{"gpu": "false"}})

	s.Submit(Job{ID: "c1", User: "alice", Queue: "q1", GangSize: 1,
		Request: ResourceVec{CPU: 1}, NodeSelector: map[string]string{"gpu": "yes"}})
	s.Submit(Job{ID: "c2", User: "alice", Queue: "q1", GangSize: 1,
		Request: ResourceVec{CPU: 1}, NodeSelector: map[string]string{"gpu": "false"}})
	s.Submit(job("c3", "alice", "q1", 0, 1, 100))
	s.RunCycle()

	if j, _ := s.Job("c1"); j.Defer != ReasonConstraintUnsatisfied {
		t.Fatalf("c1: expected ConstraintUnsatisfied, got %q", j.Defer)
	}
	if j, _ := s.Job("c2"); j.Defer != ReasonNoHealthyNode {
		t.Fatalf("c2: expected NoHealthyNode, got %q", j.Defer)
	}
	if j, _ := s.Job("c3"); j.Defer != ReasonInsufficientResource {
		t.Fatalf("c3: expected InsufficientResource, got %q", j.Defer)
	}
	mustInvariant(t, s)
}

func jobID(prefix string, n int) string {
	return prefix + "-" + itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
