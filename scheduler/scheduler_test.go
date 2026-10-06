package scheduler

import "testing"

func cpu(n int64) Resource { return Resource{CPU: n} }

func mustValidate(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.Validate(); err != nil {
		t.Fatalf("conservation check failed: %v", err)
	}
}

func state(t *testing.T, s *Scheduler, id string) JobState {
	t.Helper()
	st, ok := s.JobState(id)
	if !ok {
		t.Fatalf("job %s not found", id)
	}
	return st
}

// 场景：gang 原子启动——资源够就整组起，不够就一个都不起。
func TestGangAtomicStart(t *testing.T) {
	t.Log("场景: gang 原子启动（要么全起要么不起）")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: cpu(4), Healthy: true})

	mustSubmit(t, s, Job{ID: "g1", User: "alice", Request: cpu(2), GangSize: 2})
	s.RunCycle()
	if st := state(t, s, "g1"); st != StateRunning {
		t.Fatalf("g1 should be running, got %s", st)
	}
	t.Log("g1 整组启动")

	// 剩余 4 CPU，g2 需要 6 CPU -> 必须整体排队，不能起一半
	mustSubmit(t, s, Job{ID: "g2", User: "alice", Request: cpu(2), GangSize: 3})
	s.RunCycle()
	if st := state(t, s, "g2"); st != StatePending {
		t.Fatalf("g2 should stay pending, got %s", st)
	}
	if r, _ := s.Explain("g2"); r != ReasonInsufficientResource {
		t.Fatalf("g2 defer reason = %s, want InsufficientResource", r)
	}
	mustValidate(t, s)
	t.Log("g2 资源不足整体排队，无部分启动，校验通过")
}

// 场景：同队列内优先级高的先调度，同级按提交先后。
func TestPriorityThenFIFO(t *testing.T) {
	t.Log("场景: 优先级 + FIFO 排队")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	mustSubmit(t, s, Job{ID: "j1", User: "u", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "j2", User: "u", Request: cpu(2)})
	s.RunCycle() // j1 j2 占满
	mustSubmit(t, s, Job{ID: "lo", User: "u", Request: cpu(2), Priority: 1})
	mustSubmit(t, s, Job{ID: "hi", User: "u", Request: cpu(2), Priority: 9})
	s.Finish("j1")
	s.RunCycle()
	if st := state(t, s, "hi"); st != StateRunning {
		t.Fatalf("hi should run before lo, got %s", st)
	}
	if st := state(t, s, "lo"); st != StatePending {
		t.Fatalf("lo should still wait, got %s", st)
	}
	mustValidate(t, s)
	t.Log("高优先级作业插队成功")
}

// 场景：取消——排队中取消直接出队；运行中取消立即释放资源给后继作业。
func TestCancel(t *testing.T) {
	t.Log("场景: 取消排队与运行中的作业")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(2), Healthy: true})
	mustSubmit(t, s, Job{ID: "run", User: "u", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "wait1", User: "u", Request: cpu(2)})
	mustSubmit(t, s, Job{ID: "wait2", User: "u", Request: cpu(2)})
	s.RunCycle()
	if !s.Cancel("wait1") {
		t.Fatal("cancel pending job should succeed")
	}
	if st := state(t, s, "wait1"); st != StateCancelled {
		t.Fatalf("wait1 = %s", st)
	}
	if !s.Cancel("run") {
		t.Fatal("cancel running job should succeed")
	}
	s.RunCycle()
	if st := state(t, s, "wait2"); st != StateRunning {
		t.Fatalf("wait2 should start after cancel freed resources, got %s", st)
	}
	if s.Cancel("run") { // 重复取消无效
		t.Fatal("double cancel should fail")
	}
	mustValidate(t, s)
	t.Log("取消释放资源，后继作业启动，重复取消无副作用")
}

// 场景：节点故障——gang 整组失败，资源当场回收，重复故障通知无副作用。
func TestNodeFailure(t *testing.T) {
	t.Log("场景: 节点故障回收 gang 作业")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(2), Healthy: true})
	s.AddNode(Node{ID: "n2", Capacity: cpu(2), Healthy: true})
	mustSubmit(t, s, Job{ID: "gang", User: "u", Request: cpu(1), GangSize: 2})
	mustSubmit(t, s, Job{ID: "next", User: "u", Request: cpu(2)})
	s.RunCycle()
	if st := state(t, s, "gang"); st != StateRunning {
		t.Fatalf("gang should be running, got %s", st)
	}
	s.NodeDown("n1")
	if st := state(t, s, "gang"); st != StateFailed {
		t.Fatalf("gang should fail as a whole, got %s", st)
	}
	mustValidate(t, s)
	s.RunCycle()
	if st := state(t, s, "next"); st != StateRunning {
		t.Fatalf("next should start on remaining node, got %s", st)
	}
	s.NodeDown("n1") // 重复通知
	mustValidate(t, s)
	t.Log("故障整组回收，后继作业在健康节点启动，重复通知无副作用")
}

// 场景：队列间加权公平——服务份额按队列权重分配。
func TestQueueWeightedFairness(t *testing.T) {
	t.Log("场景: 队列间加权公平 (3:1)")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(8), Healthy: true})
	s.AddQueue(Queue{Name: "qa", Weight: 3})
	s.AddQueue(Queue{Name: "qb", Weight: 1})
	for i := 0; i < 8; i++ {
		mustSubmit(t, s, Job{ID: "a" + itoa(i), User: "alice", Queue: "qa", Request: cpu(1)})
		mustSubmit(t, s, Job{ID: "b" + itoa(i), User: "bob", Queue: "qb", Request: cpu(1)})
	}
	s.RunUntilSettled(64)
	ra, rb := 0, 0
	for i := 0; i < 8; i++ {
		if state(t, s, "a"+itoa(i)) == StateRunning {
			ra++
		}
		if state(t, s, "b"+itoa(i)) == StateRunning {
			rb++
		}
	}
	t.Logf("qa 运行 %d 个, qb 运行 %d 个", ra, rb)
	if ra != 6 || rb != 2 {
		t.Fatalf("weighted share = %d:%d, want 6:2", ra, rb)
	}
	mustValidate(t, s)
}

// 场景：优先级抢占——高优先级作业抢占可抢占的低优先级作业，被抢者重新排队。
func TestPriorityPreemption(t *testing.T) {
	t.Log("场景: 优先级抢占")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	mustSubmit(t, s, Job{ID: "low1", User: "u", Request: cpu(2), Preemptible: true})
	mustSubmit(t, s, Job{ID: "low2", User: "u", Request: cpu(2), Preemptible: true})
	s.RunCycle()
	mustSubmit(t, s, Job{ID: "high", User: "u", Request: cpu(2), Priority: 10})
	s.RunCycle()
	if st := state(t, s, "high"); st != StateRunning {
		t.Fatalf("high should run after preemption, got %s", st)
	}
	back := 0
	for _, id := range []string{"low1", "low2"} {
		if state(t, s, id) == StatePending {
			back++
		}
	}
	if back != 1 {
		t.Fatalf("exactly one low job should be preempted back to pending, got %d", back)
	}
	mustValidate(t, s)
	t.Log("高优先级抢占成功，被抢作业回到队列")
}

// 场景：队列预留——未用满预留的队列可以回收超用队列的资源。
func TestReservationReclaim(t *testing.T) {
	t.Log("场景: 队列预留回收")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	s.AddQueue(Queue{Name: "vip", Weight: 1, Guarantee: cpu(4)})
	s.AddQueue(Queue{Name: "batch", Weight: 1})
	for i := 0; i < 4; i++ {
		mustSubmit(t, s, Job{ID: "bk" + itoa(i), User: "bob", Queue: "batch", Request: cpu(1), Preemptible: true})
	}
	s.RunCycle() // batch 占满集群
	mustSubmit(t, s, Job{ID: "vipjob", User: "alice", Queue: "vip", Request: cpu(2)})
	s.RunCycle()
	if st := state(t, s, "vipjob"); st != StateRunning {
		t.Fatalf("vip job should reclaim reserved resources, got %s", st)
	}
	mustValidate(t, s)
	t.Log("预留队列回收成功")
}

func mustSubmit(t *testing.T, s *Scheduler, j Job) {
	t.Helper()
	if err := s.Submit(j); err != nil {
		t.Fatalf("submit %s: %v", j.ID, err)
	}
}

func itoa(i int) string {
	return string(rune('0' + i))
}
