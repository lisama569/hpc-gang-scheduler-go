package scheduler

import "testing"

// 场景：队列内多用户加权公平——长期服务份额按权重比 2:1 收敛。
func TestUserWeightedFairnessLongTerm(t *testing.T) {
	t.Log("业务场景: 同队列两用户权重 2:1，长期服务份额按权重比收敛")
	s := New(Config{Queues: []QueueConfig{
		{Name: "q1", Weight: 1, UserWeights: map[string]float64{"alice": 2, "bob": 1}},
	}})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 50}, Healthy: true})

	served := map[string]int{"alice": 0, "bob": 0}
	const rounds = 30
	for r := 0; r < rounds; r++ {
		// 每轮两人都保持充足积压（各 40 CPU 需求 > 25 CPU 容量份额），
		// 消除供应不足对份额的干扰。
		for i := 0; i < 20; i++ {
			s.Submit(job(jobID("a", r*20+i), "alice", "q1", 0, 1, 2))
			s.Submit(job(jobID("b", r*20+i), "bob", "q1", 0, 1, 2))
		}
		s.RunCycle()
		// 先快照本轮运行中的作业再逐个完成：
		// Complete 会自动补一轮调度，避免把补位作业算进本轮。
		var running []string
		for _, j := range s.jobs {
			if j.State == JobRunning {
				running = append(running, j.ID)
			}
		}
		for _, id := range running {
			j, _ := s.Job(id)
			served[j.User]++
			if err := s.Complete(id); err != nil {
				t.Fatal(err)
			}
		}
		mustInvariant(t, s)
	}
	t.Logf("用户服务份额: alice=%d bob=%d (期望约 2:1)", served["alice"], served["bob"])
	if served["alice"] == 0 || served["bob"] == 0 {
		t.Fatalf("both users must be served: %v", served)
	}
	ratio := float64(served["alice"]) / float64(served["bob"])
	if ratio < 1.8 || ratio > 2.2 {
		t.Fatalf("share ratio should converge to 2:1, got %.2f (%v)", ratio, served)
	}
}

// 场景：等权重两用户——单用户的大批量作业不能一直压住别人。
func TestUserFairnessNoStarvation(t *testing.T) {
	t.Log("业务场景: 等权重下，单用户大批量提交不压制其他用户")
	s := New(Config{Queues: []QueueConfig{{Name: "q1", Weight: 1}}})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 10}, Healthy: true})

	// alice 一口气投 50 个作业。
	for i := 0; i < 50; i++ {
		s.Submit(job(jobID("a", i), "alice", "q1", 0, 1, 2))
	}
	// bob 只投 1 个。
	s.Submit(job("b0", "bob", "q1", 0, 1, 2))
	s.RunCycle()
	if j, _ := s.Job("b0"); j.State != JobRunning {
		t.Fatalf("bob's job must not be starved by alice's batch, got %s", j.State)
	}
	mustInvariant(t, s)
}

// 场景：用户级公平与既有队列级公平叠加——跨队列份额仍按队列权重。
func TestUserFairnessWithQuotaInteraction(t *testing.T) {
	t.Log("业务场景: 用户额度与用户级公平叠加，额度内轮流服务")
	s := New(Config{
		Queues: []QueueConfig{
			{Name: "q1", Weight: 1, UserWeights: map[string]float64{"alice": 1, "bob": 1}},
		},
		UserQuotas: map[string]ResourceVec{"alice": {CPU: 4}},
	})
	s.AddNode(Node{ID: "n1", Capacity: ResourceVec{CPU: 100}, Healthy: true})
	// alice 有额度 4（最多 2 个作业），bob 无额度。
	for i := 0; i < 5; i++ {
		s.Submit(job(jobID("a", i), "alice", "q1", 0, 1, 2))
		s.Submit(job(jobID("b", i), "bob", "q1", 0, 1, 2))
	}
	s.RunCycle()
	aliceRunning, bobRunning := 0, 0
	for _, j := range s.jobs {
		if j.State != JobRunning {
			continue
		}
		if j.User == "alice" {
			aliceRunning++
		} else {
			bobRunning++
		}
	}
	t.Logf("运行中: alice=%d (额度上限 2) bob=%d", aliceRunning, bobRunning)
	if aliceRunning != 2 {
		t.Fatalf("alice should hold exactly her quota of 2 jobs, got %d", aliceRunning)
	}
	if bobRunning != 5 {
		t.Fatalf("unlimited bob should fill the rest, got %d", bobRunning)
	}
	mustInvariant(t, s)
}
