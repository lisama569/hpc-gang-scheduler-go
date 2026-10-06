package scheduler

import (
	"fmt"
	"testing"
)

// 场景：同队列多用户按权重轮流拿机会——单次满载时份额精确等于权重比。
func TestUserWeightedShareSingleShot(t *testing.T) {
	t.Log("场景: 队列内用户加权公平 1:3（单次）")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	s.AddQueue(Queue{Name: "q", Weight: 1})
	s.SetUserWeight("q", "alice", 1)
	s.SetUserWeight("q", "bob", 3)
	for i := 0; i < 4; i++ {
		mustSubmit(t, s, Job{ID: "a" + itoa(i), User: "alice", Queue: "q", Request: cpu(1)})
		mustSubmit(t, s, Job{ID: "b" + itoa(i), User: "bob", Queue: "q", Request: cpu(1)})
	}
	s.RunUntilSettled(16)
	ra, rb := 0, 0
	for i := 0; i < 4; i++ {
		if state(t, s, "a"+itoa(i)) == StateRunning {
			ra++
		}
		if state(t, s, "b"+itoa(i)) == StateRunning {
			rb++
		}
	}
	t.Logf("alice %d 个, bob %d 个", ra, rb)
	if ra != 1 || rb != 3 {
		t.Fatalf("user share = %d:%d, want 1:3", ra, rb)
	}
	mustValidate(t, s)
}

// 场景：长期份额收敛——两个用户持续投作业，完成份额比收敛到权重比 1:2。
func TestUserWeightedFairnessLongTerm(t *testing.T) {
	t.Log("场景: 用户加权公平长期收敛 (1:2)")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	s.AddQueue(Queue{Name: "q", Weight: 1})
	s.SetUserWeight("q", "alice", 1)
	s.SetUserWeight("q", "bob", 2)

	done := map[string]int{"alice": 0, "bob": 0}
	seq := 0
	submit := func(user string) {
		mustSubmit(t, s, Job{ID: fmt.Sprintf("%s-%d", user, seq), User: user, Queue: "q", Request: cpu(1)})
		seq++
	}
	for i := 0; i < 8; i++ { // 双方始终保持充足 backlog
		submit("alice")
		submit("bob")
	}
	const ticks = 120
	for tick := 0; tick < ticks; tick++ {
		s.RunCycle()
		// 本 tick 所有在跑作业完成，补投新作业保持压力
		for _, e := range s.Events() {
			_ = e
		}
		for _, id := range runningJobs(s) {
			user := jobUser(s, id)
			s.Finish(id)
			done[user]++
			submit(user)
		}
	}
	total := done["alice"] + done["bob"]
	shareA := float64(done["alice"]) / float64(total)
	t.Logf("完成数 alice=%d bob=%d, alice 份额=%.3f (期望≈0.333)", done["alice"], done["bob"], shareA)
	if shareA < 0.30 || shareA > 0.37 {
		t.Fatalf("alice long-term share = %.3f, want ≈ 1/3", shareA)
	}
	mustValidate(t, s)
}

// 场景：单用户排队时独占队列——公平机制不影响只有一个用户的队列。
func TestSingleUserUnaffected(t *testing.T) {
	t.Log("场景: 单用户不受用户级公平影响")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(4), Healthy: true})
	for i := 0; i < 4; i++ {
		mustSubmit(t, s, Job{ID: "j" + itoa(i), User: "alice", Request: cpu(1)})
	}
	s.RunUntilSettled(8)
	for i := 0; i < 4; i++ {
		if st := state(t, s, "j"+itoa(i)); st != StateRunning {
			t.Fatalf("j%d should run, got %s", i, st)
		}
	}
	mustValidate(t, s)
}

// 场景：用户级公平与队列级公平叠加——队列间 1:1，队列内部再按用户分。
func TestQueueAndUserFairnessCompose(t *testing.T) {
	t.Log("场景: 队列公平与用户公平叠加")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(8), Healthy: true})
	s.AddQueue(Queue{Name: "qa", Weight: 1})
	s.AddQueue(Queue{Name: "qb", Weight: 1})
	s.SetUserWeight("qa", "alice", 1)
	s.SetUserWeight("qa", "erin", 3)
	for i := 0; i < 8; i++ {
		mustSubmit(t, s, Job{ID: "a" + itoa(i), User: "alice", Queue: "qa", Request: cpu(1)})
		mustSubmit(t, s, Job{ID: "e" + itoa(i), User: "erin", Queue: "qa", Request: cpu(1)})
		mustSubmit(t, s, Job{ID: "b" + itoa(i), User: "bob", Queue: "qb", Request: cpu(1)})
	}
	s.RunUntilSettled(64)
	cnt := map[string]int{}
	for i := 0; i < 8; i++ {
		for _, p := range []struct{ id, user string }{{"a", "alice"}, {"e", "erin"}, {"b", "bob"}} {
			if state(t, s, p.id+itoa(i)) == StateRunning {
				cnt[p.user]++
			}
		}
	}
	t.Logf("alice=%d erin=%d bob=%d", cnt["alice"], cnt["erin"], cnt["bob"])
	// 队列层：qa 共 4，qb 共 4；qa 内部 alice:erin = 1:3
	if cnt["alice"] != 1 || cnt["erin"] != 3 || cnt["bob"] != 4 {
		t.Fatalf("compose share = %v, want alice:1 erin:3 bob:4", cnt)
	}
	mustValidate(t, s)
}

// 场景：配置了额度但远未触顶时，队列公平等老结论不变（兼容性）。
func TestQuotaNotBindingKeepsOldBehavior(t *testing.T) {
	t.Log("场景: 额度不触顶时老行为不变")
	s := NewScheduler()
	s.AddNode(Node{ID: "n1", Capacity: cpu(8), Healthy: true})
	s.AddQueue(Queue{Name: "qa", Weight: 3})
	s.AddQueue(Queue{Name: "qb", Weight: 1})
	s.SetUserQuota("alice", cpu(100)) // 高额度，不触顶
	s.SetUserQuota("bob", cpu(100))
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
	if ra != 6 || rb != 2 {
		t.Fatalf("share = %d:%d, want 6:2 (与无额度时一致)", ra, rb)
	}
	mustValidate(t, s)
	t.Log("队列公平结论与无额度场景一致 (6:2)")
}

func runningJobs(s *Scheduler) []string {
	var out []string
	for id, j := range s.jobs {
		if j.state == StateRunning {
			out = append(out, id)
		}
	}
	return out
}

func jobUser(s *Scheduler, id string) string { return s.jobs[id].User }
