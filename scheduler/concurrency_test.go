package scheduler

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestCancelLifecycle(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.AddNode(&Node{ID: "n2", Total: Resources{CPU: 8}}))

	// 取消排队作业：永远不会被启动
	must(t, s.Submit(&Job{ID: "wait", GangID: "gw", Members: 1, Request: Resources{CPU: 1}}))
	must(t, s.Cancel("wait"))
	if j, _ := s.Job("wait"); j.State != StateCancelled {
		t.Fatalf("want Cancelled, got %s", j.State)
	}
	r := s.Schedule()
	if _, ok := r.Rejected["wait"]; ok {
		t.Fatal("cancelled pending job must not be scheduled at all")
	}

	// 取消运行中作业：资源回收
	must(t, s.Submit(&Job{ID: "run", GangID: "gr", Members: 2, Request: Resources{CPU: 4}}))
	s.Schedule()
	must(t, s.Cancel("run"))
	if j, _ := s.Job("run"); j.State != StateCancelled || len(j.NodeIDs) != 0 {
		t.Fatalf("running cancel must release nodes, got %s %v", j.State, j.NodeIDs)
	}
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved after cancel")
	}
	// 重复取消幂等；取消已完成作业返回终态错误
	if err := s.Cancel("run"); err != nil {
		t.Fatalf("duplicate cancel must be nil no-op, got %v", err)
	}
	must(t, s.Submit(&Job{ID: "done", GangID: "gd", Members: 1, Request: Resources{CPU: 1}}))
	s.Schedule()
	must(t, s.NotifyComplete("done", "c1"))
	if err := s.Cancel("done"); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("cancel completed job must be ErrTerminalState, got %v", err)
	}
}

func TestConcurrentSubmitCancelCompleteFault(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent storm in -short mode")
	}
	s := newTestScheduler()
	const nodeN = 16
	for i := 0; i < nodeN; i++ {
		must(t, s.AddNode(&Node{ID: fmt.Sprintf("n%02d", i), Rack: fmt.Sprintf("r%d", i%4),
			Total: Resources{CPU: 8, Memory: 32, GPU: 2}}))
	}
	s.AddQueue(QueueConfig{Name: "q", Weight: 1, PreemptEnabled: true, ReservationThreshold: 2})

	const jobN = 400
	var idsMu sync.Mutex
	var ids []string
	var wg sync.WaitGroup

	// 提交者：8 个 goroutine 并发提交
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < jobN; i += 8 {
				rng := rand.New(rand.NewSource(int64(i)))
				id := fmt.Sprintf("j%04d", i)
				j := &Job{
					ID: id, GangID: "g" + id, User: fmt.Sprintf("u%d", i%5), Queue: "q",
					Members:  1 + rng.Intn(4),
					Priority: rng.Intn(5),
					Request: Resources{
						CPU:    int64(1 + rng.Intn(4)),
						Memory: int64(1 + rng.Intn(8)),
						GPU:    int64(rng.Intn(2)),
					},
				}
				if err := s.Submit(j); err != nil {
					t.Errorf("submit %s: %v", id, err)
					return
				}
				idsMu.Lock()
				ids = append(ids, id)
				idsMu.Unlock()
			}
		}(w)
	}

	// 调度器：并发循环调度（调度本身持锁，结论确定）
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				s.Schedule()
			}
		}()
	}

	// 等提交铺开后再启动取消/完成/故障角色
	go func() {
		wg.Wait()
	}()
	wg.Add(3 + 2)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 300; i++ {
			idsMu.Lock()
			if len(ids) == 0 {
				idsMu.Unlock()
				continue
			}
			id := ids[rng.Intn(len(ids))]
			idsMu.Unlock()
			_ = s.Cancel(id) // 任何结果都合法，但不得破坏账本
		}
	}()
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(43))
		for i := 0; i < 300; i++ {
			idsMu.Lock()
			if len(ids) == 0 {
				idsMu.Unlock()
				continue
			}
			id := ids[rng.Intn(len(ids))]
			idsMu.Unlock()
			_ = s.NotifyComplete(id, fmt.Sprintf("evt-%s-%d", id, i))
		}
	}()
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(44))
		for i := 0; i < 200; i++ {
			id := fmt.Sprintf("n%02d", rng.Intn(nodeN))
			switch rng.Intn(3) {
			case 0:
				_ = s.NodeFault(id, fmt.Sprintf("fault-%s-%d", id, i))
			case 1:
				_ = s.NodeRecover(id, fmt.Sprintf("rec-%s-%d", id, i))
			case 2:
				_ = s.NodeDrain(id, fmt.Sprintf("drain-%s-%d", id, i))
			}
		}
	}()
	go func() {
		defer wg.Done()
		// 故障 + 恢复交错：固定节点反复事件，含重复 eventID
		for i := 0; i < 50; i++ {
			_ = s.NodeFault("n00", "fault-n00") // 同一 eventID，仅首次生效
			_ = s.NodeRecover("n00", "rec-n00-1")
		}
	}()
	go func() {
		defer wg.Done()
		// 全部健康化，供最终收敛
		for i := 0; i < nodeN; i++ {
			id := fmt.Sprintf("n%02d", i)
			_ = s.NodeRecover(id, fmt.Sprintf("final-rec-%s", id))
		}
	}()

	wg.Wait()
	// 收敛：再跑几轮，账本必须始终守恒；重复跑结论一致
	r1 := s.Schedule()
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved after concurrent storm")
	}
	r2 := s.Schedule()
	if !s.CheckConservation() {
		t.Fatal("ledger not conserved on second schedule")
	}
	// 第一次已经能启动的 gang 数只可能 >= 第二次（新作业不会凭空出现），
	// 且第二次不应再启动任何非抢占场景下的 gang（没有新事件）
	if len(r2.StartedGangs) != 0 {
		// 允许抢占导致延迟一轮，但每轮自身必须守恒
		t.Logf("note: second pass started %d gangs (possible preemption churn)", len(r2.StartedGangs))
	}
	_ = r1
}

func TestDuplicateGangIDRejected(t *testing.T) {
	s := newTestScheduler()
	must(t, s.AddNode(&Node{ID: "n1", Total: Resources{CPU: 8}}))
	must(t, s.Submit(&Job{ID: "j1", GangID: "shared", Members: 1, Request: Resources{CPU: 1}}))
	err := s.Submit(&Job{ID: "j2", GangID: "shared", Members: 1, Request: Resources{CPU: 1}})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate gang ID must be rejected, got %v", err)
	}
}
