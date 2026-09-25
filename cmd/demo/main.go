package main

import (
	"fmt"
	"log"
	"os"

	gs "github.com/lisama569/hpc-gang-scheduler-go/scheduler"
)

func main() {
	logger := log.New(os.Stdout, "[sched] ", log.Ltime)
	s := gs.NewScheduler().WithLogger(logger)

	s.AddQueue(gs.QueueConfig{Name: "q1", Weight: 1, PreemptEnabled: true, ReservationThreshold: 3})

	// 4 台节点，2 个机架
	for _, n := range []*gs.Node{
		{ID: "n1", Rack: "r1", Total: gs.Resources{CPU: 8, Memory: 64, GPU: 2}},
		{ID: "n2", Rack: "r1", Total: gs.Resources{CPU: 8, Memory: 64, GPU: 2}},
		{ID: "n3", Rack: "r2", Total: gs.Resources{CPU: 8, Memory: 64, GPU: 2}},
		{ID: "n4", Rack: "r2", Total: gs.Resources{CPU: 8, Memory: 64, GPU: 2}},
	} {
		must(s.AddNode(n))
	}

	// gang A：4 成员，每成员 8 CPU（单机恰好放 1 个），需要全部 4 台节点
	must(s.Submit(&gs.Job{ID: "j1", GangID: "g1", User: "alice", Queue: "q1",
		Members: 4, Priority: 1, Request: gs.Resources{CPU: 8, Memory: 8}}))
	r := s.Schedule()
	fmt.Printf("round1 started=%v rejected=%v\n", r.StartedGangs, r.Rejected)

	// gang B：2 成员且要求 SameRack + GPU；资源被 A 占用 -> 拒绝原因可区分
	must(s.Submit(&gs.Job{ID: "j2", GangID: "g2", User: "bob", Queue: "q1",
		Members: 2, Priority: 1, Request: gs.Resources{CPU: 1, Memory: 4, GPU: 2},
		Constraint: gs.PlacementConstraint{SameRack: true}}))
	r = s.Schedule()
	fmt.Printf("round2 started=%v rejected=%v (j2 reason=%s)\n",
		r.StartedGangs, r.Rejected, reasonOf(s, "j2"))

	// 完成 gang A 后，gang B 应可启动
	must(s.NotifyComplete("j1", "evt-complete-j1"))
	r = s.Schedule()
	fmt.Printf("round3 started=%v rejected=%v\n", r.StartedGangs, r.Rejected)

	// 重复完成事件：幂等，不重复释放
	if err := s.NotifyComplete("j1", "evt-complete-j1"); err != gs.ErrDuplicateEvent {
		panic("expected duplicate event error")
	}
	fmt.Println("duplicate complete event ignored: OK")

	// 节点故障：gang B 整体失败，资源回收
	aff, err := s.NodeFaultAffected("n1", "evt-fault-n1")
	must(err)
	fmt.Printf("fault n1 affected jobs=%v\n", aff)
	fmt.Printf("ledger conservation after fault: %v\n", s.CheckConservation())

	// 重复故障事件：幂等
	if err := s.NodeFault("n1", "evt-fault-n1"); err != gs.ErrDuplicateEvent {
		panic("expected duplicate event error")
	}

	// 快照恢复
	snap := s.Snapshot()
	s2 := gs.Restore(snap)
	fmt.Printf("restored scheduler conservation: %v\n", s2.CheckConservation())

	// 高优先级抢占演示：低优先级作业占满资源，高优先级作业抢占
	s3 := gs.NewScheduler().WithLogger(logger)
	must(s3.AddNode(&gs.Node{ID: "x1", Rack: "r1", Total: gs.Resources{CPU: 4, Memory: 16}}))
	s3.AddQueue(gs.QueueConfig{Name: "q", PreemptEnabled: true})
	must(s3.Submit(&gs.Job{ID: "low", GangID: "glow", User: "u", Queue: "q",
		Members: 1, Priority: 1, Request: gs.Resources{CPU: 4}}))
	s3.Schedule()
	must(s3.Submit(&gs.Job{ID: "high", GangID: "ghigh", User: "u", Queue: "q",
		Members: 1, Priority: 9, Request: gs.Resources{CPU: 4}}))
	s3.Schedule()
	hj, _ := s3.Job("high")
	lj, _ := s3.Job("low")
	fmt.Printf("preemption: high=%s on=%v ; low=%s ; auditRecords=%d ; conservation=%v\n",
		hj.State, hj.NodeIDs, lj.State, len(s3.AuditLog()), s3.CheckConservation())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func reasonOf(s *gs.Scheduler, id string) gs.RejectReason {
	j, err := s.Job(id)
	if err != nil {
		return "missing"
	}
	return j.Reason
}
