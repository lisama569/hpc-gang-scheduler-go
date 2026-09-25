package scheduler

// Snapshot 是调度器某一时刻的完整可恢复快照。
type Snapshot struct {
	Nodes  []Node
	Jobs   []Job
	Queues []QueueConfig
	Events []string
	Audit  []AuditRecord
}

// Snapshot 导出深拷贝快照，可用于持久化恢复与离线守恒校验。
func (s *Scheduler) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := &Snapshot{}
	for _, n := range s.nodes {
		snap.Nodes = append(snap.Nodes, *n)
	}
	for _, j := range s.jobs {
		cp := *j
		cp.NodeIDs = append([]string(nil), j.NodeIDs...)
		cp.Constraint.Nodes = append([]string(nil), j.Constraint.Nodes...)
		snap.Jobs = append(snap.Jobs, cp)
	}
	for _, q := range s.queues {
		snap.Queues = append(snap.Queues, *q)
	}
	for e := range s.events {
		snap.Events = append(snap.Events, e)
	}
	snap.Audit = append(snap.Audit, s.AuditRecordList()...)
	return snap
}

// AuditRecordList 在持锁状态下返回复审记录副本。
func (s *Scheduler) AuditRecordList() []AuditRecord {
	out := make([]AuditRecord, len(s.audit))
	copy(out, s.audit)
	return out
}

// Restore 从快照恢复一个新调度器，恢复后的账本与原调度器等价。
func Restore(snap *Snapshot) *Scheduler {
	s := NewScheduler()
	if snap == nil {
		return s
	}
	for i := range snap.Nodes {
		n := snap.Nodes[i]
		s.nodes[n.ID] = &n
	}
	for i := range snap.Jobs {
		j := snap.Jobs[i]
		s.jobs[j.ID] = &j
		s.gangs[j.GangID] = j.ID
	}
	for i := range snap.Queues {
		q := snap.Queues[i]
		qq := q
		if qq.Weight <= 0 {
			qq.Weight = 1
		}
		s.queues[q.Name] = &qq
	}
	for _, e := range snap.Events {
		s.events[e] = true
	}
	s.audit = append(s.audit, snap.Audit...)
	return s
}

// CheckConservation 校验资源账本守恒：
// 每台节点的 Used 必须恰好等于所有运行中作业在该节点上的占用之和，
// 且任何节点不得超量分配、用量不得为负。任意操作序列后都应成立。
func (s *Scheduler) CheckConservation() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ledgerConservation()
}

func (s *Scheduler) ledgerConservation() bool {
	expect := map[string]Resources{}
	for _, n := range s.nodes {
		expect[n.ID] = Resources{}
	}
	for _, j := range s.jobs {
		if j.State != StateRunning {
			if len(j.NodeIDs) != 0 {
				return false // 非运行中作业不应仍持有节点
			}
			continue
		}
		if len(j.NodeIDs) != j.Members {
			return false // gang 运行中成员数必须等于占位记录数
		}
		for _, nid := range j.NodeIDs {
			n, ok := s.nodes[nid]
			if !ok {
				return false
			}
			if n.State == NodeFaulted {
				return false // 故障节点上不应有运行中成员（Drained 允许已有作业继续运行）
			}
			e := expect[nid]
			e.CPU += j.Request.CPU
			e.Memory += j.Request.Memory
			e.GPU += j.Request.GPU
			expect[nid] = e
		}
	}
	for id, n := range s.nodes {
		e := expect[id]
		if e != n.Used {
			return false
		}
		if n.Used.CPU < 0 || n.Used.Memory < 0 || n.Used.GPU < 0 {
			return false
		}
		if n.Used.CPU > n.Total.CPU || n.Used.Memory > n.Total.Memory || n.Used.GPU > n.Total.GPU {
			return false
		}
	}
	return true
}
