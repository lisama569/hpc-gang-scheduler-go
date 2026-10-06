package scheduler

// JobSnap 作业的可序列化状态。
type JobSnap struct {
	ID          string
	User        string
	Queue       string
	Priority    int
	Request     Resource
	GangSize    int
	Preemptible bool
	State       JobState
	Seq         int64
	Placed      map[string]int
	DeferWhy    DeferReason
}

// Snapshot 调度器完整可序列化状态（含用户额度占用）。
type Snapshot struct {
	Nodes       []Node
	Queues      []Queue
	Jobs        []JobSnap
	Pending     []string
	Quotas      map[string]Resource
	UserUsage   map[string]Resource
	UserWeights map[string]map[string]float64
	QueueVT     map[string]float64
	UserVT      map[string]map[string]float64
	Seq         int64
	Events      []Event
}

// Snapshot 导出当前状态。
func (s *Scheduler) Snapshot() *Snapshot {
	snap := &Snapshot{
		Quotas:      map[string]Resource{},
		UserUsage:   map[string]Resource{},
		UserWeights: map[string]map[string]float64{},
		QueueVT:     map[string]float64{},
		UserVT:      map[string]map[string]float64{},
	}
	for _, id := range s.nodeOrder {
		snap.Nodes = append(snap.Nodes, *s.nodes[id])
	}
	for _, name := range s.queueOrder {
		snap.Queues = append(snap.Queues, *s.queues[name])
	}
	for _, j := range s.jobs {
		placed := map[string]int{}
		for id, c := range j.placed {
			placed[id] = c
		}
		snap.Jobs = append(snap.Jobs, JobSnap{
			ID: j.ID, User: j.User, Queue: j.Queue, Priority: j.Priority,
			Request: j.Request, GangSize: j.GangSize, Preemptible: j.Preemptible,
			State: j.state, Seq: j.seq, Placed: placed, DeferWhy: j.deferWhy,
		})
	}
	for _, j := range s.pending {
		snap.Pending = append(snap.Pending, j.ID)
	}
	for u, q := range s.quotas {
		snap.Quotas[u] = q
	}
	for u, r := range s.userUsage {
		snap.UserUsage[u] = r
	}
	for q, m := range s.userWeights {
		snap.UserWeights[q] = map[string]float64{}
		for u, w := range m {
			snap.UserWeights[q][u] = w
		}
	}
	for q, vt := range s.queueVT {
		snap.QueueVT[q] = vt
	}
	for q, m := range s.userVT {
		snap.UserVT[q] = map[string]float64{}
		for u, vt := range m {
			snap.UserVT[q][u] = vt
		}
	}
	snap.Seq = s.seq
	snap.Events = s.Events()
	return snap
}

// Restore 从快照重建调度器，恢复后排队与额度结论应与原实例一致。
func Restore(snap *Snapshot) *Scheduler {
	s := NewScheduler()
	for _, n := range snap.Nodes {
		s.AddNode(n)
	}
	for _, q := range snap.Queues {
		s.AddQueue(q)
	}
	for u, q := range snap.Quotas {
		s.quotas[u] = q
	}
	for u, r := range snap.UserUsage {
		s.userUsage[u] = r
	}
	for q, m := range snap.UserWeights {
		for u, w := range m {
			s.SetUserWeight(q, u, w)
		}
	}
	for q, vt := range snap.QueueVT {
		s.queueVT[q] = vt
	}
	for q, m := range snap.UserVT {
		for u, vt := range m {
			if s.userVT[q] == nil {
				s.userVT[q] = map[string]float64{}
			}
			s.userVT[q][u] = vt
		}
	}
	s.seq = snap.Seq
	for _, js := range snap.Jobs {
		j := &Job{
			ID: js.ID, User: js.User, Queue: js.Queue, Priority: js.Priority,
			Request: js.Request, GangSize: js.GangSize, Preemptible: js.Preemptible,
			state: js.State, seq: js.Seq, deferWhy: js.DeferWhy,
			placed: map[string]int{},
		}
		for id, c := range js.Placed {
			j.placed[id] = c
		}
		s.jobs[j.ID] = j
	}
	for _, id := range snap.Pending {
		if j, ok := s.jobs[id]; ok {
			s.pending = append(s.pending, j)
		}
	}
	s.events = append(s.events, snap.Events...)
	return s
}
