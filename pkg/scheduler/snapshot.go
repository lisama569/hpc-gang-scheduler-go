package scheduler

// Snapshot 是调度器完整状态的可序列化快照。
type Snapshot struct {
	Nodes  []Node
	Jobs   []Job
	Queues []QueueSnapshot
	Quota  QuotaSnapshot
	Seq    int64
}

// QueueSnapshot 保存单个队列的公平性计数与占用，
// 保证恢复后用户级/队列级公平结论与恢复前一致。
type QueueSnapshot struct {
	Name     string
	Admitted map[string]float64
	Running  ResourceVec
}

// Snapshot 导出当前状态。
func (s *Scheduler) Snapshot() Snapshot {
	snap := Snapshot{
		Nodes: make([]Node, 0, len(s.nodes)),
		Jobs:  make([]Job, 0, len(s.jobs)),
		Quota: s.quota.Snapshot(),
		Seq:   s.seq,
	}
	for _, id := range sortedKeys(s.nodes) {
		snap.Nodes = append(snap.Nodes, *s.nodes[id])
	}
	for _, id := range sortedKeys(s.jobs) {
		j := *s.jobs[id]
		j.Assignments = append([]string(nil), j.Assignments...)
		snap.Jobs = append(snap.Jobs, j)
	}
	for _, name := range sortedKeys(s.queues) {
		q := s.queues[name]
		adm := make(map[string]float64, len(q.admitted))
		for u, a := range q.admitted {
			adm[u] = a
		}
		snap.Queues = append(snap.Queues, QueueSnapshot{
			Name:     name,
			Admitted: adm,
			Running:  q.running,
		})
	}
	return snap
}

// Restore 从快照恢复调度器状态；恢复后用户额度与排队结论与恢复前一致。
func Restore(cfg Config, snap Snapshot) *Scheduler {
	s := New(cfg)
	for _, n := range snap.Nodes {
		node := n
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		s.nodes[node.ID] = &node
	}
	for _, qs := range snap.Queues {
		q := s.queueFor(qs.Name)
		for u, a := range qs.Admitted {
			q.admitted[u] = a
		}
		q.running = qs.Running
	}
	for _, j := range snap.Jobs {
		job := j
		job.Assignments = append([]string(nil), j.Assignments...)
		s.jobs[job.ID] = &job
		if job.State == JobPending {
			q := s.queueFor(job.Queue)
			q.pending = insertSorted(q.pending, &job)
		}
	}
	s.quota.Restore(snap.Quota)
	s.seq = snap.Seq
	return s
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
