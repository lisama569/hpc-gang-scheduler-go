package scheduler

import "sort"

// feasibleOn 用一份“影子”节点视图判断：在给定可用资源映射上，目标 gang 能否放置。
// 影子视图不修改真实状态，因此抢占方案只有在验证成功后才会真正生效。
func (s *Scheduler) feasibleOn(j *Job, free map[string]Resources) bool {
	var cands []*Node
	shadowUsed := map[string]Resources{}
	build := func(n *Node) *Node {
		sn := *n
		if f, ok := free[n.ID]; ok {
			sn.Used = Resources{
				CPU:    n.Total.CPU - f.CPU,
				Memory: n.Total.Memory - f.Memory,
				GPU:    n.Total.GPU - f.GPU,
			}
		}
		shadowUsed[n.ID] = sn.Used
		return &sn
	}
	if len(j.Constraint.Nodes) > 0 {
		for _, nid := range j.Constraint.Nodes {
			n, ok := s.nodes[nid]
			if !ok {
				return false
			}
			cands = append(cands, build(n))
		}
	} else {
		for _, n := range s.nodes {
			cands = append(cands, build(n))
		}
	}
	healthy := cands[:0:0]
	for _, n := range cands {
		if n.State == NodeHealthy {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) == 0 {
		return false
	}
	capFor := func(nodes []*Node) int64 {
		var total int64
		for _, n := range nodes {
			total += memberCapacity(available(n), j.Request)
		}
		return total
	}
	if j.Constraint.SameRack {
		racks := map[string][]*Node{}
		for _, n := range healthy {
			racks[n.Rack] = append(racks[n.Rack], n)
		}
		for _, in := range racks {
			if capFor(in) >= int64(j.Members) {
				return true
			}
		}
		return false
	}
	return capFor(healthy) >= int64(j.Members)
}

// currentFree 计算当前各健康节点的空闲资源。
func (s *Scheduler) currentFree() map[string]Resources {
	out := map[string]Resources{}
	for id, n := range s.nodes {
		out[id] = available(n)
	}
	return out
}

// attemptPreemption 尝试通过抢占为排队作业 j 腾出原子放置空间。
// 规则（可控、可审计）：
//   - 仅当 j 所在队列启用抢占，且 j 为高优先级或已获得预留地位时才允许；
//   - 受害者必须与 j 同队列、正在运行、优先级严格更低；
//   - 按 (优先级升序, 已运行时长最长, 作业ID) 确定性选择受害者；
//   - 每抢占一个就在影子视图上重新验证；任何序列都放不下则不抢占、不修改状态。
func (s *Scheduler) attemptPreemption(j *Job) []string {
	q := s.queueCfg(j.Queue)
	reserved := j.PreemptCount > 0 || (q.ReservationThreshold > 0 && j.Retries >= q.ReservationThreshold)
	if !q.PreemptEnabled || !(j.Priority > 0 || reserved) {
		return nil
	}
	var victims []*Job
	for _, v := range s.jobs {
		if v.State == StateRunning && v.Queue == j.Queue && v.Priority < j.Priority {
			victims = append(victims, v)
		}
	}
	sort.Slice(victims, func(a, b int) bool {
		va, vb := victims[a], victims[b]
		if va.Priority != vb.Priority {
			return va.Priority < vb.Priority
		}
		if va.StartedAt != vb.StartedAt {
			return va.StartedAt < vb.StartedAt
		}
		return va.ID < vb.ID
	})

	free := s.currentFree()
	var chosen []*Job
	for _, v := range victims {
		if s.feasibleOn(j, free) {
			break
		}
		// 影子视图中先归还该受害者资源
		for _, nid := range v.NodeIDs {
			f := free[nid]
			f.CPU += v.Request.CPU
			f.Memory += v.Request.Memory
			f.GPU += v.Request.GPU
			free[nid] = f
		}
		chosen = append(chosen, v)
		if s.feasibleOn(j, free) {
			break
		}
	}
	if !s.feasibleOn(j, free) {
		s.log.Printf("preempt abort: job=%s gang=%s no victim sequence yields placement", j.ID, j.GangID)
		return nil
	}
	if len(chosen) == 0 {
		return nil
	}
	// 真正生效：受害者释放资源、回到排队并获得预留提升
	victimIDs := make([]string, 0, len(chosen))
	for _, v := range chosen {
		s.releaseResources(v)
		v.State = StatePending
		v.Reason = ReasonNone
		v.ReasonDetail = ""
		v.PreemptCount++
		v.NodeIDs = nil
		v.FinishedAt = 0
		victimIDs = append(victimIDs, v.ID)
		s.audit = append(s.audit, AuditRecord{
			Seq:       int64(len(s.audit) + 1),
			Time:      s.now(),
			Kind:      "preempt",
			GangID:    j.GangID,
			JobID:     j.ID,
			Queue:     j.Queue,
			VictimJob: v.ID,
			Reason:    "higher-priority or reserved gang requires resources",
		})
		s.log.Printf("preempt: victim=%s by=%s gang=%s victimPrio=%d prio=%d preemptCount(victim)=%d",
			v.ID, j.ID, j.GangID, v.Priority, j.Priority, v.PreemptCount)
	}
	return victimIDs
}

// AuditLog 返回审计记录副本。
func (s *Scheduler) AuditLog() []AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditRecord, len(s.audit))
	copy(out, s.audit)
	return out
}
