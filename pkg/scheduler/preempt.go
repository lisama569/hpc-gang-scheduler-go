package scheduler

import "sort"

// tryPreempt 为无法放置的作业 j 尝试抢占更低优先级的运行中作业。
// 先在模拟视图上验证“驱逐某批受害者后 j 确实能放下”，
// 验证通过才真实驱逐（被抢占作业回到排队），否则不做任何改动。
// 返回是否已通过抢占腾出空间。
func (s *Scheduler) tryPreempt(j *Job) bool {
	victims := s.preemptibleVictims(j)
	if len(victims) == 0 {
		return false
	}
	nodes := s.sortedNodes()
	simUsed := make(map[string]ResourceVec, len(s.nodes))
	for id, n := range s.nodes {
		simUsed[id] = n.Used
	}
	simRunning := make(map[string]ResourceVec, len(s.queues))
	for name, q := range s.queues {
		simRunning[name] = q.running
	}
	simTotalUsed := s.totalUsed()

	for k, v := range victims {
		// 增量应用第 k 个受害者的释放。
		vTotal := v.TotalRequest()
		for _, id := range v.Assignments {
			simUsed[id] = simUsed[id].Sub(v.Request)
		}
		simRunning[v.Queue] = simRunning[v.Queue].Sub(vTotal)
		simTotalUsed = simTotalUsed.Sub(vTotal)

		// 模拟视图下验证放置与预留。
		placeView := make(map[string]ResourceVec, len(simUsed))
		for id, u := range simUsed {
			placeView[id] = u
		}
		if _, ok := placeOn(nodes, placeView, j); !ok {
			continue
		}
		if !s.fitsReservationSim(j.Queue, j.TotalRequest(), simRunning, simTotalUsed) {
			continue
		}
		// 验证通过：真实驱逐 victims[0..k]。
		for _, victim := range victims[:k+1] {
			s.evict(victim)
		}
		return true
	}
	return false
}

// preemptibleVictims 选出可被 j 抢占的运行中作业并排序：
// 所在队列已超出预留保障者优先，其次优先级更低者优先，再其次更晚提交者优先。
func (s *Scheduler) preemptibleVictims(j *Job) []*Job {
	var out []*Job
	for _, v := range s.jobs {
		if v.State != JobRunning || v.Priority >= j.Priority {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(a, b int) bool {
		oa := s.overGuarantee(out[a])
		ob := s.overGuarantee(out[b])
		if oa != ob {
			return oa // 超保障队列的作业先被抢占
		}
		if out[a].Priority != out[b].Priority {
			return out[a].Priority < out[b].Priority
		}
		return out[a].SubmitSeq > out[b].SubmitSeq
	})
	return out
}

func (s *Scheduler) overGuarantee(j *Job) bool {
	return overGuarantee(s.queueFor(j.Queue))
}

func overGuarantee(q *queueState) bool {
	g := q.cfg.Guaranteed
	r := q.running
	return r.CPU > g.CPU || r.Mem > g.Mem || r.GPU > g.GPU
}

// evict 把一个运行中作业抢占回排队：释放资源与额度，重新按序入队。
func (s *Scheduler) evict(j *Job) {
	s.release(j)
	j.State = JobPending
	j.Assignments = nil
	j.Defer = ReasonNone
	q := s.queueFor(j.Queue)
	q.pending = insertSorted(q.pending, j)
}

// fitsReservationSim 是 fitsReservation 的模拟版本，供抢占验证使用。
func (s *Scheduler) fitsReservationSim(queue string, need ResourceVec, running map[string]ResourceVec, totalUsed ResourceVec) bool {
	allowance := s.totalCapacity()
	for name, q := range s.queues {
		if name == queue {
			continue
		}
		short := clampNonNeg(q.cfg.Guaranteed.Sub(running[name]))
		allowance = allowance.Sub(short)
	}
	return totalUsed.Add(need).Fits(allowance)
}
