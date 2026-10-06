package scheduler

import "sort"

// tryPreempt 尝试为 j 腾出资源：先在副本上模拟，确认能放下后才真正抢占，避免误杀。
// 受害作业回到排队队列，其占用的用户额度当场归还。
func (s *Scheduler) tryPreempt(j *Job) bool {
	q := s.queues[j.Queue]
	// 本队列当前用量低于预留量时，允许回收超用预留的其他队列
	underGuarantee := q != nil && !q.Guarantee.IsZero() && !q.Guarantee.Leq(s.queueUsage(j.Queue))

	var victims []*Job
	for _, c := range s.jobs {
		if c.state != StateRunning || !c.Preemptible || c.ID == j.ID {
			continue
		}
		if c.Priority < j.Priority {
			victims = append(victims, c) // 规则1：更低优先级的可抢占作业
			continue
		}
		if underGuarantee && c.Queue != j.Queue {
			cq := s.queues[c.Queue]
			if cq != nil && s.queueUsage(c.Queue).DominantShare(s.totalCapacity()) > cq.Guarantee.DominantShare(s.totalCapacity()) {
				victims = append(victims, c) // 规则2：本队列未用满预留，可回收超用队列的作业
			}
		}
	}
	if len(victims) == 0 {
		return false
	}
	sort.Slice(victims, func(a, b int) bool {
		if victims[a].Priority != victims[b].Priority {
			return victims[a].Priority < victims[b].Priority
		}
		return victims[a].seq > victims[b].seq // 同优先级新的先被抢
	})

	avail := s.healthyAvail()
	var chosen []*Job
	for _, v := range victims {
		// 模拟释放 victim（只加回健康节点上的部分）
		released := false
		for id, cnt := range v.placed {
			if a, ok := avail[id]; ok {
				avail[id] = a.Add(v.Request.Mul(cnt))
				released = true
			}
		}
		if released {
			chosen = append(chosen, v)
		}
		if _, ok := fitGang(j, s.nodeOrder, avail); ok {
			// 确认能放下，提交抢占
			for _, victim := range chosen {
				s.release(victim)
				victim.state = StatePending
				s.pending = append(s.pending, victim)
				s.emit("preempt", victim.ID, "preempted by "+j.ID)
			}
			return true
		}
	}
	return false
}
