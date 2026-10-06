package scheduler

// 公平性基于虚拟时间（virtual-time）加权公平排队：
// 每个队列、每个（队列内）用户各维护一个虚拟时间，调度一个作业就按
// “作业主导份额 / 权重” 推进自己的虚拟时间；每轮总是选虚拟时间最小的一方。
// 长期看各方的服务份额收敛到权重比。

// chooseQueue 在有待调度作业的队列中选虚拟时间最小者。
func (s *Scheduler) chooseQueue(active map[string]bool) *Queue {
	var best *Queue
	bestVT := 0.0
	for _, name := range s.queueOrder {
		if !active[name] {
			continue
		}
		vt := s.queueVT[name]
		if best == nil || vt < bestVT || (vt == bestVT && name < best.Name) {
			best, bestVT = s.queues[name], vt
		}
	}
	return best
}

// chooseUser 在队列内候选用户中选虚拟时间最小者。
func (s *Scheduler) chooseUser(queue string, active map[string]bool) string {
	best := ""
	bestVT := 0.0
	first := true
	for u := range active {
		vt := s.userVT[queue][u]
		if first || vt < bestVT || (vt == bestVT && u < best) {
			best, bestVT, first = u, vt, false
		}
	}
	return best
}

// lagAlign 把“完全没有排队作业”的队列/用户的虚拟时间提升到仍有 backlog 一方的最小值，
// 避免长期空闲后回来霸榜；只依据真实排队集合，与本轮临时阻塞无关。
func (s *Scheduler) lagAlign(activeQ map[string]bool, activeU map[string]map[string]bool) {
	if activeQ != nil {
		minVT, found := 0.0, false
		for name := range activeQ {
			if vt := s.queueVT[name]; !found || vt < minVT {
				minVT, found = vt, true
			}
		}
		if found {
			for name := range s.queues {
				if !activeQ[name] && s.queueVT[name] < minVT {
					s.queueVT[name] = minVT
				}
			}
		}
	}
	for q, users := range activeU {
		minVT, found := 0.0, false
		for u := range users {
			if vt := s.userVT[q][u]; !found || vt < minVT {
				minVT, found = vt, true
			}
		}
		if !found {
			continue
		}
		for u := range s.userVT[q] {
			if !users[u] && s.userVT[q][u] < minVT {
				s.userVT[q][u] = minVT
			}
		}
	}
}

// advanceVT 作业启动后推进队列与用户的虚拟时间。
func (s *Scheduler) advanceVT(j *Job) {
	share := j.Total().DominantShare(s.totalCapacity())
	if share <= 0 {
		return
	}
	q := s.queues[j.Queue]
	w := 1.0
	if q != nil && q.Weight > 0 {
		w = q.Weight
	}
	s.queueVT[j.Queue] += share / w
	if s.userVT[j.Queue] == nil {
		s.userVT[j.Queue] = map[string]float64{}
	}
	s.userVT[j.Queue][j.User] += share / s.userWeight(j.Queue, j.User)
}

func (s *Scheduler) userWeight(queue, user string) float64 {
	if m, ok := s.userWeights[queue]; ok {
		if w, ok := m[user]; ok && w > 0 {
			return w
		}
	}
	return 1
}
