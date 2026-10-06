package scheduler

import "sort"

// queueOrder 按队列间公平返回本轮调度顺序：
// 队列主导份额 / 权重 低者优先，平手按队列名保证确定性。
func (s *Scheduler) queueOrder(total ResourceVec) []*queueState {
	qs := make([]*queueState, 0, len(s.queues))
	for _, q := range s.queues {
		if len(q.pending) > 0 {
			qs = append(qs, q)
		}
	}
	sort.Slice(qs, func(i, j int) bool {
		si := qs[i].running.DominantShare(total) / queueWeight(qs[i])
		sj := qs[j].running.DominantShare(total) / queueWeight(qs[j])
		if si != sj {
			return si < sj
		}
		return qs[i].cfg.Name < qs[j].cfg.Name
	})
	return qs
}

func queueWeight(q *queueState) float64 {
	if q.cfg.Weight <= 0 {
		return 1
	}
	return q.cfg.Weight
}

// pickUser 在队列内按用户加权公平选出下一个应获得机会的用户：
// 取 已服务量/权重 最小者，平手时服务量小者优先，再平手按用户名字典序。
// skipped 中的用户（本轮已确定无法准入）不参与选择。
func (q *queueState) pickUser(skipped map[string]bool) string {
	seen := map[string]bool{}
	best := ""
	bestRatio, bestAdmitted := 0.0, 0.0
	for _, p := range q.pending {
		u := p.User
		if skipped[u] || seen[u] {
			continue
		}
		seen[u] = true
		adm := q.admitted[u]
		ratio := adm / q.userWeight(u)
		if best == "" || ratio < bestRatio ||
			(ratio == bestRatio && adm < bestAdmitted) ||
			(ratio == bestRatio && adm == bestAdmitted && u < best) {
			best, bestRatio, bestAdmitted = u, ratio, adm
		}
	}
	return best
}

// userWeight 返回用户在队列内的权重，缺省为 1。
func (q *queueState) userWeight(user string) float64 {
	if q.cfg.UserWeights != nil {
		if w, ok := q.cfg.UserWeights[user]; ok && w > 0 {
			return w
		}
	}
	return 1
}
