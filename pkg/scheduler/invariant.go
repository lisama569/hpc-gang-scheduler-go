package scheduler

import "fmt"

// CheckInvariants 校验资源守恒：
//  1. 每个节点占用不超容量且非负；
//  2. 节点占用等于其上运行中作业的请求之和；
//  3. 用户额度占用等于该用户运行中作业的请求之和（用户维度守恒）；
//  4. 队列占用统计等于队列内运行中作业的请求之和；
//  5. gang 原子性：运行中作业的分配数恰好等于 GangSize。
//
// 返回 nil 表示全部通过，否则返回描述性错误。
func (s *Scheduler) CheckInvariants() error {
	nodeUsed := make(map[string]ResourceVec, len(s.nodes))
	userUsed := map[string]ResourceVec{}
	queueUsed := map[string]ResourceVec{}

	for _, j := range s.jobs {
		if j.State != JobRunning {
			if len(j.Assignments) != 0 {
				return fmt.Errorf("invariant: job %s is %s but holds %d assignments",
					j.ID, j.State, len(j.Assignments))
			}
			continue
		}
		if len(j.Assignments) != j.GangSize {
			return fmt.Errorf("invariant: job %s running with %d/%d assignments (gang violated)",
				j.ID, len(j.Assignments), j.GangSize)
		}
		for _, id := range j.Assignments {
			if _, ok := s.nodes[id]; !ok {
				return fmt.Errorf("invariant: job %s assigned to unknown node %s", j.ID, id)
			}
			nodeUsed[id] = nodeUsed[id].Add(j.Request)
		}
		total := j.TotalRequest()
		userUsed[j.User] = userUsed[j.User].Add(total)
		queueUsed[j.Queue] = queueUsed[j.Queue].Add(total)
	}

	for id, n := range s.nodes {
		if !n.Used.Fits(n.Capacity) {
			return fmt.Errorf("invariant: node %s used %+v exceeds capacity %+v", id, n.Used, n.Capacity)
		}
		if !clampNonNeg(n.Used).Fits(n.Used) {
			return fmt.Errorf("invariant: node %s used %+v is negative", id, n.Used)
		}
		if nodeUsed[id] != n.Used {
			return fmt.Errorf("invariant: node %s used %+v != sum of jobs %+v", id, n.Used, nodeUsed[id])
		}
	}

	users := map[string]bool{}
	for u := range userUsed {
		users[u] = true
	}
	for u := range s.quota.used {
		users[u] = true
	}
	for u := range users {
		if s.quota.Used(u) != userUsed[u] {
			return fmt.Errorf("invariant: user %s quota used %+v != sum of running jobs %+v",
				u, s.quota.Used(u), userUsed[u])
		}
		if limit, ok := s.quota.Limit(u); ok && !s.quota.Used(u).Fits(limit) {
			return fmt.Errorf("invariant: user %s quota used %+v exceeds limit %+v",
				u, s.quota.Used(u), limit)
		}
	}

	for name, q := range s.queues {
		if q.running != queueUsed[name] {
			return fmt.Errorf("invariant: queue %s running %+v != sum of jobs %+v",
				name, q.running, queueUsed[name])
		}
	}
	return nil
}
