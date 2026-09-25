package scheduler

import "sort"

// QueueConfig 配置队列权重与公平份额（默认权重 1，即轮转）。
type QueueConfig struct {
	Name   string
	Weight int64
	Closed bool
	// PreemptEnabled 允许该队列中严格更高优先级的作业抢占严格更低优先级的运行中作业。
	PreemptEnabled bool
	// ReservationThreshold 等待（被推迟）次数达到该值的作业获得“预留”地位：
	// 即使没有空闲资源也排在最前，且可触发抢占；0 表示禁用预留。
	ReservationThreshold int
	// served 是该队列累计获得的服务槽位（运行时记账，不参与配置快照）。
	served int64
}

// AddQueue 注册/更新队列配置；未显式注册的队列按权重 1、轮转处理。
func (s *Scheduler) AddQueue(cfg QueueConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := cfg
	if c.Weight <= 0 {
		c.Weight = 1
	}
	s.queues[c.Name] = &c
	s.log.Printf("queue config: name=%s weight=%d preempt=%t reserveThreshold=%d closed=%t",
		c.Name, c.Weight, c.PreemptEnabled, c.ReservationThreshold, c.Closed)
}

// queueCfg 取队列配置（调用方持锁）。
func (s *Scheduler) queueCfg(name string) *QueueConfig {
	q, ok := s.queues[name]
	if !ok {
		q = &QueueConfig{Name: name, Weight: 1}
		s.queues[name] = q
	}
	return q
}

// orderedPendingGangs 按以下顺序给出本轮 gang 尝试顺序（调用方持锁）：
//  1. 预留地位（被抢占过 或 被推迟次数达到阈值）最优先，防止长期排队饿死；
//  2. 加权轮转的队列轮次（队列累计“已服务槽位”少者优先，按权重归一化）；
//  3. 队列内严格优先级高者优先；
//  4. 提交时间早者优先；
//  5. gangID 字典序兜底，保证并发提交下结论完全可复现。
func (s *Scheduler) orderedPendingGangs() []string {
	type entry struct {
		gangID string
		j      *Job
		q      *QueueConfig
	}
	entries := []entry{}
	for _, j := range s.jobs {
		if j.State != StatePending {
			continue
		}
		entries = append(entries, entry{gangID: j.GangID, j: j, q: s.queueCfg(j.Queue)})
	}
	sort.SliceStable(entries, func(a, b int) bool {
		ja, jb := entries[a].j, entries[b].j
		reserveA := ja.PreemptCount > 0 || (entries[a].q.ReservationThreshold > 0 && ja.Retries >= entries[a].q.ReservationThreshold)
		reserveB := jb.PreemptCount > 0 || (entries[b].q.ReservationThreshold > 0 && jb.Retries >= entries[b].q.ReservationThreshold)
		if reserveA != reserveB {
			return reserveA
		}
		// 加权轮转：已服务份额越少越优先
		svcA := float64(entries[a].q.served) / float64(entries[a].q.Weight)
		svcB := float64(entries[b].q.served) / float64(entries[b].q.Weight)
		if svcA != svcB {
			return svcA < svcB
		}
		if ja.Priority != jb.Priority {
			return ja.Priority > jb.Priority
		}
		if ja.SubmittedAt != jb.SubmittedAt {
			return ja.SubmittedAt < jb.SubmittedAt
		}
		return ja.GangID < jb.GangID
	})
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.gangID
	}
	return out
}

// nextPendingGang 在本轮已推迟集合之外，返回当前最应尝试的一个 gang；
// 没有候选时返回空串。每次放置成功后重新调用，使服务份额即时反映。
func (s *Scheduler) nextPendingGang(deferred map[string]bool) string {
	for _, gangID := range s.orderedPendingGangs() {
		if !deferred[gangID] {
			return gangID
		}
	}
	return ""
}
