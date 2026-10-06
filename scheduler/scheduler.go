package scheduler

import "fmt"

// Scheduler 核心调度器。
type Scheduler struct {
	nodes       map[string]*Node
	nodeOrder   []string
	queues      map[string]*Queue
	queueOrder  []string
	jobs        map[string]*Job
	pending     []*Job
	quotas      map[string]Resource // 用户额度上限；缺省=不受限
	userUsage   map[string]Resource
	userWeights map[string]map[string]float64 // queue -> user -> weight
	queueVT     map[string]float64            // 队列虚拟时间（队列间公平）
	userVT      map[string]map[string]float64 // queue -> user -> 虚拟时间（用户间公平）
	seq         int64
	events      []Event
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		nodes:       map[string]*Node{},
		queues:      map[string]*Queue{},
		jobs:        map[string]*Job{},
		quotas:      map[string]Resource{},
		userUsage:   map[string]Resource{},
		userWeights: map[string]map[string]float64{},
		queueVT:     map[string]float64{},
		userVT:      map[string]map[string]float64{},
	}
}

const defaultQueue = "default"

func (s *Scheduler) emit(kind, jobID, msg string) {
	s.events = append(s.events, Event{Kind: kind, JobID: jobID, Message: msg})
}

func (s *Scheduler) AddNode(n Node) {
	if _, ok := s.nodes[n.ID]; !ok {
		s.nodeOrder = append(s.nodeOrder, n.ID)
	}
	node := n
	s.nodes[n.ID] = &node
}

func (s *Scheduler) AddQueue(q Queue) {
	if q.Weight <= 0 {
		q.Weight = 1
	}
	if _, ok := s.queues[q.Name]; !ok {
		s.queueOrder = append(s.queueOrder, q.Name)
	}
	queue := q
	s.queues[q.Name] = &queue
}

func (s *Scheduler) SetUserQuota(user string, limit Resource) {
	s.quotas[user] = limit
}

func (s *Scheduler) SetUserWeight(queue, user string, w float64) {
	if w <= 0 {
		w = 1
	}
	if s.userWeights[queue] == nil {
		s.userWeights[queue] = map[string]float64{}
	}
	s.userWeights[queue][user] = w
}

func (s *Scheduler) Submit(j Job) error {
	if j.ID == "" {
		return fmt.Errorf("job id is empty")
	}
	if _, dup := s.jobs[j.ID]; dup {
		return fmt.Errorf("job %s already exists", j.ID)
	}
	if j.GangSize <= 0 {
		j.GangSize = 1
	}
	if j.Queue == "" {
		j.Queue = defaultQueue
	}
	if _, ok := s.queues[j.Queue]; !ok {
		s.AddQueue(Queue{Name: j.Queue, Weight: 1})
	}
	s.seq++
	job := j
	job.state = StatePending
	job.seq = s.seq
	job.placed = map[string]int{}
	s.jobs[j.ID] = &job
	s.pending = append(s.pending, &job)
	s.emit("submit", j.ID, fmt.Sprintf("user=%s queue=%s gang=%d req=%v", job.User, job.Queue, job.GangSize, job.Request))
	return nil
}

// totalCapacity 全部节点（含不健康）容量之和，作为主导份额的稳定分母。
func (s *Scheduler) totalCapacity() Resource {
	t := Resource{}
	for _, n := range s.nodes {
		t = t.Add(n.Capacity)
	}
	return t
}

// queueUsage 队列当前在跑作业总占用。
func (s *Scheduler) queueUsage(queue string) Resource {
	u := Resource{}
	for _, j := range s.jobs {
		if j.state == StateRunning && j.Queue == queue {
			u = u.Add(j.Total())
		}
	}
	return u
}

// userQueueUsage 用户在指定队列内的在跑占用。
func (s *Scheduler) userQueueUsage(queue, user string) Resource {
	u := Resource{}
	for _, j := range s.jobs {
		if j.state == StateRunning && j.Queue == queue && j.User == user {
			u = u.Add(j.Total())
		}
	}
	return u
}

// fitGang 在给定可用资源上尝试放置整个 gang，原子语义：要么全部放下要么不放。
func fitGang(j *Job, order []string, avail map[string]Resource) (map[string]int, bool) {
	sim := map[string]Resource{}
	for id, a := range avail {
		sim[id] = a
	}
	placed := map[string]int{}
	for w := 0; w < j.GangSize; w++ {
		ok := false
		for _, id := range order {
			a, exists := sim[id]
			if !exists {
				continue
			}
			if j.Request.Leq(a) {
				sim[id] = a.Sub(j.Request)
				placed[id]++
				ok = true
				break
			}
		}
		if !ok {
			return nil, false
		}
	}
	return placed, true
}

// healthyAvail 健康节点当前可用资源。
func (s *Scheduler) healthyAvail() map[string]Resource {
	avail := map[string]Resource{}
	for _, id := range s.nodeOrder {
		if n := s.nodes[id]; n.Healthy {
			avail[id] = n.Available()
		}
	}
	return avail
}

// allAvail 假设所有节点都健康时的可用资源（用于区分“节点不健康”与“资源不足”）。
func (s *Scheduler) allAvail() map[string]Resource {
	avail := map[string]Resource{}
	for _, id := range s.nodeOrder {
		n := s.nodes[id]
		avail[id] = n.Capacity.Sub(n.Used)
	}
	return avail
}

// admit 尝试启动一个作业：先查用户额度，再做 gang 原子放置，必要时尝试抢占。
func (s *Scheduler) admit(j *Job) bool {
	if !s.quotaAvailable(j.User, j.Total()) {
		j.deferWhy = ReasonUserQuotaExceeded
		s.emit("defer", j.ID, fmt.Sprintf("user %s quota exceeded: usage=%v need=%v", j.User, s.userUsage[j.User], j.Total()))
		return false
	}
	placed, ok := fitGang(j, s.nodeOrder, s.healthyAvail())
	if !ok {
		if _, fitIfHealthy := fitGang(j, s.nodeOrder, s.allAvail()); !fitIfHealthy {
			j.deferWhy = ReasonInsufficientResource
			s.emit("defer", j.ID, "insufficient cluster resource")
		} else {
			j.deferWhy = ReasonNodeUnhealthy
			s.emit("defer", j.ID, "not enough healthy nodes")
		}
		if !s.tryPreempt(j) {
			return false
		}
		placed, ok = fitGang(j, s.nodeOrder, s.healthyAvail())
		if !ok {
			return false
		}
	}
	for id, cnt := range placed {
		s.nodes[id].Used = s.nodes[id].Used.Add(j.Request.Mul(cnt))
	}
	j.placed = placed
	j.state = StateRunning
	j.deferWhy = ReasonNone
	s.chargeQuota(j.User, j.Total())
	s.advanceVT(j)
	s.removePending(j.ID)
	s.emit("start", j.ID, fmt.Sprintf("placed=%v", placed))
	return true
}

func (s *Scheduler) removePending(jobID string) {
	for i, p := range s.pending {
		if p.ID == jobID {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			return
		}
	}
}

// RunCycle 进行一轮调度：按队列公平→用户公平→优先级/FIFO 的顺序尝试启动排队作业。
func (s *Scheduler) RunCycle() {
	blocked := map[string]bool{}
	for {
		j := s.pickNext(blocked)
		if j == nil {
			return
		}
		if !s.admit(j) {
			blocked[j.ID] = true
		}
	}
}

// pickNext 选出下一个最值得给机会的排队作业：
// 队列间按队列虚拟时间，队列内用户间按用户虚拟时间，用户内部按优先级+FIFO。
func (s *Scheduler) pickNext(blocked map[string]bool) *Job {
	candQ := map[string]bool{}
	candU := map[string]map[string]bool{}
	backlogQ := map[string]bool{}
	backlogU := map[string]map[string]bool{}
	best := map[string]map[string]*Job{}
	for _, j := range s.pending {
		backlogQ[j.Queue] = true
		if backlogU[j.Queue] == nil {
			backlogU[j.Queue] = map[string]bool{}
		}
		backlogU[j.Queue][j.User] = true
		if blocked[j.ID] {
			continue
		}
		candQ[j.Queue] = true
		if candU[j.Queue] == nil {
			candU[j.Queue] = map[string]bool{}
		}
		candU[j.Queue][j.User] = true
		cur := best[j.Queue][j.User]
		if cur == nil || j.Priority > cur.Priority || (j.Priority == cur.Priority && j.seq < cur.seq) {
			if best[j.Queue] == nil {
				best[j.Queue] = map[string]*Job{}
			}
			best[j.Queue][j.User] = j
		}
	}
	if len(candQ) == 0 {
		return nil
	}
	s.lagAlign(backlogQ, backlogU)
	q := s.chooseQueue(candQ)
	if q == nil {
		return nil
	}
	u := s.chooseUser(q.Name, candU[q.Name])
	return best[q.Name][u]
}

func (s *Scheduler) RunUntilSettled(maxRounds int) {
	for i := 0; i < maxRounds; i++ {
		before := s.runningCount()
		s.RunCycle()
		if s.runningCount() == before {
			return
		}
	}
}

func (s *Scheduler) runningCount() int {
	c := 0
	for _, j := range s.jobs {
		if j.state == StateRunning {
			c++
		}
	}
	return c
}

// release 释放运行中作业占用的节点资源并归还用户额度，返回 false 表示作业不在运行（防重复归还）。
func (s *Scheduler) release(j *Job) bool {
	if j.state != StateRunning {
		return false
	}
	for id, cnt := range j.placed {
		s.nodes[id].Used = s.nodes[id].Used.Sub(j.Request.Mul(cnt))
	}
	s.refundQuota(j.User, j.Total())
	j.placed = map[string]int{}
	return true
}

func (s *Scheduler) Cancel(jobID string) bool {
	j, ok := s.jobs[jobID]
	if !ok {
		return false
	}
	switch j.state {
	case StatePending:
		s.removePending(jobID)
		j.state = StateCancelled
		s.emit("cancel", jobID, "cancelled while pending")
		return true
	case StateRunning:
		s.release(j)
		j.state = StateCancelled
		s.emit("cancel", jobID, "cancelled while running")
		return true
	}
	return false
}

func (s *Scheduler) Finish(jobID string) bool {
	j, ok := s.jobs[jobID]
	if !ok || j.state != StateRunning {
		return false // 重复/迟到的完成通知直接忽略，不会重复还额度
	}
	s.release(j)
	j.state = StateFinished
	s.emit("finish", jobID, "completed")
	return true
}

func (s *Scheduler) NodeDown(nodeID string) {
	n, ok := s.nodes[nodeID]
	if !ok || !n.Healthy {
		return
	}
	n.Healthy = false
	s.emit("node-down", "", "node "+nodeID+" down")
	// 整组失败：gang 作业任一 worker 在故障节点上，整个作业失败并释放全部资源
	var victims []*Job
	for _, j := range s.jobs {
		if j.state == StateRunning && j.placed[nodeID] > 0 {
			victims = append(victims, j)
		}
	}
	for _, j := range victims {
		s.release(j)
		j.state = StateFailed
		s.emit("fail", j.ID, "gang failed due to node "+nodeID+" down")
	}
}

func (s *Scheduler) NodeUp(nodeID string) {
	if n, ok := s.nodes[nodeID]; ok && !n.Healthy {
		n.Healthy = true
		s.emit("node-up", "", "node "+nodeID+" up")
	}
}

func (s *Scheduler) Explain(jobID string) (DeferReason, string) {
	j, ok := s.jobs[jobID]
	if !ok {
		return ReasonNone, "job not found"
	}
	switch j.deferWhy {
	case ReasonUserQuotaExceeded:
		limit := s.quotas[j.User]
		return ReasonUserQuotaExceeded, fmt.Sprintf("user %s quota exceeded: usage=%v limit=%v need=%v", j.User, s.userUsage[j.User], limit, j.Total())
	case ReasonInsufficientResource:
		return ReasonInsufficientResource, "insufficient cluster resource"
	case ReasonNodeUnhealthy:
		return ReasonNodeUnhealthy, "not enough healthy nodes"
	}
	return ReasonNone, ""
}

func (s *Scheduler) UserUsage(user string) Resource { return s.userUsage[user] }

func (s *Scheduler) JobState(jobID string) (JobState, bool) {
	j, ok := s.jobs[jobID]
	if !ok {
		return StatePending, false
	}
	return j.state, true
}

func (s *Scheduler) Events() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// Validate 资源守恒校验：节点占用、用户额度占用都必须与在跑作业一一对应。
func (s *Scheduler) Validate() error {
	nodeSum := map[string]Resource{}
	userSum := map[string]Resource{}
	for _, j := range s.jobs {
		if j.state != StateRunning {
			continue
		}
		total := Resource{}
		for id, cnt := range j.placed {
			nodeSum[id] = nodeSum[id].Add(j.Request.Mul(cnt))
			total = total.Add(j.Request.Mul(cnt))
		}
		if total != j.Total() {
			return fmt.Errorf("job %s placement %v != total %v", j.ID, total, j.Total())
		}
		userSum[j.User] = userSum[j.User].Add(total)
	}
	for id, n := range s.nodes {
		if nodeSum[id] != n.Used {
			return fmt.Errorf("node %s used=%v but placements sum=%v", id, n.Used, nodeSum[id])
		}
		if !n.Used.Leq(n.Capacity) {
			return fmt.Errorf("node %s over-allocated: used=%v capacity=%v", id, n.Used, n.Capacity)
		}
	}
	for user, u := range userSum {
		if s.userUsage[user] != u {
			return fmt.Errorf("user %s tracked usage=%v but running sum=%v", user, s.userUsage[user], u)
		}
	}
	for user, u := range s.userUsage {
		if userSum[user] != u {
			return fmt.Errorf("user %s tracked usage=%v but running sum=%v", user, u, userSum[user])
		}
		if limit, ok := s.quotas[user]; ok && !u.Leq(limit) {
			return fmt.Errorf("user %s over quota: usage=%v limit=%v", user, u, limit)
		}
	}
	return nil
}
