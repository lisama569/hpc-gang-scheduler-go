package scheduler

import (
	"errors"
	"sort"
)

// ErrUnknownJob 表示作业不存在。
var ErrUnknownJob = errors.New("scheduler: unknown job")

// DefaultQueue 是未匹配到配置队列时使用的默认队列名。
const DefaultQueue = "default"

// Scheduler 是 gang 调度器核心。
type Scheduler struct {
	cfg    Config
	quota  *QuotaManager
	nodes  map[string]*Node
	queues map[string]*queueState
	jobs   map[string]*Job
	seq    int64
}

// queueState 保存单个队列的运行时状态。
type queueState struct {
	cfg      QueueConfig
	pending  []*Job             // 按 (优先级降序, 提交序号升序) 排序
	admitted map[string]float64 // user -> 历史累计准入量（用户级公平用）
	running  ResourceVec        // 队列当前占用
}

// New 创建调度器。
func New(cfg Config) *Scheduler {
	s := &Scheduler{
		cfg:    cfg,
		quota:  NewQuotaManager(cfg.UserQuotas),
		nodes:  make(map[string]*Node),
		queues: make(map[string]*queueState),
		jobs:   make(map[string]*Job),
	}
	for _, qc := range cfg.Queues {
		s.queues[qc.Name] = &queueState{cfg: qc, admitted: make(map[string]float64)}
	}
	return s
}

// queueFor 返回队列状态，未配置的队列按默认权重建一个（默认队列语义）。
func (s *Scheduler) queueFor(name string) *queueState {
	if name == "" {
		name = DefaultQueue
	}
	q, ok := s.queues[name]
	if !ok {
		q = &queueState{
			cfg:      QueueConfig{Name: name, Weight: 1},
			admitted: make(map[string]float64),
		}
		s.queues[name] = q
	}
	return q
}

// AddNode 注册一个节点。
func (s *Scheduler) AddNode(n Node) {
	n.Used = ResourceVec{}
	if n.Labels == nil {
		n.Labels = map[string]string{}
	}
	s.nodes[n.ID] = &n
}

// NodeFailed 标记节点故障：其上所有 gang 整组失败并释放资源与额度。
// 重复通知幂等。
func (s *Scheduler) NodeFailed(nodeID string) {
	n, ok := s.nodes[nodeID]
	if !ok || !n.Healthy {
		return
	}
	n.Healthy = false
	for _, j := range s.jobs {
		if j.State != JobRunning {
			continue
		}
		onNode := false
		for _, id := range j.Assignments {
			if id == nodeID {
				onNode = true
				break
			}
		}
		if !onNode {
			continue
		}
		// gang 语义：一个任务所在节点故障，整组失败。
		s.release(j)
		j.State = JobFailed
		j.Assignments = nil
	}
	// 故障节点上的占用随整组失败全部回收。
	n.Used = ResourceVec{}
	// 释放出来的额度与资源让排队作业马上有机会。
	s.RunCycle()
}

// NodeRecovered 标记节点恢复健康。
func (s *Scheduler) NodeRecovered(nodeID string) {
	if n, ok := s.nodes[nodeID]; ok {
		n.Healthy = true
	}
}

// Submit 提交作业进入排队。不会自动触发调度，需调用 RunCycle。
func (s *Scheduler) Submit(j Job) {
	if j.Queue == "" {
		j.Queue = DefaultQueue
	}
	if j.GangSize <= 0 {
		j.GangSize = 1
	}
	s.seq++
	j.SubmitSeq = s.seq
	j.State = JobPending
	j.Defer = ReasonNone
	j.Assignments = nil
	job := j
	s.jobs[j.ID] = &job
	q := s.queueFor(j.Queue)
	q.pending = insertSorted(q.pending, &job)
}

// insertSorted 按 (优先级降序, 提交序号升序) 插入。
func insertSorted(pending []*Job, j *Job) []*Job {
	i := sort.Search(len(pending), func(i int) bool {
		p := pending[i]
		if p.Priority != j.Priority {
			return p.Priority < j.Priority
		}
		return p.SubmitSeq > j.SubmitSeq
	})
	pending = append(pending, nil)
	copy(pending[i+1:], pending[i:])
	pending[i] = j
	return pending
}

// Cancel 取消作业：排队中直接移除，运行中释放资源与额度。幂等。
func (s *Scheduler) Cancel(jobID string) error {
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrUnknownJob
	}
	switch j.State {
	case JobPending:
		s.queueFor(j.Queue).removePending(j)
		j.State = JobCancelled
	case JobRunning:
		s.release(j)
		j.State = JobCancelled
		j.Assignments = nil
		s.RunCycle()
	}
	return nil
}

// Complete 标记作业正常完成并释放资源与额度；重复通知安全（幂等）。
func (s *Scheduler) Complete(jobID string) error {
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrUnknownJob
	}
	switch j.State {
	case JobPending:
		s.queueFor(j.Queue).removePending(j)
		j.State = JobCompleted
	case JobRunning:
		s.release(j)
		j.State = JobCompleted
		j.Assignments = nil
		s.RunCycle()
	}
	return nil
}

// release 释放运行中作业占用的节点资源、用户额度与队列统计。
// 仅在状态从 Running 迁出时调用一次，保证额度恰好归还一次。
func (s *Scheduler) release(j *Job) {
	for _, id := range j.Assignments {
		if n, ok := s.nodes[id]; ok {
			n.Used = n.Used.Sub(j.Request)
		}
	}
	total := j.TotalRequest()
	s.quota.Release(j.User, total)
	q := s.queueFor(j.Queue)
	q.running = q.running.Sub(total)
}

// RunCycle 执行一轮调度：按队列间公平顺序，各队列内按用户加权公平服务。
func (s *Scheduler) RunCycle() {
	total := s.totalCapacity()
	for _, q := range s.queueOrder(total) {
		s.scheduleQueue(q)
	}
}

// scheduleQueue 在单个队列内按用户加权公平尝试准入，直到没有可服务的用户。
// 同一用户内按 (优先级, 提交序) 逐个尝试，队首不可调度不阻塞其后续作业。
func (s *Scheduler) scheduleQueue(q *queueState) {
	skipped := map[string]bool{}
	for {
		user := q.pickUser(skipped)
		if user == "" {
			return
		}
		progressed := false
		for _, j := range q.userJobs(user) {
			if s.tryAdmit(q, j) {
				progressed = true
				break // 准入一个后重新做用户选择，把机会让给其他用户
			}
		}
		if !progressed {
			skipped[user] = true
		}
	}
}

// tryAdmit 尝试准入单个作业；失败时设置可观测的推迟原因并返回 false。
func (s *Scheduler) tryAdmit(q *queueState, j *Job) bool {
	need := j.TotalRequest()
	// 用户额度检查优先于资源检查，保证“卡在额度上”的原因可见。
	if !s.quota.CanAdmit(j.User, need) {
		j.Defer = ReasonUserQuotaExceeded
		return false
	}
	resvOK := s.fitsReservation(q, j)
	var asg []string
	var ok bool
	if resvOK {
		asg, ok = s.place(j)
	}
	if (!ok || !resvOK) && s.tryPreempt(j) && s.fitsReservation(q, j) {
		asg, ok = s.place(j)
	}
	if !ok || !s.fitsReservation(q, j) {
		j.Defer = s.classifyDefer(j)
		return false
	}
	s.admit(q, j, asg)
	return true
}

// admit 原子地启动整个 gang：节点资源、额度、队列统计一次性提交。
func (s *Scheduler) admit(q *queueState, j *Job, asg []string) {
	for _, id := range asg {
		s.nodes[id].Used = s.nodes[id].Used.Add(j.Request)
	}
	total := j.TotalRequest()
	s.quota.Allocate(j.User, total)
	q.running = q.running.Add(total)
	q.admitted[j.User] += serviceUnits(total)
	q.removePending(j)
	j.State = JobRunning
	j.Assignments = asg
	j.Defer = ReasonNone
}

// place 尝试为整个 gang 做首适应放置；要么返回全部节点的分配，要么失败，
// 不会在节点上留下部分分配（gang 原子性）。
func (s *Scheduler) place(j *Job) ([]string, bool) {
	used := make(map[string]ResourceVec, len(s.nodes))
	for id, n := range s.nodes {
		used[id] = n.Used
	}
	return placeOn(s.sortedNodes(), used, j)
}

// placeOn 在给定的节点占用视图上做首适应放置，供真实放置与抢占模拟共用。
func placeOn(nodes []*Node, used map[string]ResourceVec, j *Job) ([]string, bool) {
	asg := make([]string, 0, j.GangSize)
	for len(asg) < j.GangSize {
		placed := false
		for _, n := range nodes {
			if !n.Healthy || !matchLabels(n, j.NodeSelector) {
				continue
			}
			if j.Request.Fits(n.Capacity.Sub(used[n.ID])) {
				used[n.ID] = used[n.ID].Add(j.Request)
				asg = append(asg, n.ID)
				placed = true
				break
			}
		}
		if !placed {
			return nil, false
		}
	}
	return asg, true
}

// sortedNodes 返回按 ID 排序的节点列表，保证放置结果确定。
func (s *Scheduler) sortedNodes() []*Node {
	ids := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.nodes[id])
	}
	return out
}

// fitsReservation 检查准入 j 是否侵犯其他队列未用满的预留保障。
func (s *Scheduler) fitsReservation(q *queueState, j *Job) bool {
	total := s.totalCapacity()
	allowance := total
	for name, other := range s.queues {
		if name == q.cfg.Name {
			continue
		}
		short := clampNonNeg(other.cfg.Guaranteed.Sub(other.running))
		allowance = allowance.Sub(short)
	}
	return s.totalUsed().Add(j.TotalRequest()).Fits(allowance)
}

// classifyDefer 区分资源不足、约束不满足与节点不健康三类推迟原因。
func (s *Scheduler) classifyDefer(j *Job) DeferReason {
	anyMatch, anyHealthy := false, false
	for _, n := range s.nodes {
		if !matchLabels(n, j.NodeSelector) {
			continue
		}
		anyMatch = true
		if n.Healthy {
			anyHealthy = true
		}
	}
	if !anyMatch {
		return ReasonConstraintUnsatisfied
	}
	if !anyHealthy {
		return ReasonNoHealthyNode
	}
	return ReasonInsufficientResource
}

func (s *Scheduler) totalCapacity() ResourceVec {
	t := ResourceVec{}
	for _, n := range s.nodes {
		t = t.Add(n.Capacity)
	}
	return t
}

func (s *Scheduler) totalUsed() ResourceVec {
	t := ResourceVec{}
	for _, n := range s.nodes {
		t = t.Add(n.Used)
	}
	return t
}

func (q *queueState) removePending(j *Job) {
	for i, p := range q.pending {
		if p == j {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			return
		}
	}
}

// userJobs 返回该用户排队作业的有序快照（副本，遍历期间队列可安全变更）。
func (q *queueState) userJobs(user string) []*Job {
	var out []*Job
	for _, p := range q.pending {
		if p.User == user {
			out = append(out, p)
		}
	}
	return out
}

func matchLabels(n *Node, sel map[string]string) bool {
	for k, v := range sel {
		if n.Labels[k] != v {
			return false
		}
	}
	return true
}

func clampNonNeg(r ResourceVec) ResourceVec {
	if r.CPU < 0 {
		r.CPU = 0
	}
	if r.Mem < 0 {
		r.Mem = 0
	}
	if r.GPU < 0 {
		r.GPU = 0
	}
	return r
}

// serviceUnits 把资源量折算成公平性用的标量服务量。
func serviceUnits(r ResourceVec) float64 { return float64(r.CPU + r.Mem + r.GPU) }

// Job 返回作业当前状态副本。
func (s *Scheduler) Job(jobID string) (Job, bool) {
	j, ok := s.jobs[jobID]
	if !ok {
		return Job{}, false
	}
	cp := *j
	cp.Assignments = append([]string(nil), j.Assignments...)
	return cp, true
}

// PendingJobs 返回队列中排队作业的有序快照。
func (s *Scheduler) PendingJobs(queue string) []Job {
	q, ok := s.queues[queue]
	if !ok {
		return nil
	}
	out := make([]Job, 0, len(q.pending))
	for _, p := range q.pending {
		out = append(out, *p)
	}
	return out
}

// Quota 返回额度管理器（只读用途）。
func (s *Scheduler) Quota() *QuotaManager { return s.quota }
