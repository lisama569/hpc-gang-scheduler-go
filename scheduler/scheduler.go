package scheduler

import (
	"sync"
	"time"
)

// Scheduler 是内存中的 HPC gang 调度核心，所有公开方法均在同一把互斥锁下执行，
// 因此多用户并发操作下调度结论确定、可复现。
type Scheduler struct {
	mu sync.Mutex

	nodes map[string]*Node
	jobs  map[string]*Job
	// gangID -> jobID，一个 gang 对应一个作业
	gangs map[string]string
	// 队列配置
	queues map[string]*QueueConfig
	// 已处理事件 ID（完成通知、节点事件共用），保证幂等
	events map[string]bool
	// 抢占/预留审计记录
	audit []AuditRecord

	log   Logger
	clock func() int64
}

// NewScheduler 创建空调度器。
func NewScheduler() *Scheduler {
	return &Scheduler{
		nodes:  map[string]*Node{},
		jobs:   map[string]*Job{},
		gangs:  map[string]string{},
		queues: map[string]*QueueConfig{},
		events: map[string]bool{},
		log:    nopLogger{},
		clock:  func() int64 { return time.Now().UnixMilli() },
	}
}

// WithLogger 注入日志器。
func (s *Scheduler) WithLogger(l Logger) *Scheduler {
	if l != nil {
		s.log = l
	}
	return s
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

func (s *Scheduler) now() int64 { return s.clock() }

// AddNode 注册一台节点；重复 ID 返回 ErrAlreadyExists。
func (s *Scheduler) AddNode(n *Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n == nil || n.ID == "" {
		return ErrInvalidJob
	}
	if _, ok := s.nodes[n.ID]; ok {
		return ErrAlreadyExists
	}
	cp := *n
	if cp.State == "" {
		cp.State = NodeHealthy
	}
	cp.Used = Resources{}
	s.log.Printf("node add: id=%s rack=%s total={cpu:%d mem:%d gpu:%d} state=%s",
		cp.ID, cp.Rack, cp.Total.CPU, cp.Total.Memory, cp.Total.GPU, cp.State)
	s.nodes[cp.ID] = &cp
	return nil
}

// Submit 提交一个 gang 作业：同一 GangID 的 Members 个成员必须原子启动。
func (s *Scheduler) Submit(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j == nil {
		return ErrInvalidJob
	}
	if j.ID == "" || j.GangID == "" || j.Members <= 0 {
		return ErrInvalidJob
	}
	if j.Request.CPU < 0 || j.Request.Memory < 0 || j.Request.GPU < 0 {
		return ErrInvalidJob
	}
	if _, ok := s.jobs[j.ID]; ok {
		return ErrAlreadyExists
	}
	if _, ok := s.gangs[j.GangID]; ok {
		return ErrAlreadyExists
	}
	cp := *j
	if cp.Queue == "" {
		cp.Queue = "default"
	}
	cp.State = StatePending
	cp.Reason = ReasonNone
	cp.NodeIDs = nil
	cp.SubmittedAt = s.now()
	if _, ok := s.queues[cp.Queue]; !ok {
		s.queues[cp.Queue] = &QueueConfig{Name: cp.Queue, Weight: 1}
	}
	if s.queues[cp.Queue].Closed {
		cp.State = StateRejected
		cp.Reason = ReasonQueueClosed
		cp.ReasonDetail = "queue " + cp.Queue + " is closed"
		cp.FinishedAt = s.now()
		s.log.Printf("job reject(submit): id=%s gang=%s queue=%s reason=%s",
			cp.ID, cp.GangID, cp.Queue, cp.Reason)
	}
	s.jobs[cp.ID] = &cp
	s.gangs[cp.GangID] = cp.ID
	s.log.Printf("job submit: id=%s gang=%s user=%s queue=%s members=%d prio=%d req={cpu:%d mem:%d gpu:%d} sameRack=%t nodes=%v",
		cp.ID, cp.GangID, cp.User, cp.Queue, cp.Members, cp.Priority,
		cp.Request.CPU, cp.Request.Memory, cp.Request.GPU, cp.Constraint.SameRack, cp.Constraint.Nodes)
	return nil
}

// Cancel 请求取消一个排队中或运行中的作业；对已取消作业的重复取消是幂等空操作。
func (s *Scheduler) Cancel(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	switch j.State {
	case StateCancelled:
		s.log.Printf("job cancel duplicate ignored: id=%s state=%s", j.ID, j.State)
		return nil
	case StateCompleted, StateFailed, StateRejected:
		return ErrTerminalState
	}
	if j.State == StateRunning {
		s.releaseResources(j)
	}
	j.State = StateCancelled
	j.FinishedAt = s.now()
	s.log.Printf("job cancel: id=%s gang=%s state=%s releasedNodes=%v", j.ID, j.GangID, j.State, j.NodeIDs)
	j.NodeIDs = nil
	return nil
}

// NotifyComplete 上报作业完成。eventID 用于重复/乱序事件去重：
// 同一 eventID 只会生效一次；对已结束作业的迟到通知不会造成重复扣款或状态回退。
func (s *Scheduler) NotifyComplete(jobID, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if eventID != "" {
		if s.events[eventID] {
			s.log.Printf("event duplicate ignored: event=%s job=%s", eventID, jobID)
			return ErrDuplicateEvent
		}
	}
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if j.State != StateRunning && j.State != StatePending {
		if eventID != "" {
			s.events[eventID] = true
		}
		s.log.Printf("complete late event ignored: event=%s job=%s state=%s", eventID, jobID, j.State)
		return ErrTerminalState
	}
	if eventID != "" {
		s.events[eventID] = true
	}
	if j.State == StateRunning {
		s.releaseResources(j)
	}
	j.State = StateCompleted
	j.FinishedAt = s.now()
	s.log.Printf("job complete: id=%s gang=%s nodes=%v", j.ID, j.GangID, j.NodeIDs)
	j.NodeIDs = nil
	return nil
}

// releaseResources 归还作业占用的全部资源，并校验不会超扣。
func (s *Scheduler) releaseResources(j *Job) {
	for _, nid := range j.NodeIDs {
		n := s.nodes[nid]
		if n == nil {
			continue
		}
		n.Used.CPU -= j.Request.CPU
		n.Used.Memory -= j.Request.Memory
		n.Used.GPU -= j.Request.GPU
		s.log.Printf("resource release: node=%s job=%s gang=%s nowUsed={cpu:%d mem:%d gpu:%d}",
			nid, j.ID, j.GangID, n.Used.CPU, n.Used.Memory, n.Used.GPU)
	}
}

// commitPlacement 原子提交一个 gang 的放置：一次性占用所有成员资源。
func (s *Scheduler) commitPlacement(j *Job, nodeIDs []string) {
	for _, nid := range nodeIDs {
		n := s.nodes[nid]
		n.Used.CPU += j.Request.CPU
		n.Used.Memory += j.Request.Memory
		n.Used.GPU += j.Request.GPU
		s.log.Printf("resource alloc: node=%s job=%s gang=%s nowUsed={cpu:%d mem:%d gpu:%d}",
			nid, j.ID, j.GangID, n.Used.CPU, n.Used.Memory, n.Used.GPU)
	}
	j.NodeIDs = append([]string(nil), nodeIDs...)
	j.State = StateRunning
	j.Reason = ReasonNone
	j.ReasonDetail = ""
	j.StartedAt = s.now()
}

// Schedule 执行一轮调度扫描：按公平顺序依次原子地尝试每个待调度 gang，
// 全部成员放得下才启动，否则记录可区分的拒绝原因；绝不部分启动。
func (s *Scheduler) Schedule() *ScheduleResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := &ScheduleResult{Rejected: map[string]RejectReason{}}
	// 每成功放置一个 gang 就重新计算下一个最优先的待调度 gang，
	// 使加权轮转的服务份额在同一轮内即时生效，避免同队列连续插队。
	deferred := map[string]bool{}
	for {
		next := s.nextPendingGang(deferred)
		if next == "" {
			break
		}
		gangID := next
		jobID := s.gangs[gangID]
		j := s.jobs[jobID]
		if j == nil || j.State != StatePending {
			deferred[gangID] = true
			continue
		}
		nodeIDs, reason, detail := s.tryPlaceGang(gangID)
		if reason != ReasonNone {
			// 高优先级/预留作业在资源类失败时可尝试受控抢占；
			// 拓扑/非法白名单类失败不触发抢占（抢占也无法解决）。
			if reason == ReasonInsufficientCPU || reason == ReasonInsufficientMem ||
				reason == ReasonInsufficientGPU || reason == ReasonConstraintUnsatis {
				if victims := s.attemptPreemption(j); len(victims) > 0 {
					nodeIDs, reason, detail = s.tryPlaceGang(gangID)
					// 被抢占回队的受害者本轮不再重复尝试，放到下一轮
					for _, vid := range victims {
						if vj, ok := s.jobs[vid]; ok {
							deferred[vj.GangID] = true
						}
					}
				}
			}
		}
		if reason != ReasonNone {
			j.Retries++
			j.Reason = reason
			j.ReasonDetail = detail
			res.Rejected[j.ID] = reason
			deferred[gangID] = true
			s.log.Printf("schedule defer: job=%s gang=%s retries=%d reason=%s detail=%s",
				j.ID, gangID, j.Retries, reason, detail)
			continue
		}
		// 成功启动：清除预留/抢占标记与推迟计数
		hadReserve := j.PreemptCount > 0
		j.Retries = 0
		j.PreemptCount = 0
		s.commitPlacement(j, nodeIDs)
		q := s.queueCfg(j.Queue)
		q.served += int64(j.Members)
		if hadReserve {
			s.audit = append(s.audit, AuditRecord{
				Seq: int64(len(s.audit) + 1), Time: s.now(), Kind: "reserve",
				GangID: j.GangID, JobID: j.ID, Queue: j.Queue,
				Reason: "reserved/preempted gang scheduled",
			})
		}
		res.StartedGangs = append(res.StartedGangs, gangID)
		s.log.Printf("gang start: job=%s gang=%s members=%d placement=%v queueServed=%d reserved=%t",
			j.ID, gangID, j.Members, j.NodeIDs, q.served, hadReserve)
	}
	return res
}

// Job 返回作业副本（只读快照），找不到返回 ErrNotFound。
func (s *Scheduler) Job(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *j
	cp.NodeIDs = append([]string(nil), j.NodeIDs...)
	return &cp, nil
}

// Node 返回节点副本。
func (s *Scheduler) Node(id string) (*Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *n
	return &cp, nil
}
