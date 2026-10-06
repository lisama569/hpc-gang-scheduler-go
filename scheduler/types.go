package scheduler

// Resource 表示一类可调度资源，三个维度可独立配置。
type Resource struct {
	CPU   int64
	MemGB int64
	GPU   int64
}

func (r Resource) Add(o Resource) Resource {
	return Resource{CPU: r.CPU + o.CPU, MemGB: r.MemGB + o.MemGB, GPU: r.GPU + o.GPU}
}

func (r Resource) Sub(o Resource) Resource {
	return Resource{CPU: r.CPU - o.CPU, MemGB: r.MemGB - o.MemGB, GPU: r.GPU - o.GPU}
}

func (r Resource) Mul(n int) Resource {
	return Resource{CPU: r.CPU * int64(n), MemGB: r.MemGB * int64(n), GPU: r.GPU * int64(n)}
}

// Leq 判断 r 的每个维度都不超过 o。
func (r Resource) Leq(o Resource) bool {
	return r.CPU <= o.CPU && r.MemGB <= o.MemGB && r.GPU <= o.GPU
}

func (r Resource) IsZero() bool { return r.CPU == 0 && r.MemGB == 0 && r.GPU == 0 }

// DominantShare 返回 r 相对 total 的主导份额（各维度占比的最大值）。
func (r Resource) DominantShare(total Resource) float64 {
	max := 0.0
	if total.CPU > 0 {
		if s := float64(r.CPU) / float64(total.CPU); s > max {
			max = s
		}
	}
	if total.MemGB > 0 {
		if s := float64(r.MemGB) / float64(total.MemGB); s > max {
			max = s
		}
	}
	if total.GPU > 0 {
		if s := float64(r.GPU) / float64(total.GPU); s > max {
			max = s
		}
	}
	return max
}

// JobState 作业生命周期状态。
type JobState int

const (
	StatePending JobState = iota
	StateRunning
	StateFinished
	StateFailed
	StateCancelled
)

func (s JobState) String() string {
	switch s {
	case StatePending:
		return "Pending"
	case StateRunning:
		return "Running"
	case StateFinished:
		return "Finished"
	case StateFailed:
		return "Failed"
	case StateCancelled:
		return "Cancelled"
	}
	return "Unknown"
}

// DeferReason 作业本轮未能启动的原因。
type DeferReason int

const (
	ReasonNone                 DeferReason = iota
	ReasonInsufficientResource             // 集群空闲资源不足
	ReasonNodeUnhealthy                    // 没有足够健康节点
	ReasonUserQuotaExceeded                // 卡在用户额度上
)

func (d DeferReason) String() string {
	switch d {
	case ReasonNone:
		return "None"
	case ReasonInsufficientResource:
		return "InsufficientResource"
	case ReasonNodeUnhealthy:
		return "NodeUnhealthy"
	case ReasonUserQuotaExceeded:
		return "UserQuotaExceeded"
	}
	return "Unknown"
}

// Job 一个 gang 作业：GangSize 个 worker，每个 worker 占 Request 资源，要么全起要么不起。
type Job struct {
	ID          string
	User        string
	Queue       string
	Priority    int
	Request     Resource // 单个 worker 的资源
	GangSize    int
	Preemptible bool

	state    JobState
	seq      int64          // 提交序号，FIFO  tiebreak
	placed   map[string]int // nodeID -> worker 数
	deferWhy DeferReason
}

// Total 整个 gang 的总资源需求。
func (j *Job) Total() Resource { return j.Request.Mul(j.GangSize) }

func (j *Job) State() JobState { return j.state }

// Node 集群节点。
type Node struct {
	ID       string
	Capacity Resource
	Used     Resource
	Healthy  bool
}

func (n *Node) Available() Resource {
	if !n.Healthy {
		return Resource{}
	}
	return n.Capacity.Sub(n.Used)
}

// Queue 队列，带权重（队列间公平）与预留资源。
type Queue struct {
	Name      string
	Weight    float64
	Guarantee Resource // 预留：该队列最少应能拿到的资源
}

// Event 调度器事件，供日志与测试断言。
type Event struct {
	Kind    string // submit/start/finish/cancel/fail/preempt/node-down/node-up/defer
	JobID   string
	Message string
}
