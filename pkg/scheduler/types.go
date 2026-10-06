// Package scheduler 实现一个 HPC gang 调度器核心：
// 排队、gang 原子启动、取消、节点故障回收、队列间公平、
// 优先级、预留、抢占，以及按用户的并发额度与用户级公平。
package scheduler

// ResourceVec 表示一组资源维度上的数量（CPU 核、内存 MB、GPU 卡）。
type ResourceVec struct {
	CPU int64
	Mem int64
	GPU int64
}

// Add 返回逐维度相加的结果。
func (r ResourceVec) Add(o ResourceVec) ResourceVec {
	return ResourceVec{CPU: r.CPU + o.CPU, Mem: r.Mem + o.Mem, GPU: r.GPU + o.GPU}
}

// Sub 返回逐维度相减的结果。
func (r ResourceVec) Sub(o ResourceVec) ResourceVec {
	return ResourceVec{CPU: r.CPU - o.CPU, Mem: r.Mem - o.Mem, GPU: r.GPU - o.GPU}
}

// Mul 返回逐维度乘以 n 的结果。
func (r ResourceVec) Mul(n int) ResourceVec {
	return ResourceVec{CPU: r.CPU * int64(n), Mem: r.Mem * int64(n), GPU: r.GPU * int64(n)}
}

// Fits 报告 r 是否在每个维度上都不超过 o。
func (r ResourceVec) Fits(o ResourceVec) bool {
	return r.CPU <= o.CPU && r.Mem <= o.Mem && r.GPU <= o.GPU
}

// IsZero 报告是否所有维度都为 0。
func (r ResourceVec) IsZero() bool {
	return r.CPU == 0 && r.Mem == 0 && r.GPU == 0
}

// DominantShare 返回 r 相对 total 的最大维度占比（total 为 0 的维度按 0 处理）。
func (r ResourceVec) DominantShare(total ResourceVec) float64 {
	max := 0.0
	if total.CPU > 0 {
		if s := float64(r.CPU) / float64(total.CPU); s > max {
			max = s
		}
	}
	if total.Mem > 0 {
		if s := float64(r.Mem) / float64(total.Mem); s > max {
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

// JobState 是作业的生命周期状态。
type JobState int

const (
	JobPending   JobState = iota // 排队中
	JobRunning                   // 运行中（gang 已全部启动）
	JobCompleted                 // 正常完成
	JobCancelled                 // 被取消
	JobFailed                    // 失败（如节点故障整组失败）
)

func (s JobState) String() string {
	switch s {
	case JobPending:
		return "Pending"
	case JobRunning:
		return "Running"
	case JobCompleted:
		return "Completed"
	case JobCancelled:
		return "Cancelled"
	case JobFailed:
		return "Failed"
	}
	return "Unknown"
}

// DeferReason 说明一个排队作业本轮没有被调度的原因。
type DeferReason string

const (
	ReasonNone                  DeferReason = "" // 未推迟
	ReasonInsufficientResource  DeferReason = "InsufficientResource"
	ReasonConstraintUnsatisfied DeferReason = "ConstraintUnsatisfied"
	ReasonNoHealthyNode         DeferReason = "NoHealthyNode"
	ReasonUserQuotaExceeded     DeferReason = "UserQuotaExceeded" // 卡在用户额度上
	ReasonFairShare             DeferReason = "FairShare"         // 公平性轮到别人
)

// Job 是一个 gang 作业：GangSize 个任务要么全部启动，要么一个都不启动。
type Job struct {
	ID           string
	User         string
	Queue        string
	Priority     int               // 数值越大优先级越高
	GangSize     int               // gang 内任务数
	Request      ResourceVec       // 单个任务的资源请求
	NodeSelector map[string]string // 节点标签约束
	State        JobState
	Assignments  []string // 运行时的节点分配（长度 == GangSize）
	SubmitSeq    int64    // 提交序号，用于同优先级先到先服务
	Defer        DeferReason
}

// TotalRequest 返回整个 gang 的资源请求总量。
func (j *Job) TotalRequest() ResourceVec { return j.Request.Mul(j.GangSize) }

// Node 是一个可调度节点。
type Node struct {
	ID       string
	Capacity ResourceVec
	Used     ResourceVec
	Healthy  bool
	Labels   map[string]string
}

// Available 返回节点剩余可用资源。
func (n *Node) Available() ResourceVec { return n.Capacity.Sub(n.Used) }

// QueueConfig 描述一个队列的公平与预留配置。
type QueueConfig struct {
	Name        string
	Weight      float64            // 队列间公平权重，<=0 视为 1
	Guaranteed  ResourceVec        // 队列预留（保障）资源
	UserWeights map[string]float64 // 队列内用户级公平权重，缺省为 1
}

// Config 是调度器配置。
type Config struct {
	Queues     []QueueConfig
	UserQuotas map[string]ResourceVec // 每用户并发额度；缺省用户不受限
}
