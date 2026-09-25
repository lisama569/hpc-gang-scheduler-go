package scheduler

// Resources 描述一个作业成员或一台节点的资源量。
type Resources struct {
	CPU    int64
	Memory int64
	GPU    int64
}

// NodeState 节点健康状态。
type NodeState string

const (
	NodeHealthy NodeState = "Healthy"
	NodeDrained NodeState = "Drained"
	NodeFaulted NodeState = "Faulted"
)

// JobState 作业生命周期状态。
type JobState string

const (
	StatePending   JobState = "Pending"
	StateRunning   JobState = "Running"
	StateCompleted JobState = "Completed"
	StateCancelled JobState = "Cancelled"
	StateRejected  JobState = "Rejected"
	StateFailed    JobState = "Failed" // 因节点故障被强行终止
)

// RejectReason 是调度被拒绝/推迟的可区分原因。
type RejectReason string

const (
	ReasonNone              RejectReason = ""
	ReasonInvalid           RejectReason = "InvalidJob"
	ReasonGangIncomplete    RejectReason = "GangIncomplete"
	ReasonInsufficientCPU   RejectReason = "InsufficientCPU"
	ReasonInsufficientMem   RejectReason = "InsufficientMemory"
	ReasonInsufficientGPU   RejectReason = "InsufficientGPU"
	ReasonNoHealthyNode     RejectReason = "NoHealthyNode"
	ReasonConstraintUnsatis RejectReason = "ConstraintUnsatisfiable"
	ReasonQueueClosed       RejectReason = "QueueClosed"
)

// PlacementConstraint 声明 gang 的点位/拓扑约束。
// SameRack=true 时所有成员必须落在同一 Rack；
// 指定 Nodes 时成员只能使用这些节点。
type PlacementConstraint struct {
	SameRack bool
	Nodes    []string
}

// Node 是集群中的一台计算节点。
type Node struct {
	ID    string
	Rack  string
	State NodeState
	Total Resources
	Used  Resources
}

// Job 是一个 gang 作业：同一 GangID 的 Members 个成员必须原子启动。
type Job struct {
	ID           string
	GangID       string
	User         string
	Queue        string
	Members      int
	Priority     int
	Request      Resources
	Constraint   PlacementConstraint
	State        JobState
	Reason       RejectReason
	ReasonDetail string
	NodeIDs      []string
	SubmittedAt  int64
	StartedAt    int64
	FinishedAt   int64
	// Retries 记录被调度器推迟（资源/约束不足）的次数。
	Retries int
	// PreemptCount 记录被抢占次数；被抢占作业回到队首并获得预留提升，防止饿死。
	PreemptCount int
}

// ScheduleResult 记录一次调度扫描的结论。
type ScheduleResult struct {
	StartedGangs []string
	Rejected     map[string]RejectReason
}

// Logger 用于打印关键输入与判定依据。
type Logger interface {
	Printf(format string, args ...any)
}
