package scheduler

// AuditRecord 记录一次抢占/预留的审计信息，保证可解释、可复现。
type AuditRecord struct {
	Seq       int64
	Time      int64
	Kind      string // "preempt" | "reserve"
	GangID    string
	JobID     string
	Queue     string
	VictimJob string
	Reason    string
}
