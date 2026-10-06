package scheduler

// QuotaManager 跟踪每个用户的并发额度与实时占用。
// 未配置额度的用户不受限。
type QuotaManager struct {
	limits map[string]ResourceVec
	used   map[string]ResourceVec
}

// NewQuotaManager 按配置创建额度管理器。
func NewQuotaManager(limits map[string]ResourceVec) *QuotaManager {
	cp := make(map[string]ResourceVec, len(limits))
	for u, l := range limits {
		cp[u] = l
	}
	return &QuotaManager{limits: cp, used: make(map[string]ResourceVec)}
}

// HasLimit 报告用户是否配置了额度。
func (q *QuotaManager) HasLimit(user string) bool {
	_, ok := q.limits[user]
	return ok
}

// CanAdmit 报告用户再占用 need 后是否仍不超过额度；无额度用户恒为 true。
func (q *QuotaManager) CanAdmit(user string, need ResourceVec) bool {
	limit, ok := q.limits[user]
	if !ok {
		return true
	}
	return q.used[user].Add(need).Fits(limit)
}

// Allocate 记录用户新增占用（调用方保证已通过 CanAdmit）。
func (q *QuotaManager) Allocate(user string, r ResourceVec) {
	if r.IsZero() {
		return
	}
	q.used[user] = q.used[user].Add(r)
}

// Release 归还用户占用；由调度器在作业状态迁移时恰好调用一次。
// 防御性处理：归还量不超过当前占用，避免重复通知导致负数。
func (q *QuotaManager) Release(user string, r ResourceVec) {
	if r.IsZero() {
		return
	}
	cur := q.used[user]
	next := cur.Sub(r)
	// 逐维度钳制到 0，保证幂等通知不会把占用减成负数。
	if next.CPU < 0 {
		next.CPU = 0
	}
	if next.Mem < 0 {
		next.Mem = 0
	}
	if next.GPU < 0 {
		next.GPU = 0
	}
	if next.IsZero() {
		delete(q.used, user)
		return
	}
	q.used[user] = next
}

// Used 返回用户当前占用。
func (q *QuotaManager) Used(user string) ResourceVec { return q.used[user] }

// Limit 返回用户额度与是否配置了额度。
func (q *QuotaManager) Limit(user string) (ResourceVec, bool) {
	l, ok := q.limits[user]
	return l, ok
}

// Snapshot 返回额度的可序列化副本。
func (q *QuotaManager) Snapshot() QuotaSnapshot {
	used := make(map[string]ResourceVec, len(q.used))
	for u, r := range q.used {
		used[u] = r
	}
	return QuotaSnapshot{Used: used}
}

// Restore 从快照恢复额度占用。
func (q *QuotaManager) Restore(s QuotaSnapshot) {
	q.used = make(map[string]ResourceVec, len(s.Used))
	for u, r := range s.Used {
		q.used[u] = r
	}
}

// QuotaSnapshot 是额度的快照。
type QuotaSnapshot struct {
	Used map[string]ResourceVec
}
