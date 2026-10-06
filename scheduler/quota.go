package scheduler

// quotaLimit 返回用户额度，ok=false 表示不受限。
func (s *Scheduler) quotaLimit(user string) (Resource, bool) {
	l, ok := s.quotas[user]
	return l, ok
}

// quotaAvailable 判断 user 再占用 need 是否仍不超额度；未配置额度的用户不受限。
func (s *Scheduler) quotaAvailable(user string, need Resource) bool {
	limit, ok := s.quotaLimit(user)
	if !ok {
		return true
	}
	return s.userUsage[user].Add(need).Leq(limit)
}

// chargeQuota 计入用户占用。
func (s *Scheduler) chargeQuota(user string, r Resource) {
	s.userUsage[user] = s.userUsage[user].Add(r)
}

// refundQuota 归还用户占用；只允许把占用减到 0 为止，防止重复归还导致负值超发。
func (s *Scheduler) refundQuota(user string, r Resource) {
	u := s.userUsage[user].Sub(r)
	if u.CPU < 0 {
		u.CPU = 0
	}
	if u.MemGB < 0 {
		u.MemGB = 0
	}
	if u.GPU < 0 {
		u.GPU = 0
	}
	s.userUsage[user] = u
}
