package scheduler

// 仅供测试使用的内部视图/钩子。

func (s *Scheduler) testJob(id string) *Job   { return s.jobs[id] }
func (s *Scheduler) testNode(id string) *Node { return s.nodes[id] }
func (s *Scheduler) testLedgerOK() bool       { return s.ledgerConservation() }
