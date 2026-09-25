package scheduler

import "sort"

// consumeEvent 对事件 ID 做幂等去重；已处理过的事件返回 false。
func (s *Scheduler) consumeEvent(eventID string) bool {
	if eventID == "" {
		return true
	}
	if s.events[eventID] {
		return false
	}
	s.events[eventID] = true
	return true
}

// killJobsOnNode 因节点故障终止所有在该节点上有成员的运行中 gang：
// gang 不允许“部分存活”，因此只要有成员落在故障节点，整个作业 Failed 并释放全部资源。
func (s *Scheduler) killJobsOnNode(nodeID string) []string {
	var affected []string
	for _, j := range s.jobs {
		if j.State != StateRunning {
			continue
		}
		on := false
		for _, nid := range j.NodeIDs {
			if nid == nodeID {
				on = true
				break
			}
		}
		if !on {
			continue
		}
		s.releaseResources(j)
		j.State = StateFailed
		j.ReasonDetail = "member node " + nodeID + " faulted"
		j.FinishedAt = s.now()
		j.NodeIDs = nil
		affected = append(affected, j.ID)
		s.log.Printf("job failed-by-nodefault: id=%s gang=%s node=%s", j.ID, j.GangID, nodeID)
	}
	sort.Strings(affected)
	return affected
}

// changeNodeState 在幂等校验通过后执行节点状态迁移。
func (s *Scheduler) changeNodeState(nodeID, eventID, action string, target NodeState) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.consumeEvent(eventID) {
		s.log.Printf("event duplicate ignored: event=%s action=%s node=%s", eventID, action, nodeID)
		return nil, ErrDuplicateEvent
	}
	n, ok := s.nodes[nodeID]
	if !ok {
		return nil, ErrNotFound
	}
	// 重复/迟到的状态通知：不重复处理、不重复回收资源
	if n.State == target {
		s.log.Printf("node %s no-op: already state=%s event=%s", nodeID, target, eventID)
		return nil, nil
	}
	old := n.State
	n.State = target
	s.log.Printf("node state: id=%s %s->%s event=%s", nodeID, old, target, eventID)
	if target == NodeFaulted {
		return s.killJobsOnNode(nodeID), nil
	}
	return nil, nil
}

// NodeFault 上报节点故障（幂等）：节点标记为 Faulted，其上运行作业整体失败并回收资源。
func (s *Scheduler) NodeFault(nodeID, eventID string) error {
	_, err := s.changeNodeState(nodeID, eventID, "fault", NodeFaulted)
	return err
}

// NodeDrain 节点主动下线（cordon）：不再接受新成员；已运行作业继续运行直到完成/取消。
func (s *Scheduler) NodeDrain(nodeID, eventID string) error {
	_, err := s.changeNodeState(nodeID, eventID, "drain", NodeDrained)
	return err
}

// NodeRecover 节点恢复为健康，可重新参与调度。
func (s *Scheduler) NodeRecover(nodeID, eventID string) error {
	_, err := s.changeNodeState(nodeID, eventID, "recover", NodeHealthy)
	return err
}

// NodeFaultAffected 与 NodeFault 相同，但额外返回被终止的作业 ID 列表，便于验证。
func (s *Scheduler) NodeFaultAffected(nodeID, eventID string) ([]string, error) {
	return s.changeNodeState(nodeID, eventID, "fault", NodeFaulted)
}
