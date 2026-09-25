package scheduler

import "sort"

// memberCapacity 返回单台节点还能容纳多少个同构成员，按 CPU/内存/GPU 三个维度取最小值。
// 某一维需求为 0 时该维不参与约束。
func memberCapacity(avail, req Resources) int64 {
	caps := []int64{}
	if req.CPU > 0 {
		caps = append(caps, avail.CPU/req.CPU)
	}
	if req.Memory > 0 {
		caps = append(caps, avail.Memory/req.Memory)
	}
	if req.GPU > 0 {
		caps = append(caps, avail.GPU/req.GPU)
	}
	if len(caps) == 0 {
		// 零需求作业：成员数仅受“槽位”概念限制，这里假定单节点可承载任意多个。
		return 1 << 40
	}
	m := caps[0]
	for _, c := range caps[1:] {
		if c < m {
			m = c
		}
	}
	return m
}

func available(n *Node) Resources {
	return Resources{
		CPU:    n.Total.CPU - n.Used.CPU,
		Memory: n.Total.Memory - n.Used.Memory,
		GPU:    n.Total.GPU - n.Used.GPU,
	}
}

// shortfallDimension 在候选集合上判断为什么放不下 members 个成员，
// 返回可区分的拒绝原因；优先报告单机放不下的维度，再报告总量不足的维度。
func shortfallReason(cands []*Node, req Resources, members int) (RejectReason, string) {
	// 1) 是否存在任意一台健康候选节点能容纳单个成员
	dimOK := func(dim string) bool {
		for _, n := range cands {
			a := available(n)
			switch dim {
			case "cpu":
				if a.CPU >= req.CPU {
					return true
				}
			case "mem":
				if a.Memory >= req.Memory {
					return true
				}
			case "gpu":
				if a.GPU >= req.GPU {
					return true
				}
			}
		}
		return false
	}
	if req.CPU > 0 && !dimOK("cpu") {
		return ReasonInsufficientCPU, "no node has enough free CPU for one member"
	}
	if req.Memory > 0 && !dimOK("mem") {
		return ReasonInsufficientMem, "no node has enough free memory for one member"
	}
	if req.GPU > 0 && !dimOK("gpu") {
		return ReasonInsufficientGPU, "no node has enough free GPU for one member"
	}
	// 2) 总量维度
	var sumAvail Resources
	for _, n := range cands {
		a := available(n)
		sumAvail.CPU += a.CPU
		sumAvail.Memory += a.Memory
		sumAvail.GPU += a.GPU
	}
	need := Resources{
		CPU:    req.CPU * int64(members),
		Memory: req.Memory * int64(members),
		GPU:    req.GPU * int64(members),
	}
	if req.CPU > 0 && sumAvail.CPU < need.CPU {
		return ReasonInsufficientCPU, "cluster free CPU below gang demand"
	}
	if req.Memory > 0 && sumAvail.Memory < need.Memory {
		return ReasonInsufficientMem, "cluster free memory below gang demand"
	}
	if req.GPU > 0 && sumAvail.GPU < need.GPU {
		return ReasonInsufficientGPU, "cluster free GPU below gang demand"
	}
	// 3) 总量够但按单机切分后装不下 => 碎片化，属于点位约束不可满足
	return ReasonConstraintUnsatis, "capacity fragmented: no per-node allocation can fit all members"
}

// pickPlacement 在候选集合上按节点 ID 升序贪心产生确定性的成员 -> 节点放置。
// 仅在已确认可行时调用。
func pickPlacement(cands []*Node, req Resources, members int) []string {
	sorted := append([]*Node(nil), cands...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	out := make([]string, 0, members)
	remaining := members
	for _, n := range sorted {
		c := int(memberCapacity(available(n), req))
		if c > remaining {
			c = remaining
		}
		for k := 0; k < c; k++ {
			out = append(out, n.ID)
		}
		remaining -= c
		if remaining == 0 {
			break
		}
	}
	return out
}

// tryPlaceGang 尝试为一个 gang 的全部成员寻找原子放置方案。
// 成功返回 (节点ID列表, ReasonNone, "")；失败返回 (nil, 原因, 细节)。
// 该函数不修改任何状态，因此“部分启动”不可能发生：只有完整方案返回后才提交。
func (s *Scheduler) tryPlaceGang(gangID string) ([]string, RejectReason, string) {
	jobID, ok := s.gangs[gangID]
	if !ok {
		return nil, ReasonGangIncomplete, "gang not registered"
	}
	j := s.jobs[jobID]
	if j == nil {
		return nil, ReasonGangIncomplete, "gang has no job"
	}

	// 1) 解析显式节点白名单
	cands := make([]*Node, 0, len(s.nodes))
	if len(j.Constraint.Nodes) > 0 {
		seen := map[string]bool{}
		for _, nid := range j.Constraint.Nodes {
			if seen[nid] {
				continue
			}
			seen[nid] = true
			n, exists := s.nodes[nid]
			if !exists {
				return nil, ReasonConstraintUnsatis, "constraint references unknown node " + nid
			}
			cands = append(cands, n)
		}
	} else {
		for _, n := range s.nodes {
			cands = append(cands, n)
		}
	}

	// 2) 只允许健康节点承接新成员
	healthy := cands[:0:0]
	for _, n := range cands {
		if n.State == NodeHealthy {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) == 0 {
		return nil, ReasonNoHealthyNode, "no healthy node among placement candidates"
	}

	// 3) SameRack：按 Rack 名升序逐个尝试，选择第一个可行 rack，保证结论确定
	if j.Constraint.SameRack {
		rackSet := map[string]bool{}
		racks := []string{}
		for _, n := range healthy {
			if !rackSet[n.Rack] {
				rackSet[n.Rack] = true
				racks = append(racks, n.Rack)
			}
		}
		sort.Strings(racks)
		var firstReason RejectReason
		var firstDetail string
		for _, rack := range racks {
			in := healthy[:0:0]
			for _, n := range healthy {
				if n.Rack == rack {
					in = append(in, n)
				}
			}
			total := int64(0)
			for _, n := range in {
				total += memberCapacity(available(n), j.Request)
			}
			if total >= int64(j.Members) {
				return pickPlacement(in, j.Request, j.Members), ReasonNone, ""
			}
			if firstReason == "" {
				firstReason, firstDetail = shortfallReason(in, j.Request, j.Members)
			}
		}
		// 没有任何单个 rack 可行：若整体也不够则报资源原因，否则报拓扑不可满足
		unionReason, unionDetail := shortfallReason(healthy, j.Request, j.Members)
		if unionReason != ReasonConstraintUnsatis {
			return nil, unionReason, unionDetail
		}
		_ = firstReason
		_ = firstDetail
		return nil, ReasonConstraintUnsatis, "no single rack can host the whole gang"
	}

	// 4) 无拓扑约束：全体健康候选上求可行放置
	total := int64(0)
	for _, n := range healthy {
		total += memberCapacity(available(n), j.Request)
	}
	if total < int64(j.Members) {
		r, d := shortfallReason(healthy, j.Request, j.Members)
		return nil, r, d
	}
	return pickPlacement(healthy, j.Request, j.Members), ReasonNone, ""
}
