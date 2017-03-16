package raft

type node struct {
	expiry  int64
	key     string
	version uint64
}

type nodeHeap []node

func (h *nodeHeap) Len() int {
	return len(*h)
}

func (h *nodeHeap) Less(i, j int) bool {
	return ((*h)[i].expiry < (*h)[j].expiry)
}

func (h *nodeHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *nodeHeap) Push(x interface{}) {
	*h = append(*h, x.(node))
}

func (h *nodeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
