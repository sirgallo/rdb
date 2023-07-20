package service


func NewRaftService [T comparable]() *RaftService[T] {	
	return &RaftService[T]{}
}