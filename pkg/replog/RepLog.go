package replog


func NewReplicatedLog [T comparable]() *ReplicatedLog[T] {
	return &ReplicatedLog[T]{
		replog: []*LogEntry[T]{},
	}
}

func (log *ReplicatedLog[T]) AppendEntries(incomingEntries []*LogEntry[T]) {
	log.replog = append(log.replog, incomingEntries...)
}