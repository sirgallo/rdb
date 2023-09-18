package stats


type Stats struct {
	AvailableDiskSpaceInBytes int64
	TotalDiskSpaceInBytes int64
	UsedDiskSpaceInBytes int64
	Timestamp string
}

const NAME = "Stats"