package snapshot

import "github.com/sirgallo/raft/pkg/system"

/*
	Get Alive Systems And Min Success Resps:
		helper method for both determining the current alive systems in the cluster and also the minimum successful responses
		needed for committing logs to the state machine

		--> minimum is found by floor(total alive systems / 2) + 1
*/

func (snpService *SnapshotService) GetAliveSystemsAndMinSuccessResps() ([]*system.System, int) {
	var aliveSystems []*system.System

	snpService.Systems.Range(func(key, value interface{}) bool {
		sys := value.(*system.System)
		if sys.Status == system.Ready { aliveSystems = append(aliveSystems, sys) }

		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, (totAliveSystems / 2) + 1
}