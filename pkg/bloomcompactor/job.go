package bloomcompactor

import (
	"fmt"

	"github.com/prometheus/common/model"
)

// Job holds a compaction job, which consists of a group of blocks that should be compacted together.
// Not goroutine safe.
// TODO: A job should probably contain series or chunks
type Job struct {
	tenantID    string
	startFP     model.Fingerprint
	endFP       model.Fingerprint
	shardingKey string
}

// NewJob returns a new compaction Job.
func NewJob(tenantID string, startFP, endFP model.Fingerprint) *Job {
	return &Job{
		tenantID:    tenantID,
		startFP:     startFP,
		endFP:       endFP,
		shardingKey: fmt.Sprintf("%s-%s-%s", tenantID, startFP.String(), endFP.String()),
	}
}

func (j Job) Key() string {
	return j.shardingKey
}
