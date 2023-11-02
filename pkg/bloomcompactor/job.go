package bloomcompactor

import (
	"github.com/prometheus/common/model"
)

// Job holds a compaction job, which consists of a group of blocks that should be compacted together.
// Not goroutine safe.
// TODO: A job should probably contain series or chunks
type Job struct {
	tenantID       string
	tableName      string
	startFP, endFP model.Fingerprint
}

// NewJob returns a new compaction Job.
func NewJob(tenantID string, tableName string, startFP, endFP model.Fingerprint) *Job {
	return &Job{
		tenantID:  tenantID,
		tableName: tableName,
		startFP:   startFP,
		endFP:     endFP,
	}
}

func (j Job) String() string {
	return j.tableName + "_" + j.tenantID + "_" + j.startFP.String() + "_" + j.endFP.String()
}

func (j Job) Tenant() string {
	return j.tenantID
}

func (j Job) StartFP() model.Fingerprint {
	return j.startFP
}

func (j Job) EndFP() model.Fingerprint {
	return j.endFP
}
