package bloomcompactor

// shardingStrategy describes whether compactor "owns" given user or job.
type shardingStrategy interface {
	ownTenant(tenant string) (bool, error)
	ownJob(job Job) (bool, error)
}
