package bloomcompactor

import (
	"github.com/grafana/dskit/ring"
)

var (
	// TODO: Should we include LEAVING instances in the replication set?
	RingOp = ring.NewOp([]ring.InstanceState{ring.JOINING, ring.ACTIVE}, nil)
)

// ShardingStrategy describes whether compactor "owns" given user or job.
type ShardingStrategy interface {
	OwnsTenant(tenant string) (bool, error)
	OwnsJob(job Job) (bool, error)
}

type ShuffleShardingStrategy struct {
	ring           *ring.Ring
	ringLifecycler *ring.BasicLifecycler
	limits         Limits
}

func NewShuffleShardingStrategy(ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, limits Limits) *ShuffleShardingStrategy {
	return &ShuffleShardingStrategy{
		ring:           ring,
		ringLifecycler: ringLifecycler,
		limits:         limits,
	}
}

// getShuffleShardingSubring returns the subring to be used for a given user.
func (s *ShuffleShardingStrategy) getShuffleShardingSubring(tenantID string) ring.ReadRing {
	shardSize := s.limits.BloomCompactorShardSize(tenantID)

	// A shard size of 0 means shuffle sharding is disabled for this specific user,
	// so we just return the full ring so that blocks will be sharded across all compactors.
	if shardSize <= 0 {
		return s.ring
	}

	return s.ring.ShuffleShard(tenantID, shardSize)
}

func (s *ShuffleShardingStrategy) OwnsTenant(tenantID string) (bool, error) {
	subRing := s.getShuffleShardingSubring(tenantID)
	return subRing.HasInstance(s.ringLifecycler.GetInstanceID()), nil
}

// OwnsJob makes sure only a single compactor should execute the job.
// TODO: Pretty similar to sharding strategy in pkg/bloomgateway/sharding.go
func (s *ShuffleShardingStrategy) OwnsJob(job Job) (bool, error) {
	// We check again if we own the tenant
	subRing := s.getShuffleShardingSubring(job.Tenant())
	ownsTenant := subRing.HasInstance(s.ringLifecycler.GetInstanceID())
	if !ownsTenant {
		return false, nil
	}

	bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()

	// If the lower end of the job FP range is owned by this compactor, we own the job.
	rs, err := subRing.Get(uint32(job.StartFP()), RingOp, bufDescs, bufHosts, bufZones)
	if err != nil {
		return false, err
	}
	if rs.Includes(s.ringLifecycler.GetInstanceID()) {
		return true, nil
	}

	// If the upper end of the job FP range is owned by this compactor, we own the job.
	rs, err = subRing.Get(uint32(job.EndFP()), RingOp, bufDescs, bufHosts, bufZones)
	if err != nil {
		return false, err
	}
	if rs.Includes(s.ringLifecycler.GetInstanceID()) {
		return true, nil
	}

	return false, nil
}
