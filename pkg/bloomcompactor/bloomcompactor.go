/*
Bloom-compactor

This is a standalone service that is responsible for compacting TSDB indexes into bloomfilters.
It creates and merges bloomfilters into an aggregated form, called bloom-blocks.
It maintains a list of references between bloom-blocks and TSDB indexes in files called meta.jsons.

Bloom-compactor regularly runs to check for changes in meta.jsons and runs compaction only upon changes in TSDBs.

bloomCompactor.Compactor

			| // Read/Write path
		bloomshipper.Store**
			|
		bloomshipper.Shipper
			|
		bloomshipper.BloomClient
			|
		ObjectClient
			|
	.....................service boundary
			|
		object storage
*/
package bloomcompactor

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/loki/pkg/compactor/retention"
	"github.com/grafana/loki/pkg/storage"
	v1 "github.com/grafana/loki/pkg/storage/bloom/v1"
	"github.com/grafana/loki/pkg/storage/bloom/v1/filter"
	chunk_client "github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/grafana/loki/pkg/storage/config"
	"github.com/grafana/loki/pkg/storage/stores/shipper/bloomshipper"
	index_storage "github.com/grafana/loki/pkg/storage/stores/shipper/indexshipper/storage"
	"github.com/grafana/loki/pkg/util"
	util_log "github.com/grafana/loki/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

const (
	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed.
	ringAutoForgetUnhealthyPeriods = 10

	// ringKey is the key under which we store the store gateways ring in the KVStore.
	ringKey = "bloom-compactor"

	// ringNameForServer is the name of the ring used by the compactor server.
	ringNameForServer = "bloom-compactor"

	// ringReplicationFactor should be 1 because we only want to pull back one node from the Ring
	ringReplicationFactor = 1

	// ringNumTokens sets our single token in the ring,
	// we only need to insert 1 token to be used for leader election purposes.
	ringNumTokens = 1
)

// TODO: maybe we don't need all of them
type storeClient struct {
	object chunk_client.ObjectClient
	index  index_storage.Client
	chunk  chunk_client.Client
}

type Compactor struct {
	services.Service

	cfg       Config
	logger    log.Logger
	schemaCfg config.SchemaConfig

	ringLifecycler *ring.BasicLifecycler
	ring           *ring.Ring

	// temporary workaround until store has implemented read/write shipper interface
	bloomShipperClient bloomshipper.Client
	bloomStore         bloomshipper.Store

	// Client used to run operations on the bucket storing bloom blocks.
	storeClients map[config.DayTime]storeClient

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	sharding ShardingStrategy
	grouper  Grouper
}

func New(
	cfg Config,
	limits Limits,
	storageCfg storage.Config,
	schemaCfg config.SchemaConfig,
	logger log.Logger,
	clientMetrics storage.ClientMetrics,
	r prometheus.Registerer) (*Compactor, error) {
	c := &Compactor{
		cfg:       cfg,
		logger:    logger,
		schemaCfg: schemaCfg,
	}

	bloomClient, err := bloomshipper.NewBloomClient(schemaCfg.Configs, storageCfg, clientMetrics)
	if err != nil {
		return nil, err
	}

	shipper, err := bloomshipper.NewShipper(
		bloomClient,
		storageCfg.BloomShipperConfig,
		logger,
	)
	if err != nil {
		return nil, err
	}

	store, err := bloomshipper.NewBloomStore(shipper)
	if err != nil {
		return nil, err
	}

	// temporary workaround until store has implemented read/write shipper interface
	c.bloomShipperClient = bloomClient
	c.bloomStore = store

	// Create object store clients
	c.storeClients = make(map[config.DayTime]storeClient)
	for _, periodicConfig := range schemaCfg.Configs {
		objectClient, err := storage.NewObjectClient(periodicConfig.ObjectType, storageCfg, clientMetrics)
		if err != nil {
			return nil, fmt.Errorf("error creating object client '%s': %w", periodicConfig.ObjectType, err)
		}

		c.storeClients[periodicConfig.From] = storeClient{
			object: objectClient,
			index:  index_storage.NewIndexStorageClient(objectClient, periodicConfig.IndexTables.PathPrefix),
			chunk:  chunk_client.NewClient(objectClient, nil, schemaCfg),
		}
	}

	ringStore, err := kv.NewClient(
		cfg.Ring.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("loki_", r), "compactor"),
		util_log.Logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "create KV store client")
	}

	lifecyclerCfg, err := cfg.Ring.ToLifecyclerConfig(ringNumTokens, util_log.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "invalid ring lifecycler config")
	}

	// Define lifecycler delegates in reverse order (last to be called defined first because they're
	// chained via "next delegate").
	delegate := ring.BasicLifecyclerDelegate(c)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, util_log.Logger)
	delegate = ring.NewTokensPersistencyDelegate(cfg.Ring.TokensFilePath, ring.JOINING, delegate, util_log.Logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*cfg.Ring.HeartbeatTimeout, delegate, util_log.Logger)

	c.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, ringNameForServer, ringKey, ringStore, delegate, util_log.Logger, r)
	if err != nil {
		return nil, errors.Wrap(err, "create ring lifecycler")
	}

	ringCfg := cfg.Ring.ToRingConfig(ringReplicationFactor)
	c.ring, err = ring.NewWithStoreClientAndStrategy(ringCfg, ringNameForServer, ringKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", r), util_log.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "create ring client")
	}

	c.subservices, err = services.NewManager(c.ringLifecycler, c.ring)
	if err != nil {
		return nil, err
	}
	c.subservicesWatcher = services.NewFailureWatcher()
	c.subservicesWatcher.WatchManager(c.subservices)

	// Create sharding strategy
	c.sharding = NewShuffleShardingStrategy(c.ring, c.ringLifecycler, limits)
	// TODO: Create grouper

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)

	return c, nil
}

func (c *Compactor) starting(ctx context.Context) (err error) {
	if err = services.StartManagerAndAwaitHealthy(ctx, c.subservices); err != nil {
		return errors.Wrap(err, "unable to start compactor subservices")
	}

	// Wait until the ring client detected this instance in the JOINING state to
	// make sure that when we'll run the initial sync we already know  the tokens
	// assigned to this instance.
	level.Info(util_log.Logger).Log("msg", "waiting until compactor is JOINING in the ring")
	if err = ring.WaitInstanceState(ctx, c.ring, c.ringLifecycler.GetInstanceID(), ring.JOINING); err != nil {
		return err
	}
	level.Info(util_log.Logger).Log("msg", "compactor is JOINING in the ring")

	// Change ring state to ACTIVE
	if err = c.ringLifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return errors.Wrapf(err, "switch instance to %s in the ring", ring.ACTIVE)
	}

	// Wait until the ring client detected this instance in the ACTIVE state to
	// make sure that when we'll run the loop it won't be detected as a ring
	// topology change.
	level.Info(util_log.Logger).Log("msg", "waiting until compactor is ACTIVE in the ring")
	if err = ring.WaitInstanceState(ctx, c.ring, c.ringLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return err
	}
	level.Info(util_log.Logger).Log("msg", "compactor is ACTIVE in the ring")

	// Return early if all went well
	if err == nil || c.subservices == nil {
		return nil
	}

	// In case this function will return error we want to unregister the instance
	// from the ring. We do it ensuring dependencies are gracefully stopped if they
	// were already started.
	if stopErr := services.StopManagerAndAwaitStopped(context.Background(), c.subservices); stopErr != nil {
		level.Error(util_log.Logger).Log("msg", "failed to gracefully stop compactor dependencies", "err", stopErr)
	}

	return err
}

func (c *Compactor) running(ctx context.Context) error {
	// Run an initial compaction before starting the interval.
	if err := c.runCompaction(ctx); err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to run compaction", "err", err)
	}

	ticker := time.NewTicker(util.DurationWithJitter(c.cfg.CompactionInterval, 0.05))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.runCompaction(ctx); err != nil {
				level.Error(util_log.Logger).Log("msg", "failed to run compaction", "err", err)
			}
		case <-ctx.Done():
			return nil
		case err := <-c.subservicesWatcher.Chan():
			return errors.Wrap(err, "compactor subservice failed")
		}
	}
}

func (c *Compactor) stopping(_ error) error {
	return nil
}

func (c *Compactor) runCompaction(ctx context.Context) error {
	var (
		tables []string
		// it possible for two periods to use the same storage bucket and path prefix (different indexType or schema version)
		// so more than one index storage client may end up listing the same set of buckets
		// avoid including the same table twice in the compact tables list.
		seen = make(map[string]struct{})
	)
	for _, sc := range c.storeClients {
		// refresh index list cache since previous compaction would have changed the index files in the object store
		sc.index.RefreshIndexTableNamesCache(ctx)
		tbls, err := sc.index.ListTables(ctx)
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		for _, table := range tbls {
			if _, ok := seen[table]; ok {
				continue
			}

			tables = append(tables, table)
			seen[table] = struct{}{}
		}
	}

	// process most recent tables first
	sortTablesByRange(tables)

	// apply passed in compaction limits
	if c.cfg.SkipLatestNTables <= len(tables) {
		tables = tables[c.cfg.SkipLatestNTables:]
	}
	if c.cfg.TablesToCompact > 0 && c.cfg.TablesToCompact < len(tables) {
		tables = tables[:c.cfg.TablesToCompact]
	}

	errs := multierror.New()
	if err := concurrency.ForEachJob(ctx, len(tables), c.cfg.MaxCompactionParallelism, func(ctx context.Context, i int) error {
		tableName := tables[i]
		level.Info(util_log.Logger).Log("msg", "compacting table", "table-name", tableName)
		err := c.compactTable(ctx, tableName)
		if err != nil {
			errs.Add(err)
			return nil
		}
		level.Info(util_log.Logger).Log("msg", "finished compacting table", "table-name", tableName)
		return nil
	}); err != nil {
		errs.Add(err)
	}

	return errs.Err()
}

func (c *Compactor) compactTable(ctx context.Context, tableName string) error {
	schemaCfg, ok := schemaPeriodForTable(c.schemaCfg, tableName)
	if !ok {
		level.Error(util_log.Logger).Log("msg", "skipping compaction since we can't find schema for table", "table", tableName)
		return nil
	}

	sc, ok := c.storeClients[schemaCfg.From]
	if !ok {
		return fmt.Errorf("index store client not found for period starting at %s", schemaCfg.From.String())
	}

	_, tenants, err := sc.index.ListFiles(ctx, tableName, false)
	if err != nil {
		return fmt.Errorf("failed to list files for table %s: %w", tableName, err)
	}

	level.Info(c.logger).Log("msg", "discovered tenants from bucket", "users", len(tenants))
	return c.compactUsers(ctx, sc, tableName, tenants)
}

// See: https://github.com/grafana/mimir/blob/34852137c332d4050e53128481f4f6417daee91e/pkg/compactor/compactor.go#L566-L689
func (c *Compactor) compactUsers(ctx context.Context, sc storeClient, tableName string, tenants []string) error {
	// When starting multiple compactor replicas nearly at the same time, running in a cluster with
	// a large number of tenants, we may end up in a situation where the 1st user is compacted by
	// multiple replicas at the same time. Shuffling users helps reduce the likelihood this will happen.
	rand.Shuffle(len(tenants), func(i, j int) {
		tenants[i], tenants[j] = tenants[j], tenants[i]
	})

	// Keep track of tenants owned by this shard, so that we can delete the local files for all other users.
	errs := multierror.New()
	ownedTenants := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		// Ensure the context has not been canceled (ie. compactor shutdown has been triggered).
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("interrupting compaction of tenants: %w", err)
		}

		// Ensure the tenant ID belongs to our shard.
		owned, err := c.sharding.OwnsTenant(tenant)
		if err != nil {
			// c.compactionRunSkippedTenants.Inc()
			level.Warn(c.logger).Log("msg", "unable to check if tenant is owned by this shard", "tenantID", tenant, "err", err)
			continue
		}
		if !owned {
			// c.compactionRunSkippedTenants.Inc()
			level.Debug(c.logger).Log("msg", "skipping tenant because it is not owned by this shard", "tenantID", tenant)
			continue
		}

		ownedTenants[tenant] = struct{}{}

		if err := c.compactTenantWithRetries(ctx, sc, tableName, tenant); err != nil {
			switch {
			case errors.Is(err, context.Canceled):
				// We don't want to count shutdowns as failed compactions because we will pick up with the rest of the compaction after the restart.
				level.Info(c.logger).Log("msg", "compaction for tenant was interrupted by a shutdown", "tenant", tenant)
				return nil
			default:
				// c.compactionRunFailedTenants.Inc()
				// compactionErrorCount++
				level.Error(c.logger).Log("msg", "failed to compact tenant", "tenant", tenant, "err", err)
				errs.Add(err)
			}
			continue
		}

		// c.compactionRunSucceededTenants.Inc()
		level.Info(c.logger).Log("msg", "successfully compacted tenant", "tenant", tenant)
	}

	return errs.Err()

	// TODO: Delete local files for unowned tenants, if there are any.
}

func (c *Compactor) compactTenant(ctx context.Context, sc storeClient, tableName string, tenant string) error {
	level.Info(c.logger).Log("msg", "starting compaction of tenant", "tenant", tenant)

	// Ensure the context has not been canceled (ie. compactor shutdown has been triggered).
	if err := ctx.Err(); err != nil {
		return err
	}

	// TODO: Iterate through series to process and create jobs
	jobs := []Job{
		{
			tenantID:  tenant,
			tableName: tableName,
			startFP:   0,
			endFP:     0,
		},
	}

	errs := multierror.New()
	if err := concurrency.ForEachJob(ctx, len(jobs), c.cfg.MaxCompactionParallelism, func(ctx context.Context, i int) error {
		job := jobs[i]

		// Make sure again that the compactor still owns the job.
		owns, err := c.sharding.OwnsJob(job)
		if err != nil {
			errs.Add(errors.Wrap(err, "ownJob"))
			return nil
		}
		if !owns {
			level.Info(c.logger).Log("msg", "skipped compaction because job is not owned by the compactor instance anymore", "job", job.String())
			return nil
		}

		if err := c.runBloomCompact(ctx, sc, job); err != nil {
			errs.Add(errors.Wrap(err, "runBloomCompact"))
		}

		return nil
	}); err != nil {
		errs.Add(err)
	}

	return errs.Err()
}

func runWithRetries(
	ctx context.Context,
	minBackoff, maxBackoff time.Duration,
	maxRetries int,
	f func(ctx context.Context) error,
) error {
	var lastErr error

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: minBackoff,
		MaxBackoff: maxBackoff,
		MaxRetries: maxRetries,
	})

	for retries.Ongoing() {
		lastErr = f(ctx)
		if lastErr == nil {
			return nil
		}

		retries.Wait()
	}

	return lastErr
}

func (c *Compactor) compactTenantWithRetries(ctx context.Context, sc storeClient, tableName string, tenant string) error {
	return runWithRetries(
		ctx,
		c.cfg.retryMinBackoff,
		c.cfg.retryMaxBackoff,
		c.cfg.CompactionRetries,
		func(ctx context.Context) error {
			return c.compactTenant(ctx, sc, tableName, tenant)
		},
	)
}

func (c *Compactor) filterOwnJobs(jobs []Job) ([]Job, error) {
	for ix := 0; ix < len(jobs); {
		// Skip any job which doesn't belong to this compactor instance.
		if ok, err := c.sharding.OwnsJob(jobs[ix]); err != nil {
			return nil, errors.Wrap(err, "ownJob")
		} else if !ok {
			jobs = append(jobs[:ix], jobs[ix+1:]...)
		} else {
			ix++
		}
	}
	return jobs, nil
}

func (c *Compactor) OnRingInstanceRegister(_ *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, _ string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	// When we initialize the compactor instance in the ring we want to start from
	// a clean situation, so whatever is the state we set it JOINING, while we keep existing
	// tokens (if any) or the ones loaded from file.
	var tokens []uint32
	if instanceExists {
		tokens = instanceDesc.GetTokens()
	}

	takenTokens := ringDesc.GetTokens()
	gen := ring.NewRandomTokenGenerator()
	newTokens := gen.GenerateTokens(ringNumTokens-len(tokens), takenTokens)

	// Tokens sorting will be enforced by the parent caller.
	tokens = append(tokens, newTokens...)

	return ring.JOINING, tokens
}

func (c *Compactor) OnRingInstanceTokens(_ *ring.BasicLifecycler, _ ring.Tokens) {}
func (c *Compactor) OnRingInstanceStopping(_ *ring.BasicLifecycler)              {}
func (c *Compactor) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, _ *ring.Desc, _ *ring.InstanceDesc) {
}

// TODO Get fpRange owned by the compactor instance
func NoopGetFingerprintRange() (uint64, uint64) { return 0, 0 }

// TODO List Users from TSDB and add logic to owned user via ring
func NoopGetUserID() string { return "" }

// TODO get series from objectClient (TSDB) instead of params
func NoopGetSeries() *v1.Series { return nil }

// TODO Then get chunk data from series
func NoopGetChunks() []byte { return nil }

// part1: Create a compact method that assumes no block/meta files exists (eg first compaction)
// part2: Write logic to check first for existing block/meta files and does above.
func (c *Compactor) compactNewChunks(ctx context.Context, dst string) (err error) {
	// part1
	series := NoopGetSeries()
	data := NoopGetChunks()

	bloom := v1.Bloom{ScalableBloomFilter: *filter.NewDefaultScalableBloomFilter(0.01)}
	// create bloom filters from that.
	bloom.Add([]byte(fmt.Sprint(data)))

	// block and seriesList
	seriesList := []v1.SeriesWithBloom{
		{
			Series: series,
			Bloom:  &bloom,
		},
	}

	writer := v1.NewDirectoryBlockWriter(dst)

	builder, err := v1.NewBlockBuilder(
		v1.BlockOptions{
			SeriesPageSize: 100,
			BloomPageSize:  10 << 10,
		}, writer)
	if err != nil {
		return err
	}
	// BuildFrom closes itself
	err = builder.BuildFrom(v1.NewSliceIter[v1.SeriesWithBloom](seriesList))
	if err != nil {
		return err
	}

	// TODO Ask Owen, shall we expose a method to expose these paths on BlockWriter?
	indexPath := filepath.Join(dst, "series")
	bloomPath := filepath.Join(dst, "bloom")

	blockRef := bloomshipper.BlockRef{
		IndexPath: indexPath,
		BlockPath: bloomPath,
	}

	blocks := []bloomshipper.Block{
		{
			BlockRef: blockRef,

			// TODO point to the data to be read
			Data: nil,
		},
	}

	meta := bloomshipper.Meta{
		// After successful compaction there should be no tombstones
		Tombstones: make([]bloomshipper.BlockRef, 0),
		Blocks:     []bloomshipper.BlockRef{blockRef},
	}

	err = c.bloomShipperClient.PutMeta(ctx, meta)
	if err != nil {
		return err
	}
	_, err = c.bloomShipperClient.PutBlocks(ctx, blocks)
	if err != nil {
		return err
	}
	// TODO may need to change return value of this func
	return nil
}

func (c *Compactor) runBloomCompact(ctx context.Context, sc storeClient, job Job) error {
	// TODO set MaxLookBackPeriod to Max ingester accepts
	maxLookBackPeriod := c.cfg.MaxLookBackPeriod

	end := time.Now().UTC().UnixMilli()
	start := end - maxLookBackPeriod.Milliseconds()

	metaSearchParams := bloomshipper.MetaSearchParams{
		TenantID:       job.Tenant(),
		MinFingerprint: uint64(job.StartFP()),
		MaxFingerprint: uint64(job.EndFP()),
		StartTimestamp: start,
		EndTimestamp:   end,
	}

	metas, err := c.bloomShipperClient.GetMetas(ctx, metaSearchParams)
	if err != nil {
		return err
	}

	if len(metas) == 0 {
		// run compaction from scratch
		tempDst := os.TempDir()
		err = c.compactNewChunks(ctx, tempDst)
		if err != nil {
			return err
		}
	} else {
		// part 2
		// When already compacted metas exists
		// Deduplicate index paths
		uniqueIndexPaths := make(map[string]struct{})

		for _, meta := range metas {
			for _, blockRef := range meta.Blocks {
				uniqueIndexPaths[blockRef.IndexPath] = struct{}{}
			}
		}

		// TODO complete part 2 - discuss with Owen - add part to compare chunks and blocks.
		// 1. for each period at hand, get TSDB table indexes for given fp range
		// 2. Check blocks for given uniqueIndexPaths and TSDBindexes
		//	if bloomBlock refs are a superset (covers TSDBIndexes plus more outside of range)
		//	create a new meta.json file, tombstone unused index/block paths.

		// else if: there are TSDBindexes that are not covered in bloomBlocks (a subset)
		// then call compactNewChunks on them and create a new meta.json

		// else: all good, no compaction
	}
	return nil
}

// TODO: comes from pkg/compactor/compactor.go
func sortTablesByRange(tables []string) {
	tableRanges := make(map[string]model.Interval)
	for _, table := range tables {
		tableRanges[table] = retention.ExtractIntervalFromTableName(table)
	}

	sort.Slice(tables, func(i, j int) bool {
		// less than if start time is after produces a most recent first sort order
		return tableRanges[tables[i]].Start.After(tableRanges[tables[j]].Start)
	})

}

// TODO: comes from pkg/compactor/compactor.go
func schemaPeriodForTable(cfg config.SchemaConfig, tableName string) (config.PeriodConfig, bool) {
	tableInterval := retention.ExtractIntervalFromTableName(tableName)
	schemaCfg, err := cfg.SchemaForTime(tableInterval.Start)
	if err != nil || schemaCfg.IndexTables.TableFor(tableInterval.Start) != tableName {
		return config.PeriodConfig{}, false
	}

	return schemaCfg, true
}
