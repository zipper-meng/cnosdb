package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnosdb/cnosdb"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"

	"go.uber.org/zap"
)

// The keys for statistics generated by the "write" module.
const (
	statWriteReq            = "req"
	statPointWriteReq       = "pointReq"
	statPointWriteReqLocal  = "pointReqLocal"
	statPointWriteReqRemote = "pointReqRemote"
	statWriteOK             = "writeOk"
	statWritePartial        = "writePartial"
	statWriteDrop           = "writeDrop"
	statWriteTimeout        = "writeTimeout"
	statWriteErr            = "writeError"
	statWritePointReqHH     = "pointReqHH"
	statSubWriteOK          = "subWriteOk"
	statSubWriteDrop        = "subWriteDrop"
)

var (
	// ErrTimeout is returned when a write times out.
	ErrTimeout = errors.New("timeout")

	// ErrPartialWrite is returned when a write partially succeeds but does
	// not meet the requested consistency level.
	ErrPartialWrite = errors.New("partial write")

	// ErrWriteFailed is returned when no writes succeeded.
	ErrWriteFailed = errors.New("write failed")

	// ErrInvalidConsistencyLevel is returned when parsing the string version
	// of a consistency level.
	ErrInvalidConsistencyLevel = errors.New("invalid consistency level")
)

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	mu           sync.RWMutex
	closing      chan struct{}
	WriteTimeout time.Duration
	Logger       *zap.Logger

	Node *cnosdb.Node

	MetaClient interface {
		Database(name string) (di *meta.DatabaseInfo)
		RetentionPolicy(database, rp string) (*meta.RetentionPolicyInfo, error)
		CreateShardGroup(database, rp string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	}

	TSDBStore interface {
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		WriteToShard(shardID uint64, points []models.Point) error
	}

	ShardWriter interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}

	HintedHandoff interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}

	Subscriber interface {
		Points() chan<- *WritePointsRequest
	}
	subPoints []chan<- *WritePointsRequest

	stats *WriteStatistics
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter() *PointsWriter {
	return &PointsWriter{
		closing:      make(chan struct{}),
		WriteTimeout: DefaultWriteTimeout,
		Logger:       zap.NewNop(),
		stats:        &WriteStatistics{},
	}
}

// ShardMapping contains a mapping of shards to points.
type ShardMapping struct {
	n       int
	Points  map[uint64][]models.Point  // The points associated with a shard ID
	Shards  map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
	Dropped []models.Point             // Points that were dropped
}

// NewShardMapping creates an empty ShardMapping.
func NewShardMapping(n int) *ShardMapping {
	return &ShardMapping{
		n:      n,
		Points: map[uint64][]models.Point{},
		Shards: map[uint64]*meta.ShardInfo{},
	}
}

// MapPoint adds the point to the ShardMapping, associated with the given shardInfo.
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, p models.Point) {
	if cap(s.Points[shardInfo.ID]) < s.n {
		s.Points[shardInfo.ID] = make([]models.Point, 0, s.n)
	}
	s.Points[shardInfo.ID] = append(s.Points[shardInfo.ID], p)
	s.Shards[shardInfo.ID] = shardInfo
}

// Open opens the communication channel with the point writer.
func (w *PointsWriter) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closing = make(chan struct{})
	return nil
}

// Close closes the communication channel with the point writer.
func (w *PointsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing != nil {
		close(w.closing)
	}
	if w.subPoints != nil {
		// 'nil' channels always block so this makes the
		// select statement in WritePoints hit its default case
		// dropping any in-flight writes.
		w.subPoints = nil
	}
	return nil
}

func (w *PointsWriter) AddWriteSubscriber(c chan<- *WritePointsRequest) {
	w.subPoints = append(w.subPoints, c)
}

// WithLogger sets the Logger on w.
func (w *PointsWriter) WithLogger(log *zap.Logger) {
	w.Logger = log.With(zap.String("service", "write"))
}

// WriteStatistics keeps statistics related to the PointsWriter.
type WriteStatistics struct {
	WriteReq            int64
	PointWriteReq       int64
	PointWriteReqLocal  int64
	PointWriteReqRemote int64
	WriteOK             int64
	WritePartial        int64
	WriteDropped        int64
	WriteTimeout        int64
	WriteErr            int64
	WritePointReqHH     int64
	SubWriteOK          int64
	SubWriteDrop        int64
}

// Statistics returns statistics for periodic monitoring.
func (w *PointsWriter) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "write",
		Tags: tags,
		Values: map[string]interface{}{
			statWriteReq:            atomic.LoadInt64(&w.stats.WriteReq),
			statPointWriteReq:       atomic.LoadInt64(&w.stats.PointWriteReq),
			statPointWriteReqLocal:  atomic.LoadInt64(&w.stats.PointWriteReqLocal),
			statPointWriteReqRemote: atomic.LoadInt64(&w.stats.PointWriteReqRemote),
			statWriteOK:             atomic.LoadInt64(&w.stats.WriteOK),
			statWritePartial:        atomic.LoadInt64(&w.stats.WritePartial),
			statWriteDrop:           atomic.LoadInt64(&w.stats.WriteDropped),
			statWriteTimeout:        atomic.LoadInt64(&w.stats.WriteTimeout),
			statWriteErr:            atomic.LoadInt64(&w.stats.WriteErr),
			statWritePointReqHH:     atomic.LoadInt64(&w.stats.WritePointReqHH),
			statSubWriteOK:          atomic.LoadInt64(&w.stats.SubWriteOK),
			statSubWriteDrop:        atomic.LoadInt64(&w.stats.SubWriteDrop),
		},
	}}
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (w *PointsWriter) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {
	rp, err := w.MetaClient.RetentionPolicy(wp.Database, wp.RetentionPolicy)
	if err != nil {
		return nil, err
	} else if rp == nil {
		return nil, cnosdb.ErrRetentionPolicyNotFound(wp.RetentionPolicy)
	}

	// Holds all the shard groups and shards that are required for writes.
	list := sgList{items: make(meta.ShardGroupInfos, 0, 8)}
	min := time.Unix(0, models.MinNanoTime)
	if rp.Duration > 0 {
		min = time.Now().Add(-rp.Duration)
	}

	for _, p := range wp.Points {
		// Either the point is outside the scope of the retention policy, or we already have
		// a suitable shard group for the point.
		if p.Time().Before(min) || list.Covers(p.Time()) {
			continue
		}

		// No shard groups overlap with the point's time, so we will create
		// a new shard group for this point.
		rg, err := w.MetaClient.CreateShardGroup(wp.Database, wp.RetentionPolicy, p.Time())
		if err != nil {
			return nil, err
		}

		if rg == nil {
			return nil, errors.New("nil shard group")
		}
		list.Add(*rg)
	}

	mapping := NewShardMapping(len(wp.Points))
	for _, p := range wp.Points {
		rg := list.ShardGroupAt(p.Time())
		if rg == nil {
			// We didn't create a shard group because the point was outside the
			// scope of the retention policy.
			mapping.Dropped = append(mapping.Dropped, p)
			atomic.AddInt64(&w.stats.WriteDropped, 1)
			continue
		}

		sh := rg.ShardFor(p)
		mapping.MapPoint(&sh, p)
	}
	return mapping, nil
}

// sgList is a wrapper around a meta.ShardGroupInfos where we can also check
// if a given time is covered by any of the shard groups in the list.
type sgList struct {
	items meta.ShardGroupInfos

	// needsSort indicates if items has been modified without a sort operation.
	needsSort bool

	// earliest is the last begin time of any item in items.
	earliest time.Time

	// latest is the greatest end time of any item in items.
	latest time.Time
}

func (l sgList) Covers(t time.Time) bool {
	if len(l.items) == 0 {
		return false
	}
	return l.ShardGroupAt(t) != nil
}

// ShardGroupAt attempts to find a shard group that could contain a point
// at the given time.
//
// ShardGroups are sorted first according to end time, and then according
// to start time. Therefore, if there are multiple shard groups that match
// this point's time they will be preferred in this order:
//
//  - a shard group with the earliest end time;
//  - (assuming identical end times) the shard group with the earliest start time.
func (l sgList) ShardGroupAt(t time.Time) *meta.ShardGroupInfo {
	if l.items.Len() == 0 {
		return nil
	}

	// find the earliest shard group that could contain this point using binary search.
	if l.needsSort {
		sort.Sort(l.items)
		l.needsSort = false
	}
	idx := sort.Search(l.items.Len(), func(i int) bool { return l.items[i].EndTime.After(t) })

	// Check if sort.Search actually found the proper shard. It feels like we should also
	// be checking l.items[idx].EndTime, but sort.Search was looking at that field for us.
	if idx == l.items.Len() || t.Before(l.items[idx].StartTime) {
		// This could mean we are looking for a time not in the list, or we have
		// overlapping shards. Overlapping shards do not work with binary searches
		// on 1d arrays. You have to use an interval tree, but that's a lot of
		// work for what is hopefully a rare event. Instead, we'll check if t
		// should be in l, and perform a linear search if it is. This way we'll
		// do the correct thing, it may just take a little longer. If we don't
		// do this, then we may non-silently drop writes we should have accepted.

		if t.Before(l.earliest) || t.After(l.latest) {
			// t is not in range, we can avoid going through the linear search.
			return nil
		}

		// Oh no, we've probably got overlapping shards. Perform a linear search.
		for idx = 0; idx < l.items.Len(); idx++ {
			if l.items[idx].Contains(t) {
				// Found it!
				break
			}
		}
		if idx == l.items.Len() {
			// We did not find a shard which contained t. This is very strange.
			return nil
		}
	}

	return &l.items[idx]
}

// Add appends a shard group to the list, updating the earliest/latest times of the list if needed.
func (l *sgList) Add(sgi meta.ShardGroupInfo) {
	l.items = append(l.items, sgi)
	l.needsSort = true

	// Update our earliest and latest times for l.items
	if l.earliest.IsZero() || l.earliest.After(sgi.StartTime) {
		l.earliest = sgi.StartTime
	}
	if l.latest.IsZero() || l.latest.Before(sgi.EndTime) {
		l.latest = sgi.EndTime
	}
}

// WritePointsInto is a copy of WritePoints that uses a tsdb structure instead of
// a cluster structure for information. This is to avoid a circular dependency.
func (w *PointsWriter) WritePointsInto(p *IntoWriteRequest) error {
	return w.WritePointsPrivileged(p.Database, p.RetentionPolicy, models.ConsistencyLevelOne, p.Points)
}

// WritePoints writes data to the underlying storage. consistencyLevel and user are only used for clustered scenarios.
func (w *PointsWriter) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error {
	return w.WritePointsPrivileged(database, retentionPolicy, consistencyLevel, points)
}

// WritePointsPrivileged writes the data to the underlying storage,
// consistencyLevel is only used for clustered scenarios
func (w *PointsWriter) WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	atomic.AddInt64(&w.stats.WriteReq, 1)
	atomic.AddInt64(&w.stats.PointWriteReq, int64(len(points)))

	if retentionPolicy == "" {
		db := w.MetaClient.Database(database)
		if db == nil {
			return cnosdb.ErrDatabaseNotFound(database)
		}
		retentionPolicy = db.DefaultRetentionPolicy
	}

	shardMappings, err := w.MapShards(&WritePointsRequest{Database: database, RetentionPolicy: retentionPolicy, Points: points})
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard *meta.ShardInfo, database, retentionPolicy string, points []models.Point) {
			err := w.writeToShard(shard, database, retentionPolicy, consistencyLevel, points)
			if err == tsdb.ErrShardDeletion {
				err = tsdb.PartialWriteError{Reason: fmt.Sprintf("shard %d is pending deletion", shard.ID), Dropped: len(points)}
			}
			ch <- err
		}(shardMappings.Shards[shardID], database, retentionPolicy, points)
	}

	// Send points to subscriptions if possible.
	var ok, dropped int64
	pts := &WritePointsRequest{Database: database, RetentionPolicy: retentionPolicy, Points: points}
	// We need to lock just in case the channel is about to be nil'ed
	w.mu.RLock()
	for _, ch := range w.subPoints {
		select {
		case ch <- pts:
			ok++
		default:
			dropped++
		}
	}
	w.mu.RUnlock()

	if ok > 0 {
		atomic.AddInt64(&w.stats.SubWriteOK, ok)
	}

	if dropped > 0 {
		atomic.AddInt64(&w.stats.SubWriteDrop, dropped)
	}

	if err == nil && len(shardMappings.Dropped) > 0 {
		err = tsdb.PartialWriteError{Reason: "points beyond retention policy", Dropped: len(shardMappings.Dropped)}

	}
	timeout := time.NewTimer(w.WriteTimeout)
	defer timeout.Stop()
	for range shardMappings.Points {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case <-timeout.C:
			atomic.AddInt64(&w.stats.WriteTimeout, 1)
			// return timeout error to caller
			return ErrTimeout
		case err := <-ch:
			if err != nil {
				return err
			}
		}
	}
	return err
}

// writeToShard writes points to a shard and ensures a write consistency level has been met.  If the write
// partially succeeds, ErrPartialWrite is returned.
func (w *PointsWriter) writeToShard(shard *meta.ShardInfo, database, retentionPolicy string, consistency models.ConsistencyLevel, points []models.Point) error {
	// The required number of writes to achieve the requested consistency level
	required := len(shard.Owners)
	switch consistency {
	case models.ConsistencyLevelAny, models.ConsistencyLevelOne:
		required = 1
	case models.ConsistencyLevelQuorum:
		required = required/2 + 1
	}

	// response channel for each shard writer go routine
	type AsyncWriteResult struct {
		Owner meta.ShardOwner
		Err   error
	}
	ch := make(chan *AsyncWriteResult, len(shard.Owners))

	for _, owner := range shard.Owners {
		go func(shardID uint64, owner meta.ShardOwner, points []models.Point) {
			if owner.NodeID == 0 || w.Node.ID == owner.NodeID {
				atomic.AddInt64(&w.stats.PointWriteReqLocal, int64(len(points)))
				err := w.TSDBStore.WriteToShard(shardID, points)
				// If we've written to shard that should exist on the current node, but the store has
				// not actually created this shard, tell it to create it and retry the write
				if err == tsdb.ErrShardNotFound {
					err = w.TSDBStore.CreateShard(database, retentionPolicy, shardID, true)
					if err != nil {
						ch <- &AsyncWriteResult{owner, err}
						return
					}
					err = w.TSDBStore.WriteToShard(shardID, points)
				}
				ch <- &AsyncWriteResult{owner, err}
			} else {
				atomic.AddInt64(&w.stats.PointWriteReqRemote, int64(len(points)))
				err := w.ShardWriter.WriteShard(shardID, owner.NodeID, points)
				if err != nil && tsdb.IsRetryable(err) {
					// The remote write failed so queue it via hinted handoff
					atomic.AddInt64(&w.stats.WritePointReqHH, int64(len(points)))
					hherr := w.HintedHandoff.WriteShard(shardID, owner.NodeID, points)
					if hherr != nil {
						ch <- &AsyncWriteResult{owner, hherr}
						return
					}

					// If the write consistency level is ANY, then a successful hinted handoff can
					// be considered a successful write so send nil to the response channel
					// otherwise, let the original error propagate to the response channel
					if hherr == nil && consistency == models.ConsistencyLevelAny {
						ch <- &AsyncWriteResult{owner, nil}
						return
					}
				}
				ch <- &AsyncWriteResult{owner, err}
			}
		}(shard.ID, owner, points)
	}

	var wrote int
	timeout := time.After(w.WriteTimeout)
	var writeError error
	for range shard.Owners {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case <-timeout:
			atomic.AddInt64(&w.stats.WriteTimeout, 1)
			// return timeout error to caller
			return ErrTimeout
		case result := <-ch:
			// If the write returned an error, continue to the next response
			if result.Err != nil {
				atomic.AddInt64(&w.stats.WriteErr, 1)
				w.Logger.Info("write failed", zap.Uint64("shard", shard.ID), zap.Uint64("node", result.Owner.NodeID), zap.Error(result.Err))

				// Keep track of the first error we see to return back to the client
				if writeError == nil {
					writeError = result.Err
				}
				continue
			}

			wrote++

			// We wrote the required consistency level
			if wrote >= required {
				atomic.AddInt64(&w.stats.WriteOK, 1)
				return nil
			}
		}
	}

	if wrote > 0 {
		atomic.AddInt64(&w.stats.WritePartial, 1)
		return ErrPartialWrite
	}

	if writeError != nil {
		return fmt.Errorf("write failed: %v", writeError)
	}

	return ErrWriteFailed
}