package foundationdb

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	// minCache is the minimum amount of memory in megabytes to allocate to foundationdb
	// read and write caching, split half and half.
	minCache = 16

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16

	batchGrowRec = 3000
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	fn string                      // filename for reporting
	db *fdb.Database               // FoundationDB instance
	ds directory.DirectorySubspace // FoundationDB instance

	compTimeMeter      metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter      metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter     metrics.Meter // Meter for measuring the data written during compaction
	writeDelayNMeter   metrics.Meter // Meter for measuring the write delay number due to database compaction
	writeDelayMeter    metrics.Meter // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge      metrics.Gauge // Gauge for tracking the size of all the levels in the database
	diskReadMeter      metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter     metrics.Meter // Meter for measuring the effective amount of data written
	memCompGauge       metrics.Gauge // Gauge for tracking the number of memory compaction
	level0CompGauge    metrics.Gauge // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge metrics.Gauge // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge      metrics.Gauge // Gauge for tracking the number of table compaction caused by read opt

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// New returns a wrapped FoundationDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, cache int, handles int, namespace string) (*Database, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles)

	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(620)

	// Open the default database from the system cluster
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	ds, err := directory.CreateOrOpen(db, []string{file}, nil)
	if err != nil {
		return nil, err
	}

	// Assemble the wrapper with all the registered metrics
	fdb := &Database{
		fn: file,
		db: &db,
		ds: ds,

		log:      logger,
		quitChan: make(chan chan error),
	}
	fdb.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	fdb.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	fdb.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	fdb.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	fdb.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	fdb.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)
	fdb.writeDelayMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/duration", nil)
	fdb.writeDelayNMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/counter", nil)
	fdb.memCompGauge = metrics.NewRegisteredGauge(namespace+"compact/memory", nil)
	fdb.level0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/level0", nil)
	fdb.nonlevel0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/nonlevel0", nil)
	fdb.seekCompGauge = metrics.NewRegisteredGauge(namespace+"compact/seek", nil)

	return fdb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
		db.quitChan = nil
	}

	return nil
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	exists, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := tr.Get(db.ds.Pack(tuple.Tuple{fdb.Key(key)})).Get()
		if err != nil {
			return nil, err
		}

		if exists == nil {
			return false, nil
		}

		return true, nil
	})

	return exists.(bool), err
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	value, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.Get(db.ds.Pack(tuple.Tuple{fdb.Key(key)})).Get()
	})

	return value.([]byte), err
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	db.db.Transact(func(tr fdb.Transaction) (res interface{}, err error) {
		tr.Set(db.ds.Pack(tuple.Tuple{fdb.Key(key)}), value)
		return
	})

	return nil
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	db.db.Transact(func(tr fdb.Transaction) (res interface{}, err error) {
		tr.Clear(db.ds.Pack(tuple.Tuple{fdb.Key(key)}))
		return
	})

	return nil
}

type Iterator struct {
	rangeIterator *fdb.RangeIterator

	key   []byte
	value []byte
	err   error
}

func (i Iterator) Next() bool {
	if i.rangeIterator == nil {
		return false
	}

	if !i.rangeIterator.Advance() {
		return false
	}

	kv, err := i.rangeIterator.Get()
	i.key = kv.Key
	i.value = kv.Value
	i.err = err

	return true
}

func (i Iterator) Error() error {
	return i.err
}

func (i Iterator) Key() []byte {
	return i.key
}

func (i Iterator) Value() []byte {
	return i.value
}

func (i Iterator) Release() {
	i.rangeIterator = nil
	i.key = nil
	i.value = nil
	i.err = nil
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	iter, _ := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		iter := tr.GetRange(fdb.KeyRange{
			Begin: tuple.Tuple{fdb.Key(append(prefix, start...))},
			End:   nil,
		}, fdb.RangeOptions{})

		return &Iterator{rangeIterator: iter.Iterator()}, nil
	})

	return iter.(Iterator)
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	// TODO: implement stat
	return "5", nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

type keyType uint

// Value types encoded as the last component of internal keys.
// Don't modify; this value are saved to disk.
const (
	keyTypeDel = keyType(0)
	keyTypeVal = keyType(1)
)

type batchIndex struct {
	keyType            keyType
	keyPos, keyLen     int
	valuePos, valueLen int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

func (index batchIndex) kv(data []byte) (key, value []byte) {
	return index.k(data), index.v(data)
}

// batch is a write-only FoundationDB transaction that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db *fdb.Database
	ds directory.DirectorySubspace
	tr fdb.Transaction

	size int

	data  []byte
	index []batchIndex

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen int
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	tr, _ := db.db.CreateTransaction()
	return &batch{
		db: db.db,
		ds: db.ds,
		tr: tr,
	}
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.tr.Set(b.ds.Pack(tuple.Tuple{fdb.Key(key)}), value)
	b.appendRec(keyTypeVal, key, value)
	b.size += len(value)
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.tr.Clear(tuple.Tuple{fdb.Key(key)})
	b.appendRec(keyTypeDel, key, nil)
	b.size++
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	if b.size == 0 {
		return nil
	}

	// give it some time to think about the commit
	time.Sleep(5 * time.Millisecond)
	return b.tr.Commit().Get()
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.tr, _ = b.db.CreateTransaction()
	b.data = b.data[:0]
	b.index = b.index[:0]

	b.size = 0
	b.internalLen = 0
}

func (b *batch) appendRec(kt keyType, key, value []byte) {
	n := 1 + binary.MaxVarintLen32 + len(key)
	if kt == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	index := batchIndex{keyType: kt}
	o := len(b.data)
	data := b.data[:o+n]
	data[o] = byte(kt)
	o++
	o += binary.PutUvarint(data[o:], uint64(len(key)))
	index.keyPos = o
	index.keyLen = len(key)
	o += copy(data[o:], key)
	if kt == keyTypeVal {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	b.index = append(b.index, index)
	b.internalLen += index.keyLen + index.valueLen + 8
}

func (b *batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n {
		div := 1
		if len(b.index) > batchGrowRec {
			div = len(b.index) / batchGrowRec
		}
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	r := &replayer{writer: w}

	for _, index := range b.index {
		switch index.keyType {
		case keyTypeVal:
			r.Put(index.k(b.data), index.v(b.data))
		case keyTypeDel:
			r.Delete(index.k(b.data))
		}
	}

	return nil
}

// replayer is a small wrapper to implement the correct replay methods.
type replayer struct {
	writer  ethdb.KeyValueWriter
	failure error
}

// Put inserts the given value into the key-value data store.
func (r *replayer) Put(key, value []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Put(key, value)
}

// Delete removes the key from the key-value data store.
func (r *replayer) Delete(key []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Delete(key)
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}
