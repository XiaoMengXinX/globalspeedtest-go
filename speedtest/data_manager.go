package speedtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest/internal"
)

type Manager interface {
	SetRateCaptureFrequency(duration time.Duration) Manager
	SetCaptureTime(duration time.Duration) Manager

	NewChunk() Chunk

	GetTotalDownload() int64
	GetTotalUpload() int64
	GetUploadBacklog() int64
	GetUploadConfirmationRatio() float64
	AddTotalDownload(value int64)
	AddTotalUpload(value int64)

	GetAvgDownloadRate() float64
	GetAvgUploadRate() float64

	GetEWMADownloadRate() float64
	GetEWMAUploadRate() float64

	SetCallbackDownload(callback func(downRate ByteRate))
	SetCallbackUpload(callback func(upRate ByteRate))

	PrepareDownload()
	PrepareUpload()
	RegisterDownloadHandler(fn func()) *TestDirection
	RegisterUploadHandler(fn func()) *TestDirection

	// Wait for the upload or download task to end to avoid errors caused by core occupation
	Wait()
	Reset()
	Snapshots() *Snapshots

	SetNThread(n int) Manager
	WorkerLimit(upload bool) int
}

type Chunk interface {
	UploadHandler(size int64) Chunk
	DownloadHandler(r io.Reader) error

	GetRate() float64
	GetDuration() time.Duration
	GetParent() Manager

	Read(b []byte) (n int, err error)
}

const readChunkSize = 1024 // 1 KBytes with higher frequency rate feedback

type DataType int32

const (
	typeEmptyChunk = iota
	typeDownload
	typeUpload
)

var (
	ErrorUninitializedManager = errors.New("uninitialized manager")
)

type funcGroup struct {
	fns []func()
}

func (f *funcGroup) Add(fn func()) {
	f.fns = append(f.fns, fn)
}

type DataManager struct {
	SnapshotStore *Snapshots
	Snapshot      *Snapshot
	sync.Mutex

	repeatByte *[]byte

	captureTime          time.Duration
	rateCaptureFrequency time.Duration
	nThread              int
	uploadMaxWorkers     int

	lifecycle sync.Mutex
	running   bool
	runningRW sync.RWMutex

	download *TestDirection
	upload   *TestDirection
}

type TestDirection struct {
	mu              sync.RWMutex
	TestType        int                         // test type
	manager         *DataManager                // manager
	totalDataVolume int64                       // total send/receive data volume
	totalReadVolume int64                       // upload bytes read into the transport
	activeWorkers   int32                       // current upload worker limit
	maxWorkers      int32                       // configured upload worker limit
	RateSequence    []int64                     // rate history sequence
	welford         *internal.Welford           // std/EWMA/mean
	captureCallback func(realTimeRate ByteRate) // user callback
	closeFunc       func()                      // close func
	*funcGroup                                  // actually exec function
}

func (dm *DataManager) NewDataDirection(testType int) *TestDirection {
	return &TestDirection{
		TestType:  testType,
		manager:   dm,
		funcGroup: &funcGroup{},
	}
}

func NewDataManager() *DataManager {
	r := bytes.Repeat([]byte{0xAA}, readChunkSize) // uniformly distributed sequence of bits
	ret := &DataManager{
		nThread:              runtime.NumCPU(),
		captureTime:          time.Second * 15,
		rateCaptureFrequency: time.Millisecond * 50,
		Snapshot:             &Snapshot{},
		repeatByte:           &r,
		uploadMaxWorkers:     8,
	}
	ret.download = ret.NewDataDirection(typeDownload)
	ret.upload = ret.NewDataDirection(typeUpload)
	ret.SnapshotStore = newHistorySnapshots(maxSnapshotSize)
	return ret
}

func (dm *DataManager) SetCallbackDownload(callback func(downRate ByteRate)) {
	dm.download.mu.Lock()
	dm.download.captureCallback = callback
	dm.download.mu.Unlock()
}

func (dm *DataManager) SetCallbackUpload(callback func(upRate ByteRate)) {
	dm.upload.mu.Lock()
	dm.upload.captureCallback = callback
	dm.upload.mu.Unlock()
}

func (dm *DataManager) Wait() {
	dm.lifecycle.Lock()
	dm.lifecycle.Unlock()
}

func (dm *DataManager) PrepareDownload() {
	dm.lifecycle.Lock()
	defer dm.lifecycle.Unlock()
	dm.download.prepare()
}

func (dm *DataManager) PrepareUpload() {
	dm.lifecycle.Lock()
	defer dm.lifecycle.Unlock()
	dm.upload.prepare()
}

func (td *TestDirection) prepare() {
	td.mu.Lock()
	defer td.mu.Unlock()
	td.fns = nil
	td.resetMeasurements()
}

func (td *TestDirection) resetMeasurements() {
	atomic.StoreInt64(&td.totalDataVolume, 0)
	atomic.StoreInt64(&td.totalReadVolume, 0)
	atomic.StoreInt32(&td.activeWorkers, 0)
	atomic.StoreInt32(&td.maxWorkers, 0)
	td.RateSequence = nil
	td.welford = nil
	td.closeFunc = nil
}

func (dm *DataManager) RegisterUploadHandler(fn func()) *TestDirection {
	return dm.registerHandler(dm.upload, fn)
}

func (dm *DataManager) RegisterDownloadHandler(fn func()) *TestDirection {
	return dm.registerHandler(dm.download, fn)
}

func (dm *DataManager) registerHandler(td *TestDirection, fn func()) *TestDirection {
	dm.Lock()
	limit := dm.nThread
	dm.Unlock()
	td.mu.Lock()
	if len(td.fns) < limit {
		td.Add(fn)
	}
	td.mu.Unlock()
	return td
}

func (td *TestDirection) GetTotalDataVolume() int64 {
	return atomic.LoadInt64(&td.totalDataVolume)
}

func (td *TestDirection) AddTotalDataVolume(delta int64) int64 {
	return atomic.AddInt64(&td.totalDataVolume, delta)
}

func (td *TestDirection) GetTotalReadVolume() int64 {
	return atomic.LoadInt64(&td.totalReadVolume)
}

func (td *TestDirection) AddTotalReadVolume(delta int64) int64 {
	return atomic.AddInt64(&td.totalReadVolume, delta)
}

func (td *TestDirection) Start(cancel context.CancelFunc, mainRequestHandlerIndex int) {
	td.StartContext(context.Background(), cancel, mainRequestHandlerIndex)
}

// StartContext runs a phase and waits for its workers and sampler to stop.
// Handlers must return when cancel is called.
func (td *TestDirection) StartContext(ctx context.Context, cancel context.CancelFunc, mainRequestHandlerIndex int) {
	dm := td.manager
	dm.lifecycle.Lock()
	defer dm.lifecycle.Unlock()

	dm.Lock()
	workerLimit, uploadMaxWorkers := dm.workerLimit(td.TestType == typeUpload), dm.uploadMaxWorkers
	captureTime, frequency := dm.captureTime, dm.rateCaptureFrequency
	dm.Unlock()
	td.mu.Lock()
	fns := append([]func(){}, td.fns...)
	td.resetMeasurements()
	td.mu.Unlock()
	if len(fns) == 0 {
		panic("empty task stack")
	}
	if mainRequestHandlerIndex < 0 || mainRequestHandlerIndex >= len(fns) {
		mainRequestHandlerIndex = 0
	}
	mainN := int(0.1 * float64(len(fns)))
	if mainN < 1 {
		mainN = 1
	}
	if len(fns) == 1 || mainN > workerLimit {
		mainN = workerLimit
	}
	atomic.StoreInt32(&td.maxWorkers, int32(workerLimit))
	initialWorkers := workerLimit
	if td.TestType == typeUpload && uploadMaxWorkers <= 8 && initialWorkers > runtime.NumCPU() {
		initialWorkers = runtime.NumCPU()
	}
	atomic.StoreInt32(&td.activeWorkers, int32(initialWorkers))

	runCtx, stopRun := context.WithCancel(ctx)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			dm.runningRW.Lock()
			dm.running = false
			dm.runningRW.Unlock()
			stopRun()
			if cancel != nil {
				cancel()
			}
		})
	}
	defer stop()
	if ctx.Err() != nil {
		return
	}
	dm.runningRW.Lock()
	dm.running = true
	dm.runningRW.Unlock()
	td.mu.Lock()
	td.closeFunc = stop
	td.mu.Unlock()

	timer := time.NewTimer(captureTime)
	defer timer.Stop()
	stable := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Go(func() { td.rateCapture(runCtx, frequency, stable) })
	if td.TestType == typeUpload && workerLimit > 1 {
		wg.Go(func() { td.adaptUploadWorkers(runCtx) })
	}
	aux := make([]func(), 0, len(fns)-1)
	for i, fn := range fns {
		if i != mainRequestHandlerIndex {
			aux = append(aux, fn)
		}
	}
	for workerID := 0; workerID < workerLimit; workerID++ {
		fn := fns[mainRequestHandlerIndex]
		if workerID >= mainN {
			fn = aux[(workerID-mainN)%len(aux)]
		}
		wg.Go(func() { td.runWorker(runCtx, workerID, fn) })
	}
	select {
	case <-runCtx.Done():
	case <-timer.C:
	case <-stable:
	}
	stop()
	wg.Wait()
	td.mu.Lock()
	td.closeFunc = nil
	td.mu.Unlock()
}

func (td *TestDirection) runWorker(ctx context.Context, workerID int, fn func()) {
	for ctx.Err() == nil {
		if !td.workerEnabled(workerID) {
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		fn()
	}
}

func (td *TestDirection) workerEnabled(workerID int) bool {
	if td.TestType != typeUpload {
		return true
	}
	return int32(workerID) < atomic.LoadInt32(&td.activeWorkers)
}

func (td *TestDirection) adaptUploadWorkers(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	previousConfirmed := td.GetTotalDataVolume()
	previousAt := time.Now()
	bestRate := 0.0
	maxWorkers := atomic.LoadInt32(&td.maxWorkers)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		confirmed := td.GetTotalDataVolume()
		delta := confirmed - previousConfirmed
		elapsed := now.Sub(previousAt).Seconds()
		previousConfirmed = confirmed
		previousAt = now
		if delta <= 0 || elapsed <= 0 {
			continue
		}

		rate := float64(delta) / elapsed
		active := atomic.LoadInt32(&td.activeWorkers)
		read := td.GetTotalReadVolume()
		backlog := read - confirmed
		confirmation := td.manager.GetUploadConfirmationRatio()
		if bestRate == 0 || rate > bestRate*1.05 {
			bestRate = rate
			if active < maxWorkers {
				active++
				atomic.StoreInt32(&td.activeWorkers, active)
				dbg.Printf("Upload workers: %d/%d (confirmed: %.2f MB/s, confirmation: %.1f%%, backlog: %.2f MB)\n", active, maxWorkers, rate/1000/1000, confirmation*100, float64(backlog)/1000/1000)
			}
			continue
		}

		if active > 1 && (rate < bestRate*0.85 || backlog > int64(active)*2*1024*1024) {
			active = reducedUploadWorkerCount(active, rate, bestRate, backlog)
			atomic.StoreInt32(&td.activeWorkers, active)
			dbg.Printf("Upload workers: %d/%d (confirmed: %.2f MB/s, confirmation: %.1f%%, backlog: %.2f MB)\n", active, maxWorkers, rate/1000/1000, confirmation*100, float64(backlog)/1000/1000)
			bestRate = rate
		}
	}
}

func reducedUploadWorkerCount(active int32, rate, bestRate float64, backlog int64) int32 {
	if active <= 1 {
		return 1
	}

	factor := 1.0
	if bestRate > 0 && rate < bestRate*0.85 {
		factor = math.Min(factor, rate/bestRate)
	}
	backlogLimit := int64(active) * 2 * 1024 * 1024
	if backlog > backlogLimit {
		factor = math.Min(factor, float64(backlogLimit)/float64(backlog))
	}
	if factor >= 1 {
		return active - 1
	}

	target := int32(math.Ceil(float64(active) * factor))
	if target >= active {
		target = active - 1
	}
	if target < 1 {
		return 1
	}
	return target
}

func (td *TestDirection) rateCapture(ctx context.Context, frequency time.Duration, stable chan<- struct{}) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	welfordFrequency := min(frequency, time.Second)
	welford := internal.NewWelford(5*time.Second, welfordFrequency)
	scale := float64(time.Second / welfordFrequency)
	started := time.Now()
	previousAt := started
	var previousTotal int64
	sampled := false
	sample := func() bool {
		now := time.Now()
		total := td.GetTotalDataVolume()
		delta := total - previousTotal
		elapsed := now.Sub(previousAt).Seconds()
		rate := 0.0
		if elapsed > 0 {
			rate = float64(delta) / elapsed
		}
		globalAvg := 0.0
		if since := now.Sub(started).Seconds(); since > 0 {
			globalAvg = float64(total) / since
		}
		previousTotal, previousAt = total, now
		if td.TestType == typeUpload {
			// Completed upload requests confirm bytes in batches.
			rate = globalAvg
		}
		td.mu.Lock()
		if delta != 0 {
			td.RateSequence = append(td.RateSequence, delta)
		}
		isStable := welford.Update(globalAvg, rate/scale)
		td.welford = welford
		capturedRate := ByteRate(welford.EWMA())
		callback := td.captureCallback
		td.mu.Unlock()
		if callback != nil {
			callback(capturedRate)
		}
		return isStable
	}
	for {
		select {
		case <-ctx.Done():
			if !sampled {
				sample()
			}
			return
		case <-ticker.C:
		}
		sampled = true
		if sample() {
			select {
			case stable <- struct{}{}:
			default:
			}
		}
	}
}

func (dm *DataManager) NewChunk() Chunk {
	var dc DataChunk
	dc.manager = dm
	dm.Lock()
	*dm.Snapshot = append(*dm.Snapshot, &dc)
	dm.Unlock()
	return &dc
}

func (dm *DataManager) AddTotalDownload(value int64) {
	dm.download.AddTotalDataVolume(value)
}

func (dm *DataManager) AddTotalUpload(value int64) {
	dm.upload.AddTotalDataVolume(value)
}

func (dm *DataManager) GetTotalDownload() int64 {
	return dm.download.GetTotalDataVolume()
}

func (dm *DataManager) GetTotalUpload() int64 {
	return dm.upload.GetTotalDataVolume()
}

// GetUploadBacklog returns upload bytes read by the transport but not yet
// confirmed by a successful upload response.
func (dm *DataManager) GetUploadBacklog() int64 {
	backlog := dm.upload.GetTotalReadVolume() - dm.GetTotalUpload()
	if backlog < 0 {
		return 0
	}
	return backlog
}

// GetUploadConfirmationRatio returns server-confirmed upload bytes divided by
// bytes read from upload request bodies. It is an application-level estimate,
// not a TCP ACK counter.
func (dm *DataManager) GetUploadConfirmationRatio() float64 {
	read := dm.upload.GetTotalReadVolume()
	if read <= 0 {
		return 0
	}
	confirmed := dm.GetTotalUpload()
	ratio := float64(confirmed) / float64(read)
	if ratio > 1 {
		return 1
	}
	if ratio < 0 {
		return 0
	}
	return ratio
}

func (dm *DataManager) SetRateCaptureFrequency(duration time.Duration) Manager {
	dm.Lock()
	dm.rateCaptureFrequency = duration
	dm.Unlock()
	return dm
}

func (dm *DataManager) SetCaptureTime(duration time.Duration) Manager {
	dm.Lock()
	dm.captureTime = duration
	dm.Unlock()
	return dm
}

func (dm *DataManager) SetNThread(n int) Manager {
	dm.Lock()
	defer dm.Unlock()
	if n < 1 {
		dm.nThread = runtime.NumCPU()
		dm.uploadMaxWorkers = 8
	} else {
		dm.nThread = n
		dm.uploadMaxWorkers = n
	}
	return dm
}

// WorkerLimit reports how many handlers a phase schedules concurrently.
func (dm *DataManager) WorkerLimit(upload bool) int {
	dm.Lock()
	defer dm.Unlock()
	return dm.workerLimit(upload)
}

// workerLimit requires dm's lock to be held.
func (dm *DataManager) workerLimit(upload bool) int {
	if upload && dm.uploadMaxWorkers < dm.nThread {
		return dm.uploadMaxWorkers
	}
	return dm.nThread
}

func (dm *DataManager) Snapshots() *Snapshots {
	return dm.SnapshotStore
}

func (dm *DataManager) Reset() {
	dm.lifecycle.Lock()
	defer dm.lifecycle.Unlock()
	dm.Lock()
	dm.SnapshotStore.push(dm.Snapshot)
	dm.Snapshot = &Snapshot{}
	dm.Unlock()
	dm.download.prepare()
	dm.upload.prepare()
}

func (dm *DataManager) averageRate(total int64) float64 {
	dm.Lock()
	duration := dm.captureTime
	dm.Unlock()
	if duration <= 0 {
		return 0
	}
	return float64(total) * 8 / 1e6 / duration.Seconds()
}

func (dm *DataManager) GetAvgDownloadRate() float64 {
	return dm.averageRate(dm.GetTotalDownload())
}

func (td *TestDirection) ewmaRate() float64 {
	td.mu.RLock()
	defer td.mu.RUnlock()
	if td.welford != nil {
		return td.welford.EWMA()
	}
	return 0
}

func (dm *DataManager) GetEWMADownloadRate() float64 {
	return dm.download.ewmaRate()
}

func (dm *DataManager) GetAvgUploadRate() float64 {
	return dm.averageRate(dm.GetTotalUpload())
}

func (dm *DataManager) GetEWMAUploadRate() float64 {
	return dm.upload.ewmaRate()
}

type DataChunk struct {
	manager             *DataManager
	dateType            DataType
	startTime           time.Time
	endTime             time.Time
	err                 error
	ContentLength       int64
	remainOrDiscardSize int64
}

var blackHolePool = sync.Pool{
	New: func() any {
		b := make([]byte, 8192)
		return &b
	},
}

func (dc *DataChunk) GetDuration() time.Duration {
	return dc.endTime.Sub(dc.startTime)
}

func (dc *DataChunk) GetRate() float64 {
	switch dc.dateType {
	case typeDownload:
		return float64(dc.remainOrDiscardSize) / dc.GetDuration().Seconds()
	case typeUpload:
		return float64(dc.ContentLength-dc.remainOrDiscardSize) * 8 / 1000 / 1000 / dc.GetDuration().Seconds()
	default:
		return 0
	}
}

// DownloadHandler No value will be returned here, because the error will interrupt the test.
// The error chunk is generally caused by the remote server actively closing the connection.
func (dc *DataChunk) DownloadHandler(r io.Reader) error {
	if dc.dateType != typeEmptyChunk {
		dc.err = errors.New("multiple calls to the same chunk handler are not allowed")
		return dc.err
	}
	dc.dateType = typeDownload
	dc.startTime = time.Now()
	defer func() {
		dc.endTime = time.Now()
	}()
	bufP := blackHolePool.Get().(*[]byte)
	defer blackHolePool.Put(bufP)
	readSize := 0
	for {
		dc.manager.runningRW.RLock()
		running := dc.manager.running
		dc.manager.runningRW.RUnlock()
		if !running {
			return nil
		}
		readSize, dc.err = r.Read(*bufP)
		rs := int64(readSize)

		dc.remainOrDiscardSize += rs
		dc.manager.download.AddTotalDataVolume(rs)
		if dc.err != nil {
			if dc.err == io.EOF {
				return nil
			}
			return dc.err
		}
	}
}

func (dc *DataChunk) UploadHandler(size int64) Chunk {
	if dc.dateType != typeEmptyChunk {
		dc.err = errors.New("multiple calls to the same chunk handler are not allowed")
	}

	if size <= 0 {
		panic("the size of repeated bytes should be > 0")
	}

	dc.ContentLength = size
	dc.remainOrDiscardSize = size
	dc.dateType = typeUpload
	dc.startTime = time.Now()
	return dc
}

func (dc *DataChunk) GetParent() Manager {
	return dc.manager
}

func (dc *DataChunk) Read(b []byte) (n int, err error) {
	if dc.remainOrDiscardSize < readChunkSize {
		if dc.remainOrDiscardSize <= 0 {
			dc.endTime = time.Now()
			return n, io.EOF
		}
		n = copy(b, (*dc.manager.repeatByte)[:dc.remainOrDiscardSize])
	} else {
		n = copy(b, *dc.manager.repeatByte)
	}
	n64 := int64(n)
	dc.remainOrDiscardSize -= n64
	if dc.dateType == typeUpload {
		dc.manager.upload.AddTotalReadVolume(n64)
	}
	return
}

// calcMAFilter Median-Averaging Filter
func _(list []int64) float64 {
	if len(list) == 0 {
		return 0
	}
	var sum int64 = 0
	n := len(list)
	if n == 0 {
		return 0
	}
	length := len(list)
	for i := 0; i < length-1; i++ {
		for j := i + 1; j < length; j++ {
			if list[i] > list[j] {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
	for i := 1; i < n-1; i++ {
		sum += list[i]
	}
	return float64(sum) / float64(n-2)
}

func pautaFilter(vector []int64) []int64 {
	dbg.Println("Per capture unit")
	dbg.Printf("Raw Sequence len: %d\n", len(vector))
	dbg.Printf("Raw Sequence: %v\n", vector)
	if len(vector) == 0 {
		return vector
	}
	mean, _, std, _, _ := sampleVariance(vector)
	var retVec []int64
	for _, value := range vector {
		if math.Abs(float64(value-mean)) < float64(3*std) {
			retVec = append(retVec, value)
		}
	}
	dbg.Printf("Raw average: %dByte\n", mean)
	dbg.Printf("Pauta Sequence len: %d\n", len(retVec))
	dbg.Printf("Pauta Sequence: %v\n", retVec)
	return retVec
}

// sampleVariance sample Variance
func sampleVariance(vector []int64) (mean, variance, stdDev, min, max int64) {
	if len(vector) == 0 {
		return 0, 0, 0, 0, 0
	}
	var sumNum, accumulate int64
	min = math.MaxInt64
	max = math.MinInt64
	for _, value := range vector {
		sumNum += value
		if min > value {
			min = value
		}
		if max < value {
			max = value
		}
	}
	mean = sumNum / int64(len(vector))
	if len(vector) == 1 {
		return
	}
	for _, value := range vector {
		accumulate += (value - mean) * (value - mean)
	}
	variance = accumulate / int64(len(vector)-1) // Bessel's correction
	stdDev = int64(math.Sqrt(float64(variance)))
	return
}

const maxSnapshotSize = 10

type Snapshot []*DataChunk

type Snapshots struct {
	mu      sync.RWMutex
	sp      []*Snapshot
	maxSize int
}

func newHistorySnapshots(size int) *Snapshots {
	return &Snapshots{
		sp:      make([]*Snapshot, 0, size),
		maxSize: size,
	}
}

func (rs *Snapshots) push(value *Snapshot) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.sp) == rs.maxSize {
		rs.sp = rs.sp[1:]
	}
	rs.sp = append(rs.sp, value)
}

func (rs *Snapshots) Latest() *Snapshot {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if len(rs.sp) > 0 {
		return rs.sp[len(rs.sp)-1]
	}
	return nil
}

func (rs *Snapshots) All() []*Snapshot {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return append([]*Snapshot{}, rs.sp...)
}

func (rs *Snapshots) Clean() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.sp = make([]*Snapshot, 0, rs.maxSize)
}
