package speedtest

import (
	"math"
	"sync"
	"testing"
	"time"
)

func BenchmarkDataManager_NewDataChunk(b *testing.B) {
	dmp := NewDataManager()
	dmp.Reset()
	for i := 0; i < b.N; i++ {
		dmp.NewChunk()
	}
}

func BenchmarkDataManager_AddTotalDownload(b *testing.B) {
	dmp := NewDataManager()
	for i := 0; i < b.N; i++ {
		dmp.AddTotalDownload(43521)
	}
}

func TestDataManager_AddTotalDownload(t *testing.T) {
	dmp := NewDataManager()
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				dmp.AddTotalDownload(43521)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if dmp.download.GetTotalDataVolume() != 43521000000 {
		t.Fatal()
	}
}

func TestDataManager_GetAvgDownloadRate(t *testing.T) {
	dm := NewDataManager()
	dm.download.totalDataVolume = 3000000
	dm.captureTime = time.Second * 10

	result := dm.GetAvgDownloadRate()
	if result != 2.4 {
		t.Fatal()
	}
}

func TestDataManager_AverageRateUsesSeconds(t *testing.T) {
	dm := NewDataManager()
	dm.SetCaptureTime(1500 * time.Microsecond)
	dm.AddTotalDownload(123)
	dm.AddTotalUpload(123)
	want := float64(123) * 8 / 1e6 / 0.0015
	for _, got := range []float64{dm.GetAvgDownloadRate(), dm.GetAvgUploadRate()} {
		if math.Abs(got-want) > 1e-12 {
			t.Fatalf("average rate = %g, want %g", got, want)
		}
	}
}

func TestDataManager_GetUploadConfirmationRatio(t *testing.T) {
	dm := NewDataManager()
	dm.upload.AddTotalReadVolume(2 * 1000 * 1000)
	dm.AddTotalUpload(500 * 1000)
	if got := dm.GetUploadBacklog(); got != 1500*1000 {
		t.Fatalf("got upload backlog %d, want %d", got, 1500*1000)
	}
	if got := dm.GetUploadConfirmationRatio(); got != 0.25 {
		t.Fatalf("got confirmation ratio %v, want 0.25", got)
	}

	dm.AddTotalUpload(3 * 1000 * 1000)
	if got := dm.GetUploadConfirmationRatio(); got != 1 {
		t.Fatalf("got confirmation ratio %v, want 1 after clamping", got)
	}
	if got := dm.GetUploadBacklog(); got != 0 {
		t.Fatalf("got upload backlog %d, want 0", got)
	}
}

func TestDataManager_UploadWorkerLimit(t *testing.T) {
	dm := NewDataManager()
	dm.SetNThread(12)
	if dm.uploadMaxWorkers != 12 {
		t.Fatalf("explicit worker limit was capped at %d", dm.uploadMaxWorkers)
	}
	dm.SetNThread(0)
	if dm.uploadMaxWorkers != 8 {
		t.Fatalf("default worker limit is %d, want 8", dm.uploadMaxWorkers)
	}
}

func TestReducedUploadWorkerCount(t *testing.T) {
	if got := reducedUploadWorkerCount(10, 0.5, 1, 0); got != 5 {
		t.Fatalf("got %d workers after a 50%% rate drop, want 5", got)
	}
	if got := reducedUploadWorkerCount(10, 0.9, 1, 0); got != 9 {
		t.Fatalf("got %d workers after a mild rate drop, want 9", got)
	}
	if got := reducedUploadWorkerCount(2, 0.5, 1, 0); got != 1 {
		t.Fatalf("got %d workers at the minimum, want 1", got)
	}
}

func TestSampleVarianceSingleton(t *testing.T) {
	mean, variance, stdDev, min, max := sampleVariance([]int64{42})
	if mean != 42 || variance != 0 || stdDev != 0 || min != 42 || max != 42 {
		t.Fatalf("singleton statistics = %d, %d, %d, %d, %d", mean, variance, stdDev, min, max)
	}
}

func TestDataManager_ResetSnapshots(t *testing.T) {
	dm := NewDataManager()
	store := dm.Snapshots()
	chunk := dm.NewChunk()
	current := dm.Snapshot
	dm.AddTotalDownload(123)
	dm.AddTotalUpload(456)
	dm.Reset()
	if dm.Snapshots() != store || store.Latest() != current {
		t.Fatal("reset did not archive the current snapshot in the existing store")
	}
	if len(*store.Latest()) != 1 || (*store.Latest())[0] != chunk {
		t.Fatal("archived snapshot lost its chunk")
	}
	if dm.Snapshot == current || len(*dm.Snapshot) != 0 {
		t.Fatal("reset did not start an empty snapshot")
	}
	if dm.GetTotalDownload() != 0 || dm.GetTotalUpload() != 0 {
		t.Fatal("reset retained transfer counters")
	}
	for range maxSnapshotSize + 2 {
		dm.Reset()
	}
	if len(store.All()) != maxSnapshotSize {
		t.Fatalf("snapshot history size = %d, want %d", len(store.All()), maxSnapshotSize)
	}
	store.Clean()
	if store.Latest() != nil || len(store.All()) != 0 {
		t.Fatal("clean retained snapshot history")
	}
}
