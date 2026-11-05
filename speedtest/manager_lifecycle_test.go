package speedtest

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitManagerPhase(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager phase did not stop")
	}
}

func TestManagerLifecycleParentCancellation(t *testing.T) {
	for _, upload := range []bool{false, true} {
		name := "download"
		if upload {
			name = "upload"
		}
		t.Run(name, func(t *testing.T) {
			dm := NewDataManager()
			dm.SetNThread(3).SetCaptureTime(5 * time.Second).SetRateCaptureFrequency(time.Millisecond)
			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			requestCtx, cancelRequests := context.WithCancel(parent)
			defer cancelRequests()
			var active, cancellations atomic.Int32
			started := make(chan struct{}, 3)
			handler := func() {
				active.Add(1)
				defer active.Add(-1)
				started <- struct{}{}
				<-requestCtx.Done()
			}
			td := dm.RegisterDownloadHandler(handler)
			if upload {
				td = dm.RegisterUploadHandler(handler)
			}
			done := make(chan struct{})
			go func() {
				td.StartContext(parent, func() {
					cancellations.Add(1)
					cancelRequests()
				}, 0)
				close(done)
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("worker did not start")
			}
			stoppedAt := time.Now()
			cancelParent()
			waitManagerPhase(t, done)
			if elapsed := time.Since(stoppedAt); elapsed > 300*time.Millisecond {
				t.Fatalf("cancellation took %s", elapsed)
			}
			if active.Load() != 0 || cancellations.Load() != 1 {
				t.Fatalf("after return: active workers = %d, cancellations = %d", active.Load(), cancellations.Load())
			}
			dm.runningRW.RLock()
			running := dm.running
			dm.runningRW.RUnlock()
			if running {
				t.Fatal("manager is still running")
			}
		})
	}
}

func TestManagerLifecycleCaptureTime(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "context"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			dm := NewDataManager()
			dm.SetNThread(2).SetCaptureTime(30 * time.Millisecond).SetRateCaptureFrequency(time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var active atomic.Int32
			td := dm.RegisterDownloadHandler(func() {
				active.Add(1)
				defer active.Add(-1)
				<-ctx.Done()
			})
			started := time.Now()
			done := make(chan struct{})
			go func() {
				if legacy {
					td.Start(cancel, 0)
				} else {
					td.StartContext(ctx, cancel, 0)
				}
				close(done)
			}()
			waitManagerPhase(t, done)
			if elapsed := time.Since(started); elapsed < 30*time.Millisecond || elapsed > 300*time.Millisecond {
				t.Fatalf("capture duration = %s", elapsed)
			}
			if ctx.Err() != context.Canceled || active.Load() != 0 {
				t.Fatal("capture limit did not cancel and join workers")
			}
		})
	}
}

func TestManagerLifecycleSuccessiveTimers(t *testing.T) {
	dm := NewDataManager()
	dm.SetNThread(1).SetCaptureTime(50 * time.Millisecond).SetRateCaptureFrequency(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dm.SetCallbackDownload(func(ByteRate) { cancel() })
	dm.RegisterDownloadHandler(func() { <-ctx.Done() }).StartContext(ctx, cancel, 0)

	dm.PrepareDownload()
	dm.SetCallbackDownload(nil)
	dm.SetCaptureTime(90 * time.Millisecond)
	nextCtx, cancelNext := context.WithCancel(context.Background())
	defer cancelNext()
	started := time.Now()
	dm.RegisterDownloadHandler(func() { <-nextCtx.Done() }).StartContext(nextCtx, cancelNext, 0)
	if elapsed := time.Since(started); elapsed < 90*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("second phase duration = %s", elapsed)
	}
}

func TestManagerLifecycleCanceledBeforeStart(t *testing.T) {
	dm := NewDataManager()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls, cancellations atomic.Int32
	td := dm.RegisterDownloadHandler(func() { calls.Add(1) })
	td.StartContext(ctx, func() { cancellations.Add(1) }, 0)
	if calls.Load() != 0 || cancellations.Load() != 1 {
		t.Fatalf("pre-canceled start: handler calls = %d, cancellations = %d", calls.Load(), cancellations.Load())
	}
}

func TestManagerLifecycleJoinsCallback(t *testing.T) {
	dm := NewDataManager()
	dm.SetNThread(1).SetCaptureTime(time.Second).SetRateCaptureFrequency(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(releaseCallback) })
	var callbacks atomic.Int32
	dm.SetCallbackDownload(func(ByteRate) {
		if callbacks.Add(1) == 1 {
			close(callbackStarted)
		}
		<-releaseCallback
	})
	workerStopped := make(chan struct{})
	td := dm.RegisterDownloadHandler(func() {
		<-ctx.Done()
		close(workerStopped)
	})
	done := make(chan struct{})
	go func() {
		td.StartContext(ctx, cancel, 0)
		close(done)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	cancel()
	select {
	case <-workerStopped:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	select {
	case <-done:
		t.Fatal("phase returned while callback was still running")
	default:
	}
	release.Do(func() { close(releaseCallback) })
	waitManagerPhase(t, done)
	if callbacks.Load() != 1 {
		t.Fatalf("callback count = %d, want 1", callbacks.Load())
	}
}

func TestManagerLifecyclePrepareDirection(t *testing.T) {
	dm := NewDataManager()
	dm.AddTotalDownload(100)
	dm.AddTotalUpload(200)
	dm.upload.AddTotalReadVolume(300)
	dm.RegisterDownloadHandler(func() {})
	dm.RegisterUploadHandler(func() {})
	dm.PrepareDownload()
	if dm.GetTotalDownload() != 0 || len(dm.download.fns) != 0 {
		t.Fatal("download preparation retained phase state")
	}
	if dm.GetTotalUpload() != 200 || dm.upload.GetTotalReadVolume() != 300 || len(dm.upload.fns) != 1 {
		t.Fatal("download preparation changed upload state")
	}
	dm.AddTotalDownload(400)
	dm.PrepareUpload()
	if dm.GetTotalDownload() != 400 {
		t.Fatal("upload preparation changed download totals")
	}
	if dm.GetTotalUpload() != 0 || dm.GetUploadBacklog() != 0 || dm.GetUploadConfirmationRatio() != 0 || len(dm.upload.fns) != 0 {
		t.Fatal("upload preparation retained phase state")
	}
}

func TestManagerLifecycleRepeatedPhases(t *testing.T) {
	dm := NewDataManager()
	dm.SetNThread(1).SetCaptureTime(15 * time.Millisecond).SetRateCaptureFrequency(time.Millisecond)
	var downloadCallbacks, uploadCallbacks atomic.Int32
	dm.SetCallbackDownload(func(ByteRate) { downloadCallbacks.Add(1) })
	dm.SetCallbackUpload(func(ByteRate) { uploadCallbacks.Add(1) })
	download, upload := dm.download, dm.upload
	for phase := 0; phase < 3; phase++ {
		if phase == 2 {
			dm.Reset()
		}
		for _, isUpload := range []bool{false, true} {
			prepare := dm.PrepareDownload
			register := dm.RegisterDownloadHandler
			callbacks := &downloadCallbacks
			td := dm.download
			if isUpload {
				prepare, register, callbacks, td = dm.PrepareUpload, dm.RegisterUploadHandler, &uploadCallbacks, dm.upload
			}
			prepare()
			if td.welford != nil || len(td.RateSequence) != 0 || td.GetTotalDataVolume() != 0 || td.GetTotalReadVolume() != 0 {
				t.Fatal("preparation retained measurements")
			}
			ctx, cancel := context.WithCancel(context.Background())
			before := callbacks.Load()
			var handlerCalls atomic.Int32
			register(func() {
				handlerCalls.Add(1)
				td.AddTotalDataVolume(100)
				if isUpload {
					td.AddTotalReadVolume(200)
				}
				select {
				case <-ctx.Done():
				case <-time.After(time.Millisecond):
				}
			}).StartContext(ctx, cancel, -1)
			cancel()
			if handlerCalls.Load() == 0 || td.GetTotalDataVolume() != int64(handlerCalls.Load())*100 {
				t.Fatal("phase reused an earlier handler or counter")
			}
			if callbacks.Load() <= before {
				t.Fatal("phase lost its callback")
			}
			if isUpload && dm.GetUploadConfirmationRatio() != 0.5 {
				t.Fatal("phase retained earlier upload read totals")
			}
		}
	}
	if dm.download != download || dm.upload != upload || dm.nThread != 1 || dm.captureTime != 15*time.Millisecond || dm.rateCaptureFrequency != time.Millisecond {
		t.Fatal("phase reset changed direction identity or settings")
	}
}

func TestManagerLifecycleConcurrentSampling(t *testing.T) {
	dm := NewDataManager()
	dm.SetNThread(2).SetCaptureTime(25 * time.Millisecond).SetRateCaptureFrequency(750 * time.Microsecond)
	callback := func(rate ByteRate) {
		if math.IsNaN(float64(rate)) || math.IsInf(float64(rate), 0) {
			t.Error("invalid sampled rate")
		}
		_ = dm.GetEWMADownloadRate()
		_ = dm.GetEWMAUploadRate()
	}
	dm.SetCallbackDownload(callback)
	dm.SetCallbackUpload(callback)
	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	readers.Go(func() {
		for {
			select {
			case <-stopReaders:
				return
			default:
			}
			dm.SetCallbackDownload(callback)
			dm.SetCallbackUpload(callback)
			_ = dm.GetEWMADownloadRate()
			_ = dm.GetEWMAUploadRate()
			_ = dm.GetAvgDownloadRate()
			_ = dm.GetAvgUploadRate()
			_ = dm.GetTotalDownload()
			_ = dm.GetUploadConfirmationRatio()
			_ = dm.Snapshots().All()
		}
	})
	defer func() {
		close(stopReaders)
		readers.Wait()
	}()
	for range 3 {
		ctx, cancel := context.WithCancel(context.Background())
		dm.RegisterDownloadHandler(func() {
			dm.AddTotalDownload(100)
			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
		}).StartContext(ctx, cancel, 0)
		cancel()
		dm.PrepareDownload()
		ctx, cancel = context.WithCancel(context.Background())
		dm.RegisterUploadHandler(func() {
			dm.upload.AddTotalReadVolume(200)
			dm.AddTotalUpload(100)
			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
		}).StartContext(ctx, cancel, 0)
		cancel()
		dm.Reset()
	}
}
