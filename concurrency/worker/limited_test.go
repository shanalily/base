package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gostdlib/base/telemetry/log"
)

func TestRegressionLimitedCancelRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		p, err := New(ctx, "")
		if err != nil {
			panic(err)
		}
		defer p.Close(ctx)

		const limit = 2
		l := p.Limited(ctx, "", limit)

		// Submit with an already-cancelled context. The token must be released
		// even though the job never runs, otherwise the pool leaks capacity.
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		for i := 0; i < limit; i++ {
			if l.Submit(cancelled, func() { t.Errorf("TestRegressionLimitedCancelRelease: job should not run on cancelled context") }) {
				t.Errorf("TestRegressionLimitedCancelRelease: Submit on a cancelled context returned true, want false")
			}
		}

		// If tokens leaked, this submit on a live context would block forever and the bubble's
		// deadlock detector would fail the test.
		done := make(chan struct{})
		go func() {
			if !l.Submit(ctx, func() {}) {
				t.Errorf("TestRegressionLimitedCancelRelease: Submit on a live context returned false, want true")
			}
			close(done)
		}()

		<-done
	})
}

func TestLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exit := make(chan struct{}, 1)
		var done atomic.Int32
		startedWG := sync.WaitGroup{}
		startedWG.Add(4)
		f := func() {
			startedWG.Done()
			<-exit
			done.Add(1)
		}

		ctx := context.Background()
		p, err := New(ctx, "")
		if err != nil {
			panic(err)
		}
		defer p.Close(ctx)

		l := p.Limited(ctx, "", 4)
		for i := 0; i < 4; i++ {
			if !l.Submit(ctx, f) {
				t.Fatalf("TestLimited: Submit on a live context returned false, want true")
			}
		}

		// Wait for each of these to start running.
		startedWG.Wait()

		blockedHappened := make(chan struct{})
		go func() {
			// This submit blocks until a slot frees, then runs, so it must report true.
			if !l.Submit(ctx, func() { defer close(blockedHappened) }) {
				t.Errorf("TestLimited: blocked Submit on a live context returned false, want true")
			}
		}()

		// Make sure this is working like we think.
		if done.Load() != 0 {
			t.Fatalf("TestLimited: a function ended early")
		}

		// The 5th job cannot run: all 4 limited slots are occupied. Once the bubble settles, the
		// 5th submit is durably blocked acquiring a slot, so its function has not run.
		synctest.Wait()
		select {
		case <-blockedHappened:
			t.Fatalf("TestLimited: a function that should have been blocked wasn't")
		default:
		}

		// Free exactly one slot: one original job finishes, which lets the 5th job run.
		exit <- struct{}{}

		synctest.Wait()
		select {
		case <-blockedHappened:
		default:
			t.Fatalf("TestLimited: a blocked function should have completed")
		}

		// One of our original goroutines should be done.
		if done.Load() != 1 {
			t.Fatalf("TestLimited: goroutine should have finished")
		}

		// Force the others to finish.
		close(exit)

		synctest.Wait()
		if done.Load() != 4 {
			t.Fatalf("TestLimited: all goroutines should have finished")
		}
	})
}

func TestLimitedPoolNameInWarning(t *testing.T) {
	t.Parallel()

	// Save and restore original warnTimer
	originalWarnTimer := warnTimer
	defer func() { warnTimer = originalWarnTimer }()

	// Set short timeout for testing
	warnTimer = 50 * time.Millisecond

	tests := []struct {
		name         string
		limitedName  string
		expectedName string
	}{
		{
			name:         "Named limited pool should use provided name",
			limitedName:  "myLimitedPool",
			expectedName: "myLimitedPool",
		},
		{
			name:         "Empty name should show unnamed",
			limitedName:  "",
			expectedName: "unnamed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Capture log output
			var buf bytes.Buffer
			handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
			testLogger := slog.New(handler)

			// Save and restore original logger
			originalLogger := log.Default()
			log.Set(testLogger)
			defer log.Set(originalLogger)

			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()

				p, err := New(ctx, "testPool")
				if err != nil {
					t.Fatalf("got err == %s, want err == nil", err)
				}
				defer p.Close(ctx)

				// Create a limited pool with the test name
				limited := p.Limited(ctx, test.limitedName, 1)

				// Track completion
				firstDone := make(chan struct{})
				secondDone := make(chan struct{})

				// Fill the slot with a job that blocks until we release it
				release := make(chan struct{})
				limited.Submit(ctx, func() {
					<-release
					close(firstDone)
				})

				// Submit second job that will have to wait
				go func() {
					limited.Submit(ctx, func() {
						close(secondDone)
					})
				}()

				// Let the second submit reach its blocked state, then
				// advance the fake clock past warnTimer so the waiting submit logs its warning
				synctest.Wait()
				time.Sleep(warnTimer + time.Millisecond)
				synctest.Wait()

				// Release the first job
				close(release)

				// Wait for both jobs to complete
				<-firstDone
				<-secondDone

				// Verify the pool name appears in the warning
				logOutput := buf.String()
				expectedMsg := "worker.Pool(" + test.expectedName + ")"

				if !strings.Contains(logOutput, expectedMsg) {
					t.Errorf("expected warning to contain %q, got: %s", expectedMsg, logOutput)
				}
			})
		})
	}
}
