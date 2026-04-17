package ebi

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestHandleChannel_ErrorSendDoesNotDeadlockWhenReaderGone reproduces the
// production deadlock observed 2026-04-17: an unbuffered errorCh where the
// reader goroutine exits via ctx.Done() first, then the writer tries to
// send an error. Without the ctx-aware select in the send, this hangs.
//
// The test asserts the work handler returns within a tight deadline instead
// of blocking forever.
func TestHandleChannel_ErrorSendDoesNotDeadlockWhenReaderGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errorCh := make(chan error) // UNBUFFERED — matches UVS usage.
	workCh := make(chan int, 1)
	doneCh := make(chan struct{})

	// Kill the error reader immediately to simulate the post-ctx-cancel state
	// where the reader has already exited.
	readerDone := make(chan struct{})

	go func() {
		// Reader that exits on ctx cancel (same pattern as UVS's error handler).
		for {
			select {
			case <-ctx.Done():
				close(readerDone)

				return
			case err := <-errorCh:
				// Drop it — simulates early reader exit by not processing.
				_ = err
			}
		}
	}()

	// Send one work item that will make cbFunc return an error.
	workCh <- 1

	// Cancel ctx and wait for reader to exit BEFORE the worker tries to send.
	cancel()
	<-readerDone

	// Now run HandleChannel as the work handler. Its cbFunc returns an error.
	// With the fix, HandleChannel must return promptly (because ctx is already
	// cancelled). Without the fix, the errorCh <- err send blocks forever.
	workerDone := make(chan struct{})

	go func() {
		HandleChannel(ctx, workCh, errorCh, doneCh, func(_ int) error {
			return errors.New("forced")
		})

		close(workerDone)
	}()

	select {
	case <-workerDone:
		// PASS — the handler unblocked on ctx.Done despite the dead reader.
	case <-time.After(2 * time.Second):
		t.Fatal("HandleChannel deadlocked on errorCh send after reader exited; " +
			"the ctx-aware select fix is missing or broken")
	}
}

// TestHandleChannel_NilErrorChDoesNotPanic verifies the nil errorCh guard.
// A bare send on a nil channel blocks forever in Go; the guard must catch
// that case.
func TestHandleChannel_NilErrorChDoesNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workCh := make(chan int, 1)
	workCh <- 1

	close(workCh)

	doneCh := make(chan struct{})

	// No panic, no hang — even though cbFunc returns an error and errorCh is nil.
	done := make(chan struct{})

	go func() {
		HandleChannel(ctx, workCh, nil, doneCh, func(_ int) error {
			return errors.New("forced")
		})

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("HandleChannel hung when errorCh was nil")
	}
}

// TestHandleChannel_HappyPath_ForwardsError verifies the normal case still
// works: cbFunc returns error → errorCh receives it.
func TestHandleChannel_HappyPath_ForwardsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workCh := make(chan int, 1)
	workCh <- 1

	errorCh := make(chan error, 1) // buffered so we don't need a reader.
	doneCh := make(chan struct{})

	go HandleChannel(ctx, workCh, errorCh, doneCh, func(_ int) error {
		return errors.New("forced")
	})

	select {
	case err := <-errorCh:
		if err == nil || err.Error() != "forced" {
			t.Fatalf("expected forwarded error, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("error was not forwarded to errorCh")
	}
}

// TestHandleChannel_DoneChCancelsErrorSend verifies that if doneCh closes
// while we're trying to send an error, we exit cleanly (not deadlock).
func TestHandleChannel_DoneChCancelsErrorSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errorCh := make(chan error) // unbuffered, no reader.
	workCh := make(chan int, 1)
	workCh <- 1
	doneCh := make(chan struct{})

	workerDone := make(chan struct{})

	go func() {
		HandleChannel(ctx, workCh, errorCh, doneCh, func(_ int) error {
			return errors.New("forced")
		})

		close(workerDone)
	}()

	// Close doneCh mid-stuck-send.
	time.Sleep(50 * time.Millisecond) // let the worker get to the send.

	close(doneCh)

	select {
	case <-workerDone:
		// PASS
	case <-time.After(1 * time.Second):
		t.Fatal("HandleChannel did not exit on doneCh close while errorCh send was pending")
	}
}

// TestAsyncErrorSend_DoesNotBlockOnCtxCancel verifies the package-level
// helper extracted from BulkCreate's asyncErrorHandler: with an unbuffered
// errorCh and no reader, a cancelled ctx must unblock the send.
func TestAsyncErrorSend_DoesNotBlockOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	errorCh := make(chan error) // unbuffered, no reader.

	done := make(chan struct{})

	go func() {
		defer close(done)

		// With ctx already cancelled this must return false quickly.
		cancel()

		if asyncErrorSend(ctx, errorCh, errors.New("forced")) {
			t.Errorf("asyncErrorSend reported delivery when ctx was cancelled")
		}
	}()

	select {
	case <-done:
		// PASS
	case <-time.After(1 * time.Second):
		t.Fatal("asyncErrorSend did not return after ctx cancel with dead reader")
	}
}

// TestAsyncErrorSend_DeliversWhenReaderPresent verifies the happy path: a
// live reader receives the error and the helper reports delivered=true.
func TestAsyncErrorSend_DeliversWhenReaderPresent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errorCh := make(chan error, 1) // buffered so the send completes.

	delivered := asyncErrorSend(ctx, errorCh, errors.New("forced"))
	if !delivered {
		t.Fatal("asyncErrorSend reported non-delivery with a ready reader")
	}

	select {
	case err := <-errorCh:
		if err == nil || err.Error() != "forced" {
			t.Fatalf("expected forwarded error, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("error not received on errorCh")
	}
}

// TestAsyncErrorSend_NoPanicOnNilChannel verifies the nil guard — a naive
// `errorCh <- err` on a nil channel blocks forever in Go, so the guard is
// load-bearing.
func TestAsyncErrorSend_NoPanicOnNilChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		if asyncErrorSend(ctx, nil, errors.New("forced")) {
			t.Errorf("asyncErrorSend reported delivery on nil channel")
		}
	}()

	select {
	case <-done:
		// PASS
	case <-time.After(500 * time.Millisecond):
		t.Fatal("asyncErrorSend hung on nil channel")
	}
}
