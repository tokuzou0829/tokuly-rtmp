package archive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"tokuly-live-rtmp-server/pkg/inspect"
	"tokuly-live-rtmp-server/pkg/policy"
)

func TestFinalizeArchiveAndPreviewStatus(t *testing.T) {
	tests := []struct {
		name            string
		previewEnabled  bool
		convertFails    bool
		previewFails    bool
		wantStatus      bool
		wantPreviewCall bool
		wantManifest    bool
	}{
		{name: "archive and preview succeed", previewEnabled: true, wantStatus: true, wantPreviewCall: true, wantManifest: true},
		{name: "preview failure keeps archive successful", previewEnabled: true, previewFails: true, wantStatus: true, wantPreviewCall: true},
		{name: "archive conversion failure", previewEnabled: true, convertFails: true, wantStatus: false},
		{name: "preview disabled", previewEnabled: false, wantStatus: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			recordPath := filepath.Join(root, "archive.mp4")
			hlsDir := filepath.Join(root, "hls")
			if err := os.WriteFile(recordPath, []byte("archive"), 0644); err != nil {
				t.Fatal(err)
			}

			cfg := previewTestConfig()
			cfg.Enable = true
			cfg.PreviewEnable = tt.previewEnabled
			cfg.HLSSegmentDuration = 10 * time.Second
			notifier := &archiveStatusPolicy{}
			manager := NewManager(cfg, notifier, false)
			manager.states["stream"] = &ArchiveState{
				streamName: "stream",
				recordPath: recordPath,
				hlsDir:     hlsDir,
			}

			previewCalled := false
			manager.runCommand = func(name string, args ...string) ([]byte, error) {
				if name == cfg.FFprobePath {
					previewCalled = true
					if tt.previewFails {
						return []byte("probe failed"), errors.New("exit status 1")
					}
					return previewProbeJSON("3.0"), nil
				}
				if containsArg(args, "hls") {
					if tt.convertFails {
						return []byte("conversion failed"), errors.New("exit status 1")
					}
					playlistPath := args[len(args)-1]
					if err := os.MkdirAll(filepath.Dir(playlistPath), 0755); err != nil {
						return nil, err
					}
					return nil, os.WriteFile(playlistPath, []byte("#EXTM3U\n"), 0644)
				}
				outputPattern := args[len(args)-1]
				outputPath := strings.Replace(outputPattern, "%03d", "001", 1)
				return nil, os.WriteFile(outputPath, []byte("jpeg"), 0644)
			}

			manager.finalize("stream")

			status, calls := notifier.result()
			if calls != 1 || status != tt.wantStatus {
				t.Fatalf("archive notification = status %v calls %d, want status %v calls 1", status, calls, tt.wantStatus)
			}
			if previewCalled != tt.wantPreviewCall {
				t.Fatalf("preview called = %v, want %v", previewCalled, tt.wantPreviewCall)
			}
			_, manifestErr := os.Stat(filepath.Join(hlsDir, "video_preview", "manifest.json"))
			if tt.wantManifest && manifestErr != nil {
				t.Fatalf("preview manifest missing: %v", manifestErr)
			}
			if !tt.wantManifest && manifestErr == nil {
				t.Fatal("preview manifest exists unexpectedly")
			}
		})
	}
}

func TestFinalizeRemainsBusyWhileGeneratingPreview(t *testing.T) {
	root := t.TempDir()
	recordPath := filepath.Join(root, "archive.mp4")
	hlsDir := filepath.Join(root, "hls")
	if err := os.WriteFile(recordPath, []byte("archive"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := previewTestConfig()
	cfg.Enable = true
	cfg.PreviewSamplingMode = "sequential"
	manager := NewManager(cfg, &archiveStatusPolicy{}, false)
	manager.states["stream"] = &ArchiveState{streamName: "stream", recordPath: recordPath, hlsDir: hlsDir}

	previewStarted := make(chan struct{})
	releasePreview := make(chan struct{})
	manager.runCommand = func(name string, args ...string) ([]byte, error) {
		if name == cfg.FFprobePath {
			close(previewStarted)
			<-releasePreview
			return previewProbeJSON("3.0"), nil
		}
		if containsArg(args, "hls") {
			playlistPath := args[len(args)-1]
			if err := os.MkdirAll(filepath.Dir(playlistPath), 0755); err != nil {
				return nil, err
			}
			return nil, os.WriteFile(playlistPath, []byte("#EXTM3U\n"), 0644)
		}
		outputPath := strings.Replace(args[len(args)-1], "%03d", "001", 1)
		return nil, os.WriteFile(outputPath, []byte("jpeg"), 0644)
	}

	done := make(chan struct{})
	go func() {
		manager.finalize("stream")
		close(done)
	}()
	select {
	case <-previewStarted:
	case <-time.After(time.Second):
		t.Fatal("preview did not start")
	}
	if err := manager.CanPublish("stream"); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("CanPublish() = %v, want ErrArchiveBusy", err)
	}
	close(releasePreview)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("finalize did not complete")
	}
	if err := manager.CanPublish("stream"); err != nil {
		t.Fatalf("CanPublish() after finalize = %v, want nil", err)
	}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

type archiveStatusPolicy struct {
	mu     sync.Mutex
	status bool
	calls  int
}

func (p *archiveStatusPolicy) Authorize(context.Context, string, string, string, string) (policy.Result, error) {
	return policy.Result{Decision: policy.DecisionAccept}, nil
}

func (p *archiveStatusPolicy) Evaluate(context.Context, inspect.Result) policy.Result {
	return policy.Result{Decision: policy.DecisionAccept}
}

func (p *archiveStatusPolicy) NotifyStreamEnd(context.Context, string) error { return nil }

func (p *archiveStatusPolicy) NotifyVideoInfo(context.Context, string, inspect.Result) error {
	return nil
}

func (p *archiveStatusPolicy) NotifyArchiveStatus(_ context.Context, _ string, status bool, _ float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = status
	p.calls++
	return nil
}

func (p *archiveStatusPolicy) result() (bool, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status, p.calls
}
