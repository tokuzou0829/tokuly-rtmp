package config

import (
	"testing"
	"time"
)

func TestDefaultArchivePreviewConfig(t *testing.T) {
	cfg := DefaultConfig().Archive
	if !cfg.PreviewEnable || cfg.FFprobePath != "/opt/homebrew/bin/ffprobe" {
		t.Fatalf("default preview enable/path = %v/%q", cfg.PreviewEnable, cfg.FFprobePath)
	}
	if cfg.PreviewIntervalMode != "smooth" || cfg.PreviewSamplingMode != "auto" {
		t.Fatalf("default preview modes = %q/%q", cfg.PreviewIntervalMode, cfg.PreviewSamplingMode)
	}
	if cfg.PreviewMaxFrames != 320 || cfg.PreviewMinInterval != 5*time.Second {
		t.Fatalf("default preview limits = %d/%s", cfg.PreviewMaxFrames, cfg.PreviewMinInterval)
	}
	if cfg.PreviewSeekJobs != 4 || cfg.PreviewSeekThreshold != 15*time.Second {
		t.Fatalf("default preview seek settings = %d/%s", cfg.PreviewSeekJobs, cfg.PreviewSeekThreshold)
	}
}

func TestLoadArchivePreviewConfig(t *testing.T) {
	t.Setenv("ARCHIVE_PREVIEW_ENABLE", "false")
	t.Setenv("ARCHIVE_FFPROBE_PATH", "/usr/local/bin/ffprobe")
	t.Setenv("ARCHIVE_PREVIEW_INTERVAL_MODE", "legacy")
	t.Setenv("ARCHIVE_PREVIEW_MAX_FRAMES", "100")
	t.Setenv("ARCHIVE_PREVIEW_MIN_INTERVAL", "7s")
	t.Setenv("ARCHIVE_PREVIEW_SAMPLING_MODE", "seek")
	t.Setenv("ARCHIVE_PREVIEW_SEEK_JOBS", "8")
	t.Setenv("ARCHIVE_PREVIEW_SEEK_INTERVAL_THRESHOLD", "20s")

	cfg := Load().Archive
	if cfg.PreviewEnable || cfg.FFprobePath != "/usr/local/bin/ffprobe" {
		t.Fatalf("loaded preview enable/path = %v/%q", cfg.PreviewEnable, cfg.FFprobePath)
	}
	if cfg.PreviewIntervalMode != "legacy" || cfg.PreviewSamplingMode != "seek" {
		t.Fatalf("loaded preview modes = %q/%q", cfg.PreviewIntervalMode, cfg.PreviewSamplingMode)
	}
	if cfg.PreviewMaxFrames != 100 || cfg.PreviewMinInterval != 7*time.Second {
		t.Fatalf("loaded preview limits = %d/%s", cfg.PreviewMaxFrames, cfg.PreviewMinInterval)
	}
	if cfg.PreviewSeekJobs != 8 || cfg.PreviewSeekThreshold != 20*time.Second {
		t.Fatalf("loaded preview seek settings = %d/%s", cfg.PreviewSeekJobs, cfg.PreviewSeekThreshold)
	}
}
