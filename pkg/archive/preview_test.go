package archive

import (
	"encoding/json"
	"errors"
	"fmt"
	"image/jpeg"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"tokuly-live-rtmp-server/pkg/config"
)

func TestCalculatePreviewInterval(t *testing.T) {
	cfg := previewTestConfig()
	tests := []struct {
		name     string
		duration float64
		want     float64
	}{
		{name: "five minutes", duration: 300, want: 1},
		{name: "ten minutes", duration: 600, want: 5},
		{name: "twenty minutes", duration: 1200, want: 10},
		{name: "three hours", duration: 10800, want: 10},
		{name: "four hours", duration: 14400, want: 20},
		{name: "six hours", duration: 21600, want: 20},
		{name: "eight hours", duration: 28800, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculatePreviewInterval(tt.duration, cfg); got != tt.want {
				t.Fatalf("calculatePreviewInterval(%v) = %v, want %v", tt.duration, got, tt.want)
			}
		})
	}

	cfg.PreviewIntervalMode = "legacy"
	cfg.PreviewMaxFrames = 320
	cfg.PreviewMinInterval = 5 * time.Second
	if got, want := calculatePreviewInterval(60, cfg), float64(5); got != want {
		t.Fatalf("legacy short interval = %v, want %v", got, want)
	}
	if got, want := calculatePreviewInterval(3201, cfg), float64(11); got != want {
		t.Fatalf("legacy rounded interval = %v, want %v", got, want)
	}
}

func TestSelectPreviewSamplingMode(t *testing.T) {
	cfg := previewTestConfig()
	cfg.PreviewSeekThreshold = 15 * time.Second
	tests := []struct {
		name     string
		input    string
		interval float64
		mode     string
		want     string
	}{
		{name: "local above threshold", input: "/archive/index.m3u8", interval: 15, mode: "auto", want: "seek"},
		{name: "local below threshold", input: "/archive/index.m3u8", interval: 14.999, mode: "auto", want: "sequential"},
		{name: "remote above threshold", input: "https://example.com/index.m3u8", interval: 20, mode: "auto", want: "sequential"},
		{name: "forced seek", input: "https://example.com/index.m3u8", interval: 1, mode: "seek", want: "seek"},
		{name: "forced sequential", input: "/archive/index.m3u8", interval: 20, mode: "sequential", want: "sequential"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg.PreviewSamplingMode = tt.mode
			if got := selectPreviewSamplingMode(tt.input, tt.interval, cfg); got != tt.want {
				t.Fatalf("selectPreviewSamplingMode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGenerateVideoPreviewPublishesManifestAndSprites(t *testing.T) {
	cfg := previewTestConfig()
	cfg.PreviewSamplingMode = "sequential"
	root := t.TempDir()
	input := filepath.Join(root, "index.m3u8")
	outputDir := filepath.Join(root, "video_preview")
	if err := os.WriteFile(input, []byte("#EXTM3U\n"), 0644); err != nil {
		t.Fatal(err)
	}

	runner := func(name string, args ...string) ([]byte, error) {
		if name == cfg.FFprobePath {
			return previewProbeJSON("26.0"), nil
		}
		outputPattern := args[len(args)-1]
		for i := 1; i <= 2; i++ {
			path := strings.Replace(outputPattern, "%03d", fmt.Sprintf("%03d", i), 1)
			if err := os.WriteFile(path, []byte("jpeg"), 0644); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	if err := generateVideoPreview(cfg, input, outputDir, runner); err != nil {
		t.Fatalf("generateVideoPreview() error = %v", err)
	}
	manifestData, err := os.ReadFile(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest previewManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.FrameCount != 26 || manifest.IntervalSeconds != 1 {
		t.Fatalf("manifest plan = frames %d interval %v, want 26 and 1", manifest.FrameCount, manifest.IntervalSeconds)
	}
	if got, want := manifest.Sprites, []string{"video_preview_001.jpg", "video_preview_002.jpg"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("manifest sprites = %v, want %v", got, want)
	}
	for _, sprite := range manifest.Sprites {
		if _, err := os.Stat(filepath.Join(outputDir, sprite)); err != nil {
			t.Fatalf("published sprite %q: %v", sprite, err)
		}
	}
}

func TestGenerateVideoPreviewSeekHonorsParallelLimit(t *testing.T) {
	cfg := previewTestConfig()
	cfg.PreviewSamplingMode = "seek"
	cfg.PreviewSeekJobs = 2
	root := t.TempDir()
	outputDir := filepath.Join(root, "video_preview")

	var mu sync.Mutex
	active := 0
	maxActive := 0
	runner := func(name string, args ...string) ([]byte, error) {
		if name == cfg.FFprobePath {
			return previewProbeJSON("3.0"), nil
		}
		outputPath := args[len(args)-1]
		if strings.Contains(filepath.Base(outputPath), "frame_") {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			if err := os.WriteFile(outputPath, []byte("bmp"), 0644); err != nil {
				return nil, err
			}
			mu.Lock()
			active--
			mu.Unlock()
			return nil, nil
		}
		return nil, os.WriteFile(outputPath, []byte("jpeg"), 0644)
	}

	if err := generateVideoPreview(cfg, filepath.Join(root, "index.m3u8"), outputDir, runner); err != nil {
		t.Fatalf("generateVideoPreview() error = %v", err)
	}
	if maxActive != 2 {
		t.Fatalf("maximum parallel seek jobs = %d, want 2", maxActive)
	}
}

func TestGenerateVideoPreviewFailureRemovesOutput(t *testing.T) {
	cfg := previewTestConfig()
	cfg.PreviewSamplingMode = "sequential"
	root := t.TempDir()
	outputDir := filepath.Join(root, "video_preview")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "partial.jpg"), []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}
	runner := func(name string, args ...string) ([]byte, error) {
		if name == cfg.FFprobePath {
			return previewProbeJSON("3.0"), nil
		}
		return []byte("encoder failed"), errors.New("exit status 1")
	}

	if err := generateVideoPreview(cfg, filepath.Join(root, "index.m3u8"), outputDir, runner); err == nil {
		t.Fatal("generateVideoPreview() error = nil, want failure")
	}
	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		t.Fatalf("preview output remains after failure: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".video-preview-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary preview directories remain: %v", matches)
	}
}

func TestGenerateVideoPreviewRejectsInvalidProbeData(t *testing.T) {
	cfg := previewTestConfig()
	runner := func(name string, args ...string) ([]byte, error) {
		return previewProbeJSON("N/A"), nil
	}
	outputDir := filepath.Join(t.TempDir(), "video_preview")
	if err := generateVideoPreview(cfg, "index.m3u8", outputDir, runner); err == nil {
		t.Fatal("generateVideoPreview() error = nil, want invalid duration error")
	}
	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		t.Fatalf("preview output exists after probe failure: %v", err)
	}
}

func TestGenerateVideoPreviewWithFFmpeg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ffmpeg integration test in short mode")
	}
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	ffprobePath, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe is not installed")
	}

	root := t.TempDir()
	input := filepath.Join(root, "index.m3u8")
	segmentPattern := filepath.Join(root, "segment_%03d.ts")
	cmd := exec.Command(
		ffmpegPath,
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=320x180:rate=10",
		"-t", "3",
		"-c:v", "libx264", "-pix_fmt", "yuv420p",
		"-f", "hls", "-hls_time", "1", "-hls_list_size", "0",
		"-hls_segment_filename", segmentPattern,
		input,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("could not create ffmpeg test fixture: %v output=%s", err, output)
	}

	cfg := previewTestConfig()
	cfg.FFmpegPath = ffmpegPath
	cfg.FFprobePath = ffprobePath
	cfg.PreviewSamplingMode = "sequential"
	outputDir := filepath.Join(root, "video_preview")
	if err := generateVideoPreview(cfg, input, outputDir, nil); err != nil {
		t.Fatalf("generateVideoPreview() error = %v", err)
	}

	file, err := os.Open(filepath.Join(outputDir, "video_preview_001.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	imageConfig, err := jpeg.DecodeConfig(file)
	if err != nil {
		t.Fatalf("decode preview JPEG: %v", err)
	}
	if imageConfig.Width != previewTileWidth*previewColumns || imageConfig.Height != previewTileHeight*previewRows {
		t.Fatalf("preview sprite size = %dx%d, want %dx%d", imageConfig.Width, imageConfig.Height, previewTileWidth*previewColumns, previewTileHeight*previewRows)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "manifest.json")); err != nil {
		t.Fatalf("preview manifest missing: %v", err)
	}

	cfg.PreviewSamplingMode = "seek"
	seekOutputDir := filepath.Join(root, "video_preview_seek")
	if err := generateVideoPreview(cfg, input, seekOutputDir, nil); err != nil {
		t.Fatalf("generateVideoPreview(seek) error = %v", err)
	}
	temporaryFrames, err := filepath.Glob(filepath.Join(seekOutputDir, "frame_*.bmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporaryFrames) != 0 {
		t.Fatalf("seek output contains temporary frames: %v", temporaryFrames)
	}
	if _, err := os.Stat(filepath.Join(seekOutputDir, "manifest.json")); err != nil {
		t.Fatalf("seek preview manifest missing: %v", err)
	}
}

func previewTestConfig() config.ArchiveConfig {
	return config.ArchiveConfig{
		FFmpegPath:           "ffmpeg-test",
		FFprobePath:          "ffprobe-test",
		PreviewEnable:        true,
		PreviewIntervalMode:  "smooth",
		PreviewMaxFrames:     320,
		PreviewMinInterval:   5 * time.Second,
		PreviewSamplingMode:  "auto",
		PreviewSeekJobs:      4,
		PreviewSeekThreshold: 15 * time.Second,
	}
}

func previewProbeJSON(duration string) []byte {
	return []byte(fmt.Sprintf(`{"streams":[{"codec_name":"h264","width":1920,"height":1080,"r_frame_rate":"30/1"}],"format":{"duration":%q}}`, duration))
}
