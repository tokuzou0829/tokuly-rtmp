package archive

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"tokuly-live-rtmp-server/pkg/config"
)

const (
	previewColumns        = 5
	previewRows           = 5
	previewTileWidth      = 160
	previewTileHeight     = 90
	previewTilesPerSprite = previewColumns * previewRows
)

type previewCommandRunner func(name string, args ...string) ([]byte, error)

type previewProbe struct {
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
	Streams []struct {
		CodecName string `json:"codec_name"`
		Width     int    `json:"width"`
		Height    int    `json:"height"`
		FrameRate string `json:"r_frame_rate"`
	} `json:"streams"`
}

type previewManifest struct {
	Version         int      `json:"version"`
	IntervalSeconds float64  `json:"intervalSeconds"`
	FrameCount      int      `json:"frameCount"`
	TileWidth       int      `json:"tileWidth"`
	TileHeight      int      `json:"tileHeight"`
	Columns         int      `json:"columns"`
	Rows            int      `json:"rows"`
	Sprites         []string `json:"sprites"`
}

func defaultPreviewCommandRunner(name string, args ...string) ([]byte, error) {
	return exec.Command(name, args...).CombinedOutput()
}

func generateVideoPreview(cfg config.ArchiveConfig, input, outputDir string, runner previewCommandRunner) error {
	if err := validatePreviewConfig(cfg); err != nil {
		return err
	}
	if input == "" {
		return fmt.Errorf("preview input empty")
	}
	cleanOutput := filepath.Clean(outputDir)
	if outputDir == "" || cleanOutput == "." || cleanOutput == string(filepath.Separator) {
		return fmt.Errorf("preview output dir invalid")
	}
	if runner == nil {
		runner = defaultPreviewCommandRunner
	}
	parentDir := filepath.Dir(cleanOutput)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return err
	}
	if err := os.RemoveAll(cleanOutput); err != nil {
		return err
	}

	probe, duration, err := probePreviewInput(cfg.FFprobePath, input, runner)
	if err != nil {
		return err
	}
	interval := calculatePreviewInterval(duration, cfg)
	if interval <= 0 || math.IsNaN(interval) || math.IsInf(interval, 0) {
		return fmt.Errorf("preview interval invalid: %v", interval)
	}
	frameCount := int(math.Ceil(duration / interval))
	if frameCount <= 0 {
		return fmt.Errorf("preview frame count invalid: %d", frameCount)
	}
	spriteCount := (frameCount + previewTilesPerSprite - 1) / previewTilesPerSprite
	mode := selectPreviewSamplingMode(input, interval, cfg)

	streamDescription := "video stream unavailable"
	if len(probe.Streams) > 0 {
		stream := probe.Streams[0]
		streamDescription = fmt.Sprintf("codec=%s size=%dx%d frameRate=%s", stream.CodecName, stream.Width, stream.Height, stream.FrameRate)
	}
	log.Printf("archive preview started: input=%s duration=%.3fs interval=%.3fs frames=%d sprites=%d mode=%s %s", input, duration, interval, frameCount, spriteCount, mode, streamDescription)

	tmpDir, err := os.MkdirTemp(parentDir, ".video-preview-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		_ = os.RemoveAll(tmpDir)
		if !published {
			_ = os.RemoveAll(cleanOutput)
		}
	}()

	if mode == "seek" {
		err = generateSeekPreview(cfg, input, tmpDir, interval, frameCount, spriteCount, runner)
	} else {
		err = generateSequentialPreview(cfg, input, tmpDir, interval, runner)
	}
	if err != nil {
		return err
	}
	temporaryFrames, err := filepath.Glob(filepath.Join(tmpDir, "frame_*.bmp"))
	if err != nil {
		return err
	}
	for _, frame := range temporaryFrames {
		if err := os.Remove(frame); err != nil {
			return fmt.Errorf("remove temporary preview frame: %w", err)
		}
	}

	sprites := make([]string, spriteCount)
	for i := 1; i <= spriteCount; i++ {
		name := previewSpriteName(i)
		info, statErr := os.Stat(filepath.Join(tmpDir, name))
		if statErr != nil {
			return fmt.Errorf("preview sprite missing: %s: %w", name, statErr)
		}
		if info.Size() == 0 {
			return fmt.Errorf("preview sprite empty: %s", name)
		}
		sprites[i-1] = name
	}
	actualSprites, err := filepath.Glob(filepath.Join(tmpDir, "video_preview_*.jpg"))
	if err != nil {
		return err
	}
	if len(actualSprites) != spriteCount {
		return fmt.Errorf("unexpected preview sprite count: expected=%d actual=%d", spriteCount, len(actualSprites))
	}

	manifest := previewManifest{
		Version:         1,
		IntervalSeconds: interval,
		FrameCount:      frameCount,
		TileWidth:       previewTileWidth,
		TileHeight:      previewTileHeight,
		Columns:         previewColumns,
		Rows:            previewRows,
		Sprites:         sprites,
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	manifestData = append(manifestData, '\n')
	if err := os.WriteFile(filepath.Join(tmpDir, "manifest.json"), manifestData, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpDir, cleanOutput); err != nil {
		return err
	}
	published = true
	log.Printf("archive preview completed: output=%s frames=%d sprites=%d mode=%s", cleanOutput, frameCount, spriteCount, mode)
	return nil
}

func validatePreviewConfig(cfg config.ArchiveConfig) error {
	if cfg.FFmpegPath == "" {
		return fmt.Errorf("archive ffmpeg path empty")
	}
	if cfg.FFprobePath == "" {
		return fmt.Errorf("archive ffprobe path empty")
	}
	if cfg.PreviewIntervalMode != "smooth" && cfg.PreviewIntervalMode != "legacy" {
		return fmt.Errorf("preview interval mode must be smooth or legacy")
	}
	if cfg.PreviewSamplingMode != "auto" && cfg.PreviewSamplingMode != "sequential" && cfg.PreviewSamplingMode != "seek" {
		return fmt.Errorf("preview sampling mode must be auto, sequential or seek")
	}
	if cfg.PreviewMaxFrames <= 0 {
		return fmt.Errorf("preview max frames must be greater than zero")
	}
	if cfg.PreviewMinInterval <= 0 {
		return fmt.Errorf("preview minimum interval must be greater than zero")
	}
	if cfg.PreviewSeekJobs <= 0 {
		return fmt.Errorf("preview seek jobs must be greater than zero")
	}
	if cfg.PreviewSeekThreshold <= 0 {
		return fmt.Errorf("preview seek threshold must be greater than zero")
	}
	return nil
}

func probePreviewInput(ffprobePath, input string, runner previewCommandRunner) (previewProbe, float64, error) {
	args := []string{
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "format=duration:stream=codec_name,width,height,r_frame_rate",
		"-of", "json",
		input,
	}
	output, err := runner(ffprobePath, args...)
	if err != nil {
		return previewProbe{}, 0, commandError("ffprobe", err, output)
	}
	var probe previewProbe
	if err := json.Unmarshal(output, &probe); err != nil {
		return previewProbe{}, 0, fmt.Errorf("parse ffprobe output: %w", err)
	}
	duration, err := strconv.ParseFloat(probe.Format.Duration, 64)
	if err != nil || duration <= 0 || math.IsNaN(duration) || math.IsInf(duration, 0) {
		return previewProbe{}, 0, fmt.Errorf("could not determine a finite preview duration: %q", probe.Format.Duration)
	}
	if len(probe.Streams) == 0 {
		return previewProbe{}, 0, fmt.Errorf("preview input has no video stream")
	}
	return probe, duration, nil
}

func calculatePreviewInterval(duration float64, cfg config.ArchiveConfig) float64 {
	if cfg.PreviewIntervalMode == "legacy" {
		interval := math.Ceil(duration / float64(cfg.PreviewMaxFrames))
		minimum := cfg.PreviewMinInterval.Seconds()
		if interval < minimum {
			interval = minimum
		}
		return interval
	}
	interval := 1 +
		4*smoothstep((duration-300)/300) +
		5*smoothstep((duration-600)/600) +
		10*smoothstep((duration-10800)/3600) +
		30*smoothstep((duration-21600)/7200)
	return math.Round(interval*1000) / 1000
}

func smoothstep(value float64) float64 {
	if value <= 0 {
		return 0
	}
	if value >= 1 {
		return 1
	}
	return value * value * (3 - 2*value)
}

func selectPreviewSamplingMode(input string, interval float64, cfg config.ArchiveConfig) string {
	if cfg.PreviewSamplingMode != "auto" {
		return cfg.PreviewSamplingMode
	}
	if !strings.Contains(input, "://") && interval >= cfg.PreviewSeekThreshold.Seconds() {
		return "seek"
	}
	return "sequential"
}

func generateSequentialPreview(cfg config.ArchiveConfig, input, tmpDir string, interval float64, runner previewCommandRunner) error {
	filter := fmt.Sprintf(
		"setpts=PTS-STARTPTS,fps=1/%s:start_time=0:round=down:eof_action=pass,scale=%d:%d:force_original_aspect_ratio=decrease:flags=fast_bilinear,pad=%d:%d:(ow-iw)/2:(oh-ih)/2:black,setsar=1,tile=%dx%d:nb_frames=%d:padding=0:margin=0",
		formatPreviewSeconds(interval), previewTileWidth, previewTileHeight, previewTileWidth, previewTileHeight, previewColumns, previewRows, previewTilesPerSprite,
	)
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y",
		"-i", input,
		"-vf", filter,
		"-fps_mode", "passthrough",
		"-pix_fmt", "yuv420p",
		"-q:v", "3",
		filepath.Join(tmpDir, "video_preview_%03d.jpg"),
	}
	output, err := runner(cfg.FFmpegPath, args...)
	if err != nil {
		return commandError("ffmpeg sequential preview", err, output)
	}
	return nil
}

func generateSeekPreview(cfg config.ArchiveConfig, input, tmpDir string, interval float64, frameCount, spriteCount int, runner previewCommandRunner) error {
	for batchStart := 1; batchStart <= frameCount; batchStart += cfg.PreviewSeekJobs {
		batchEnd := batchStart + cfg.PreviewSeekJobs - 1
		if batchEnd > frameCount {
			batchEnd = frameCount
		}
		errs := make([]error, batchEnd-batchStart+1)
		var wg sync.WaitGroup
		for frameIndex := batchStart; frameIndex <= batchEnd; frameIndex++ {
			frameIndex := frameIndex
			wg.Add(1)
			go func() {
				defer wg.Done()
				seekTime := float64(frameIndex-1) * interval
				filter := fmt.Sprintf(
					"scale=%d:%d:force_original_aspect_ratio=decrease:flags=fast_bilinear,pad=%d:%d:(ow-iw)/2:(oh-ih)/2:black,setsar=1",
					previewTileWidth, previewTileHeight, previewTileWidth, previewTileHeight,
				)
				args := []string{
					"-hide_banner", "-loglevel", "error", "-y",
					"-ss", formatPreviewSeconds(seekTime),
					"-i", input,
					"-map", "0:v:0",
					"-frames:v", "1",
					"-vf", filter,
					"-update", "1",
					filepath.Join(tmpDir, fmt.Sprintf("frame_%06d.bmp", frameIndex)),
				}
				output, err := runner(cfg.FFmpegPath, args...)
				if err != nil {
					errs[frameIndex-batchStart] = commandError(fmt.Sprintf("ffmpeg preview seek frame %d", frameIndex), err, output)
				}
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}

	frames, err := filepath.Glob(filepath.Join(tmpDir, "frame_*.bmp"))
	if err != nil {
		return err
	}
	if len(frames) != frameCount {
		return fmt.Errorf("unexpected extracted preview frame count: expected=%d actual=%d", frameCount, len(frames))
	}

	startNumber := 1
	for spriteIndex := 1; spriteIndex <= spriteCount; spriteIndex++ {
		batch := previewTilesPerSprite
		remaining := frameCount - startNumber + 1
		if remaining < batch {
			batch = remaining
		}
		filter := fmt.Sprintf("tile=%dx%d:nb_frames=%d:padding=0:margin=0", previewColumns, previewRows, batch)
		args := []string{
			"-hide_banner", "-loglevel", "error", "-y",
			"-framerate", "1",
			"-start_number", strconv.Itoa(startNumber),
			"-i", filepath.Join(tmpDir, "frame_%06d.bmp"),
			"-vf", filter,
			"-frames:v", "1",
			"-update", "1",
			"-pix_fmt", "yuv420p",
			"-q:v", "3",
			filepath.Join(tmpDir, previewSpriteName(spriteIndex)),
		}
		output, err := runner(cfg.FFmpegPath, args...)
		if err != nil {
			return commandError(fmt.Sprintf("ffmpeg preview sprite %d", spriteIndex), err, output)
		}
		startNumber += batch
	}
	return nil
}

func previewSpriteName(index int) string {
	return fmt.Sprintf("video_preview_%03d.jpg", index)
}

func formatPreviewSeconds(seconds float64) string {
	return strconv.FormatFloat(seconds, 'f', 3, 64)
}

func commandError(operation string, err error, output []byte) error {
	message := strings.TrimSpace(string(output))
	if len(message) > 4000 {
		message = message[len(message)-4000:]
	}
	if message == "" {
		return fmt.Errorf("%s failed: %w", operation, err)
	}
	return fmt.Errorf("%s failed: %w output=%s", operation, err, message)
}
