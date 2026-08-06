package archive

import (
	"os"
	"testing"
	"time"
)

func TestRecorderDurationIncludesFinalVideoSample(t *testing.T) {
	recorder := newDurationTestRecorder(t)
	recorder.StartSession()

	if err := recorder.AddVideoSample(1000, 0, []byte{0x01}, true); err != nil {
		t.Fatalf("AddVideoSample(first) error = %v", err)
	}
	if err := recorder.AddVideoSample(2500, 0, []byte{0x02}, false); err != nil {
		t.Fatalf("AddVideoSample(second) error = %v", err)
	}

	// The second sample is still pending until Close and uses the preceding
	// sample's 1.5 second duration.
	recorder.Close()
	if got, want := recorder.Duration(), 3*time.Second; got != want {
		t.Fatalf("Duration() = %s, want %s", got, want)
	}
}

func TestRecorderDurationUsesContinuousTimelineAcrossSessions(t *testing.T) {
	recorder := newDurationTestRecorder(t)
	recorder.StartSession()
	if err := recorder.AddVideoSample(0, 0, []byte{0x01}, true); err != nil {
		t.Fatalf("AddVideoSample(first session first) error = %v", err)
	}
	if err := recorder.AddVideoSample(1000, 0, []byte{0x02}, false); err != nil {
		t.Fatalf("AddVideoSample(first session second) error = %v", err)
	}
	recorder.Flush()

	recorder.StartSession()
	if err := recorder.AddVideoSample(0, 0, []byte{0x03}, true); err != nil {
		t.Fatalf("AddVideoSample(second session first) error = %v", err)
	}
	if err := recorder.AddVideoSample(1000, 0, []byte{0x04}, false); err != nil {
		t.Fatalf("AddVideoSample(second session second) error = %v", err)
	}
	recorder.Close()

	// Reconnecting resumes immediately after the previous media timestamp;
	// wall-clock time between StartSession calls is not part of the duration.
	if got, want := recorder.Duration(), 3001*time.Millisecond; got != want {
		t.Fatalf("Duration() = %s, want %s", got, want)
	}
}

func TestRecorderDurationIgnoresAudioTrack(t *testing.T) {
	recorder := newDurationTestRecorder(t)
	recorder.StartSession()
	if err := recorder.AddVideoSample(0, 0, []byte{0x01}, true); err != nil {
		t.Fatalf("AddVideoSample(first) error = %v", err)
	}
	if err := recorder.AddVideoSample(1000, 0, []byte{0x02}, false); err != nil {
		t.Fatalf("AddVideoSample(second) error = %v", err)
	}
	recorder.Flush()
	videoDuration := recorder.Duration()

	recorder.audioID = 2
	recorder.audioTS = 48000
	recorder.audioState = trackState{
		timescale:     recorder.audioTS,
		defaultDurMS:  20,
		trackID:       recorder.audioID,
		sampleIsVideo: false,
	}
	if err := recorder.AddAudioSample(5000, []byte{0x03}); err != nil {
		t.Fatalf("AddAudioSample(first) error = %v", err)
	}
	if err := recorder.AddAudioSample(5020, []byte{0x04}); err != nil {
		t.Fatalf("AddAudioSample(second) error = %v", err)
	}
	if got := recorder.Duration(); got != videoDuration {
		t.Fatalf("Duration() after audio = %s, want unchanged %s", got, videoDuration)
	}
	recorder.Close()
}

func newDurationTestRecorder(t *testing.T) *Recorder {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "archive-duration-*.mp4")
	if err != nil {
		t.Fatalf("os.CreateTemp() error = %v", err)
	}
	recorder := &Recorder{
		file:               file,
		initWritten:        true,
		videoID:            1,
		videoTS:            90000,
		fragmentDurationMS: 10000,
	}
	recorder.videoState = trackState{
		timescale:     recorder.videoTS,
		defaultDurMS:  33,
		trackID:       recorder.videoID,
		sampleIsVideo: true,
	}
	return recorder
}
