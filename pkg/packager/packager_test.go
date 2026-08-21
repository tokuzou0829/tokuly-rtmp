package packager

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"

	"tokuly-live-rtmp-server/pkg/storage"
)

func TestPackagerGeneratesOrderedSegmentsForFrameRates(t *testing.T) {
	tests := []struct {
		name string
		num  int64
		den  int64
	}{
		{name: "23.976", num: 24000, den: 1001},
		{name: "24", num: 24, den: 1},
		{name: "25", num: 25, den: 1},
		{name: "29.97", num: 30000, den: 1001},
		{name: "30", num: 30, den: 1},
		{name: "50", num: 50, den: 1},
		{name: "59.94", num: 60000, den: 1001},
		{name: "60", num: 60, den: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPackager(t, false)
			timestamps := frameTimestamps(2500, tt.num, tt.den)
			for i, tsMS := range timestamps {
				if err := p.AddVideoSample(tsMS, 0, []byte{byte(i)}, i == 0); err != nil {
					t.Fatalf("AddVideoSample(%d) error = %v", tsMS, err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if got := len(parseRenderedPlaylist(t, p.playlist.Render())); got < 2 {
				t.Fatalf("completed segment count = %d, want at least 2", got)
			}

			counts := verifyPackagerOutput(t, p)
			if got := counts[p.videoID]; got != len(timestamps) {
				t.Fatalf("video sample count = %d, want %d", got, len(timestamps))
			}
		})
	}
}

func TestPackagerReordersAudioLeadingOBSFrames(t *testing.T) {
	p := newTestPackager(t, true)

	type event struct {
		kind       string
		dtsMS      int64
		arrivalMS  int64
		inputOrder int
	}
	var events []event
	videoTimestamps := frameTimestamps(3200, 30, 1)
	for i, tsMS := range videoTimestamps {
		events = append(events, event{kind: "video", dtsMS: tsMS, arrivalMS: tsMS, inputOrder: i})
	}
	audioTimestamps := audioTimestamps(3200, 48000)
	for i, tsMS := range audioTimestamps {
		// OBS may deliver muxed audio ahead of video near a part boundary.
		events = append(events, event{kind: "audio", dtsMS: tsMS, arrivalMS: tsMS - 80, inputOrder: i})
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].arrivalMS == events[j].arrivalMS {
			return events[i].kind < events[j].kind
		}
		return events[i].arrivalMS < events[j].arrivalMS
	})

	for _, event := range events {
		var err error
		if event.kind == "video" {
			err = p.AddVideoSample(event.dtsMS, 0, []byte{byte(event.inputOrder)}, event.inputOrder == 0)
		} else {
			err = p.AddAudioSample(event.dtsMS, []byte{byte(event.inputOrder)})
		}
		if err != nil {
			t.Fatalf("Add%sSample(%d) error = %v", event.kind, event.dtsMS, err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	counts := verifyPackagerOutput(t, p)
	if got := counts[p.videoID]; got != len(videoTimestamps) {
		t.Fatalf("video sample count = %d, want %d", got, len(videoTimestamps))
	}
	if got := counts[p.audioID]; got != len(audioTimestamps) {
		t.Fatalf("audio sample count = %d, want %d", got, len(audioTimestamps))
	}
}

func TestPackagerGeneratesSegmentsAcrossFrameRateChanges(t *testing.T) {
	p := newTestPackager(t, false)
	timestamps := frameTimestamps(1000, 24, 1)
	last := timestamps[len(timestamps)-1]
	for last < 2100 {
		last += 17
		timestamps = append(timestamps, last)
	}
	for _, intervalMS := range []int64{24, 41, 33, 50, 17, 67, 20, 45, 29, 55} {
		last += intervalMS
		timestamps = append(timestamps, last)
	}

	for i, tsMS := range timestamps {
		if err := p.AddVideoSample(tsMS, 0, []byte{byte(i)}, i == 0); err != nil {
			t.Fatalf("AddVideoSample(%d) error = %v", tsMS, err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	counts := verifyPackagerOutput(t, p)
	if got := counts[p.videoID]; got != len(timestamps) {
		t.Fatalf("video sample count = %d, want %d", got, len(timestamps))
	}
}

func TestVideoSampleDurationsFollowVariableDTS(t *testing.T) {
	p := &Packager{videoID: 1, videoTS: 90000, partDurationMS: 200}
	state := trackState{
		timescale:     p.videoTS,
		trackID:       p.videoID,
		sampleIsVideo: true,
		defaultDurMS:  fallbackVideoSampleDurationMS,
	}
	timestamps := []int64{0, 42, 83, 133, 166, 207}
	wantDurationsMS := []int64{42, 41, 50, 33, 41}

	for i, tsMS := range timestamps {
		sample, err := p.ingestSample(&state, pendingSample{dtsMS: tsMS, data: []byte{byte(i)}})
		if err != nil {
			t.Fatalf("ingestSample(%d) error = %v", tsMS, err)
		}
		if i == 0 {
			if sample != nil {
				t.Fatal("first sample was emitted before its duration was known")
			}
			continue
		}
		if sample == nil {
			t.Fatalf("sample %d was not emitted", i-1)
		}
		want := msToTimescale(wantDurationsMS[i-1], p.videoTS)
		if sample.sample.Dur != want {
			t.Fatalf("sample %d duration = %d, want %d", i-1, sample.sample.Dur, want)
		}
	}
}

func TestDrainReorderQueueDropsSampleOlderThanCommittedTimeline(t *testing.T) {
	p := newTestPackager(t, false)
	p.hasLastOutput = true
	p.lastOutputDTSMS = 500
	p.enqueueSample(trackSample{
		trackID: p.videoID,
		dtsMS:   400,
		sample:  buildFullSample(&p.videoState, pendingSample{dtsMS: 400, data: []byte{0x01}}, 40),
	})

	if err := p.drainReorderQueue(true); err != nil {
		t.Fatalf("drainReorderQueue() error = %v", err)
	}
	if len(p.reorderQueue) != 0 {
		t.Fatalf("reorder queue length = %d, want 0", len(p.reorderQueue))
	}
	if p.currentPart != nil {
		t.Fatal("late sample was appended to a finalized timeline")
	}
}

func TestSingleVideoFrameUsesFallbackDuration(t *testing.T) {
	p := newTestPackager(t, false)
	if err := p.AddVideoSample(0, 0, []byte{0x01}, true); err != nil {
		t.Fatalf("AddVideoSample() error = %v", err)
	}
	if err := p.flushTrack(&p.videoState); err != nil {
		t.Fatalf("flushTrack() error = %v", err)
	}
	if got, want := p.reorderQueue[0].sample.Dur, msToTimescale(fallbackVideoSampleDurationMS, p.videoTS); got != want {
		t.Fatalf("fallback duration = %d, want %d", got, want)
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	counts := verifyPackagerOutput(t, p)
	if got := counts[p.videoID]; got != 1 {
		t.Fatalf("video sample count = %d, want 1", got)
	}
}

func newTestPackager(t *testing.T, withAudio bool) *Packager {
	t.Helper()
	root := t.TempDir()
	st := storage.New(root, filepath.Join(root, "rewind"), false)
	p := New(Config{
		SegmentDuration:     time.Second,
		PartDuration:        200 * time.Millisecond,
		PlaylistWindow:      10 * time.Second,
		TargetDuration:      time.Second,
		HoldBack:            3 * time.Second,
		PartHoldBack:        time.Second,
		KeepSegments:        20,
		InitFilename:        "init.mp4",
		SegmentFilenameTmpl: "seg_%06d.m4s",
		PartFilenameTmpl:    "part_%06d_%02d.m4s",
		PlaylistName:        "index.m3u8",
		EnablePartial:       true,
	}, st, "test-stream")
	p.initWritten = true
	p.videoID = 1
	p.videoTS = 90000
	p.videoState = trackState{
		timescale:     p.videoTS,
		trackID:       p.videoID,
		sampleIsVideo: true,
		defaultDurMS:  fallbackVideoSampleDurationMS,
	}
	if withAudio {
		p.audioID = 2
		p.audioTS = 48000
		p.aacConfig.SampleRate = 48000
		p.audioState = trackState{
			timescale:    p.audioTS,
			trackID:      p.audioID,
			defaultDurMS: 21,
		}
	}
	return p
}

func verifyPackagerOutput(t *testing.T, p *Packager) map[uint32]int {
	t.Helper()
	segments := parseRenderedPlaylist(t, p.playlist.Render())
	if len(segments) == 0 {
		t.Fatal("no completed segments")
	}

	seenURIs := make(map[string]bool)
	lastEnd := make(map[uint32]uint64)
	hasLastEnd := make(map[uint32]bool)
	counts := make(map[uint32]int)
	for _, segment := range segments {
		lastPartIndex := -1
		for _, partURI := range segment.parts {
			if seenURIs[partURI] {
				t.Fatalf("duplicate part URI %q", partURI)
			}
			seenURIs[partURI] = true
			var seq uint64
			var partIndex int
			if _, err := fmt.Sscanf(partURI, "part_%06d_%02d.m4s", &seq, &partIndex); err != nil {
				t.Fatalf("parse part URI %q: %v", partURI, err)
			}
			if seq != segment.seq {
				t.Fatalf("part %q belongs to segment %d", partURI, segment.seq)
			}
			if partIndex <= lastPartIndex {
				t.Fatalf("part index moved backward in segment %d: %d after %d", segment.seq, partIndex, lastPartIndex)
			}
			lastPartIndex = partIndex

			path := filepath.Join(p.storage.StreamDir(p.streamID), partURI)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("open %s: %v", partURI, err)
			}
			decoded, decodeErr := mp4.DecodeFile(file, mp4.WithDecodeFlags(mp4.DecStartOnMoof))
			closeErr := file.Close()
			if decodeErr != nil {
				t.Fatalf("decode %s: %v", partURI, decodeErr)
			}
			if closeErr != nil {
				t.Fatalf("close %s: %v", partURI, closeErr)
			}
			for _, mediaSegment := range decoded.Segments {
				for _, fragment := range mediaSegment.Fragments {
					for _, traf := range fragment.Moof.Trafs {
						trackID := traf.Tfhd.TrackID
						decodeTime := traf.Tfdt.BaseMediaDecodeTime()
						if hasLastEnd[trackID] && decodeTime != lastEnd[trackID] {
							t.Fatalf("track %d decode timeline discontinuity in %s: start=%d want=%d",
								trackID, partURI, decodeTime, lastEnd[trackID])
						}
						for _, trun := range traf.Truns {
							for _, sample := range trun.Samples {
								if sample.Dur == 0 {
									t.Fatalf("track %d has zero-duration sample in %s", trackID, partURI)
								}
								decodeTime += uint64(sample.Dur)
								counts[trackID]++
							}
						}
						lastEnd[trackID] = decodeTime
						hasLastEnd[trackID] = true
					}
				}
			}
		}
	}
	if len(seenURIs) == 0 {
		t.Fatal("no HLS parts were generated")
	}
	return counts
}

type renderedSegment struct {
	seq   uint64
	parts []string
}

func parseRenderedPlaylist(t *testing.T, playlist string) []renderedSegment {
	t.Helper()
	lines := strings.Split(playlist, "\n")
	var segments []renderedSegment
	var parts []string
	var seq uint64
	hasMediaSequence := false
	waitingForSegmentURI := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			value := strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:")
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				t.Fatalf("parse media sequence %q: %v", value, err)
			}
			seq = parsed
			hasMediaSequence = true
		case strings.HasPrefix(line, "#EXT-X-PART:"):
			marker := `URI="`
			start := strings.Index(line, marker)
			if start < 0 {
				t.Fatalf("part line has no URI: %q", line)
			}
			uri := line[start+len(marker):]
			end := strings.IndexByte(uri, '"')
			if end < 0 {
				t.Fatalf("part URI is unterminated: %q", line)
			}
			parts = append(parts, uri[:end])
		case strings.HasPrefix(line, "#EXTINF:"):
			waitingForSegmentURI = true
		case waitingForSegmentURI && line != "" && !strings.HasPrefix(line, "#"):
			if !hasMediaSequence {
				t.Fatal("playlist has a segment before its media sequence")
			}
			segments = append(segments, renderedSegment{seq: seq, parts: append([]string(nil), parts...)})
			seq++
			parts = nil
			waitingForSegmentURI = false
		}
	}
	if waitingForSegmentURI || len(parts) > 0 {
		t.Fatal("playlist is incomplete after Flush")
	}
	return segments
}

func frameTimestamps(endMS, num, den int64) []int64 {
	var timestamps []int64
	for frame := int64(0); ; frame++ {
		tsMS := (frame*1000*den + num/2) / num
		if tsMS > endMS {
			return timestamps
		}
		timestamps = append(timestamps, tsMS)
	}
}

func audioTimestamps(endMS, sampleRate int64) []int64 {
	var timestamps []int64
	for sample := int64(0); ; sample++ {
		tsMS := (sample*1024*1000 + sampleRate/2) / sampleRate
		if tsMS > endMS {
			return timestamps
		}
		timestamps = append(timestamps, tsMS)
	}
}
