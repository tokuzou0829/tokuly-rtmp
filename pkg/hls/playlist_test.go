package hls

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tokuly-live-rtmp-server/pkg/storage"
)

func TestClassicPlaylistContainsOnlyCompleteSegments(t *testing.T) {
	root := t.TempDir()
	p := New(Config{
		TargetDuration:      2 * time.Second,
		PartDuration:        200 * time.Millisecond,
		HoldBack:            6 * time.Second,
		PartHoldBack:        time.Second,
		EnablePartial:       true,
		InitFilename:        "init.mp4",
		PlaylistName:        "index.m3u8",
		ClassicPlaylistName: "index_classic.m3u8",
	}, storage.New(root, "", false), "stream")
	p.AddPart(42, "part_42_00.m4s", 200*time.Millisecond)
	p.FinalizeSegment(42, "seg_42.m4s", 2*time.Second)
	p.AddPart(43, "part_43_00.m4s", 200*time.Millisecond)

	classic := p.RenderClassic()
	for _, want := range []string{
		"#EXT-X-VERSION:7", "#EXT-X-MEDIA-SEQUENCE:42",
		"#EXT-X-MAP:URI=\"init.mp4\"", "#EXTINF:2.000,\nseg_42.m4s",
	} {
		if !strings.Contains(classic, want) {
			t.Errorf("classic playlist missing %q: %s", want, classic)
		}
	}
	for _, excluded := range []string{"#EXT-X-PART", "#EXT-X-SERVER-CONTROL", "seg_43.m4s"} {
		if strings.Contains(classic, excluded) {
			t.Errorf("classic playlist contains %q: %s", excluded, classic)
		}
	}
	if !strings.Contains(p.Render(), "part_43_00.m4s") {
		t.Fatal("low-latency playlist lost its partial segment")
	}
	if err := p.Write(); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join(root, "stream", "index_classic.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != classic {
		t.Errorf("written classic playlist does not match rendered content")
	}
}
