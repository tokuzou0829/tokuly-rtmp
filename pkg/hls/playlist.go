package hls

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"tokuly-live-rtmp-server/pkg/storage"
)

type Config struct {
	SegmentDuration time.Duration
	PartDuration    time.Duration
	PlaylistWindow  time.Duration
	TargetDuration  time.Duration
	HoldBack        time.Duration
	PartHoldBack    time.Duration
	KeepSegments    int
	EnablePartial   bool
	InitFilename    string
	PlaylistName    string
}

type Part struct {
	URI      string
	Duration float64
}

type Segment struct {
	Seq            uint64
	URI            string
	Duration       float64
	Parts          []Part
	Discontinuity  bool
	Complete       bool
	CreationTimeMS int64
}

type PlaylistManager struct {
	cfg                 Config
	storage             *storage.Storage
	streamID            string
	segments            []Segment
	pendingDiscontinuity bool
}

func New(cfg Config, storage *storage.Storage, streamID string) *PlaylistManager {
	return &PlaylistManager{
		cfg:      cfg,
		storage:  storage,
		streamID: streamID,
	}
}

func (p *PlaylistManager) MarkDiscontinuityNext() {
	p.pendingDiscontinuity = true
}

func (p *PlaylistManager) AddPart(segSeq uint64, partURI string, duration time.Duration) {
	seg := p.ensureSegment(segSeq)
	seg.Parts = append(seg.Parts, Part{URI: partURI, Duration: duration.Seconds()})
	p.updateSegment(seg)
}

func (p *PlaylistManager) FinalizeSegment(segSeq uint64, segURI string, duration time.Duration) {
	seg := p.ensureSegment(segSeq)
	seg.URI = segURI
	seg.Duration = duration.Seconds()
	seg.Complete = true
	p.updateSegment(seg)
}

func (p *PlaylistManager) ensureSegment(segSeq uint64) *Segment {
	for i := range p.segments {
		if p.segments[i].Seq == segSeq {
			return &p.segments[i]
		}
	}
	seg := Segment{Seq: segSeq, CreationTimeMS: time.Now().UnixMilli()}
	if p.pendingDiscontinuity {
		seg.Discontinuity = true
		p.pendingDiscontinuity = false
	}
	p.segments = append(p.segments, seg)
	return &p.segments[len(p.segments)-1]
}

func (p *PlaylistManager) updateSegment(seg *Segment) {
	for i := range p.segments {
		if p.segments[i].Seq == seg.Seq {
			p.segments[i] = *seg
			return
		}
	}
}

func (p *PlaylistManager) Prune() []Segment {
	if p.cfg.KeepSegments <= 0 || len(p.segments) <= p.cfg.KeepSegments {
		return nil
	}
	removeCount := len(p.segments) - p.cfg.KeepSegments
	if removeCount <= 0 {
		return nil
	}
	removed := append([]Segment(nil), p.segments[:removeCount]...)
	p.segments = append([]Segment(nil), p.segments[removeCount:]...)
	return removed
}

func (p *PlaylistManager) Render() string {
	b := &strings.Builder{}
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:9\n")
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(p.cfg.TargetDuration.Seconds()))))
	b.WriteString(fmt.Sprintf("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=%.3f,PART-HOLD-BACK=%.3f\n",
		p.cfg.HoldBack.Seconds(), p.cfg.PartHoldBack.Seconds()))
	if p.cfg.EnablePartial {
		b.WriteString(fmt.Sprintf("#EXT-X-PART-INF:PART-TARGET=%.3f\n", p.cfg.PartDuration.Seconds()))
	}
	b.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", p.cfg.InitFilename))
	if len(p.segments) > 0 {
		b.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", p.segments[0].Seq))
	} else {
		b.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")
	}

	for _, seg := range p.segments {
		if seg.Discontinuity {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if p.cfg.EnablePartial {
			for _, part := range seg.Parts {
				b.WriteString(fmt.Sprintf("#EXT-X-PART:DURATION=%.3f,URI=\"%s\"\n", part.Duration, part.URI))
			}
		}
		if seg.Complete {
			b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", seg.Duration))
			b.WriteString(seg.URI)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func (p *PlaylistManager) Write() error {
	return p.WriteTo(p.storage.StreamDir(p.streamID))
}

func (p *PlaylistManager) WriteTo(dir string) error {
	playlist := p.Render()
	path := filepath.Join(dir, p.cfg.PlaylistName)
	return storage.WriteFileAtomic(path, []byte(playlist))
}

func (p *PlaylistManager) LoadFromFile(path string, dropIncomplete bool) (uint64, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	segments, lastSeq, err := parsePlaylist(string(data), dropIncomplete)
	if err != nil {
		return 0, false, err
	}
	p.segments = segments
	return lastSeq, len(segments) > 0, nil
}

func (p *PlaylistManager) RemoveFiles(segments []Segment) error {
	if len(segments) == 0 {
		return nil
	}
	liveDir := p.storage.StreamDir(p.streamID)
	for _, seg := range segments {
		if seg.URI != "" {
			_ = storage.RemoveFile(filepath.Join(liveDir, seg.URI))
		}
		for _, part := range seg.Parts {
			_ = storage.RemoveFile(filepath.Join(liveDir, part.URI))
		}
	}
	return nil
}

func parsePlaylist(content string, dropIncomplete bool) ([]Segment, uint64, error) {
	lines := strings.Split(content, "\n")
	var segments []Segment
	var pendingDiscontinuity bool
	currentIdx := -1
	hasMediaSeq := false
	nextSeq := uint64(0)
	expectURI := false
	pendingDuration := 0.0
	nowMS := time.Now().UnixMilli()

	createSegment := func() int {
		seg := Segment{
			Seq:            nextSeq,
			CreationTimeMS: nowMS,
		}
		nextSeq++
		if pendingDiscontinuity {
			seg.Discontinuity = true
			pendingDiscontinuity = false
		}
		segments = append(segments, seg)
		return len(segments) - 1
	}

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if expectURI {
			if strings.HasPrefix(line, "#") {
				continue
			}
			idx := currentIdx
			if idx < 0 || segments[idx].Complete {
				idx = createSegment()
			}
			segments[idx].URI = line
			segments[idx].Duration = pendingDuration
			segments[idx].Complete = true
			currentIdx = idx
			expectURI = false
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:") {
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"))
			seq, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, 0, err
			}
			hasMediaSeq = true
			nextSeq = seq
			continue
		}
		if line == "#EXT-X-DISCONTINUITY" {
			pendingDiscontinuity = true
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-PART:") {
			part, ok := parsePartLine(line)
			if !ok {
				continue
			}
			idx := currentIdx
			if idx < 0 || segments[idx].Complete {
				idx = createSegment()
			}
			segments[idx].Parts = append(segments[idx].Parts, part)
			currentIdx = idx
			continue
		}
		if strings.HasPrefix(line, "#EXTINF:") {
			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXTINF:"))
			if comma := strings.IndexByte(value, ','); comma != -1 {
				value = value[:comma]
			}
			dur, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, 0, err
			}
			pendingDuration = dur
			expectURI = true
			continue
		}
	}

	if dropIncomplete && len(segments) > 0 && !segments[len(segments)-1].Complete {
		segments = segments[:len(segments)-1]
	}

	if !hasMediaSeq && len(segments) > 0 {
		segments[0].Seq = 0
		for i := 1; i < len(segments); i++ {
			segments[i].Seq = segments[i-1].Seq + 1
		}
	}

	var lastSeq uint64
	if len(segments) > 0 {
		lastSeq = segments[len(segments)-1].Seq
	}
	return segments, lastSeq, nil
}

func parsePartLine(line string) (Part, bool) {
	attrs := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-PART:"))
	if attrs == "" {
		return Part{}, false
	}
	var part Part
	fields := strings.Split(attrs, ",")
	for _, field := range fields {
		kv := strings.SplitN(strings.TrimSpace(field), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := kv[0]
		value := strings.Trim(kv[1], "\"")
		switch key {
		case "DURATION":
			if dur, err := strconv.ParseFloat(value, 64); err == nil {
				part.Duration = dur
			}
		case "URI":
			part.URI = value
		}
	}
	if part.URI == "" {
		return Part{}, false
	}
	return part, true
}
