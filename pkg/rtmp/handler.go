package rtmp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strings"

	"github.com/Eyevinn/mp4ff/avc"
	"github.com/yutopp/go-flv/tag"
	"github.com/yutopp/go-rtmp"
	rtmpmsg "github.com/yutopp/go-rtmp/message"

	"tokuly-live-rtmp-server/pkg/archive"
	"tokuly-live-rtmp-server/pkg/config"
	"tokuly-live-rtmp-server/pkg/policy"
	"tokuly-live-rtmp-server/pkg/storage"
	"tokuly-live-rtmp-server/pkg/util"
)

type Handler struct {
	rtmp.DefaultHandler

	cfg     config.Config
	policy  policy.Policy
	storage *storage.Storage
	manager *StreamManager
	archiveManager *archive.Manager

	conn      net.Conn
	app       string
	userAgent string
	remoteIP  string
	streamKey string
	streamName string
	session   *Session
}

func NewHandler(cfg config.Config, pol policy.Policy, storage *storage.Storage, manager *StreamManager, archiveManager *archive.Manager, conn net.Conn) *Handler {
	remoteIP := ""
	if conn != nil {
		host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
		if err == nil {
			remoteIP = host
		}
	}
	return &Handler{
		cfg:     cfg,
		policy:  pol,
		storage: storage,
		manager: manager,
		archiveManager: archiveManager,
		conn:    conn,
		remoteIP: remoteIP,
	}
}

func (h *Handler) OnServe(conn *rtmp.Conn) {
}

func (h *Handler) OnConnect(timestamp uint32, cmd *rtmpmsg.NetConnectionConnect) error {
	h.app = cmd.Command.App
	h.userAgent = cmd.Command.FlashVer
	if err := h.validateApp(); err != nil {
		return err
	}
	return nil
}

func (h *Handler) OnPublish(_ *rtmp.StreamContext, timestamp uint32, cmd *rtmpmsg.NetStreamPublish) error {
	if err := h.validateApp(); err != nil {
		return err
	}
	streamKey := sanitizeStreamKey(cmd.PublishingName)
	if streamKey == "" {
		return fmt.Errorf("publish name empty")
	}
	if h.session != nil {
		return fmt.Errorf("already publishing")
	}

	authResult, err := h.policy.Authorize(context.Background(), streamKey, h.remoteIP, h.userAgent, h.app)
	if err != nil || authResult.Decision == policy.DecisionReject {
		return fmt.Errorf("authorization failed")
	}

	streamName := streamKey
	if authResult.StreamName != "" {
		streamName = sanitizeStreamID(authResult.StreamName)
		if streamName == "" {
			streamName = streamKey
		}
	}
	if h.cfg.DebugRTMP {
		streamName = "rtmp-test"
	}
	if h.archiveManager != nil {
		if err := h.archiveManager.CanPublish(streamName); err != nil {
			return err
		}
	}
	enableRewind := h.cfg.Storage.EnableRewind
	if authResult.AllowRewind != nil {
		enableRewind = enableRewind && *authResult.AllowRewind
	}
	session := NewSession(h.cfg, h.policy, h.storage, h.archiveManager, streamKey, streamName, h.app, h.remoteIP, h.userAgent, enableRewind)
	if err := h.manager.Register(session); err != nil {
		return fmt.Errorf("stream already active")
	}
	h.streamKey = streamKey
	h.streamName = streamName
	h.session = session
	log.Printf("publish start: stream_key_hash=%s app=%s remote=%s", maskStreamKey(streamKey), h.app, h.remoteIP)
	return nil
}

func (h *Handler) OnSetDataFrame(timestamp uint32, data *rtmpmsg.NetStreamSetDataFrame) error {
	if h.session == nil || len(data.Payload) == 0 {
		return nil
	}
	var script tag.ScriptData
	if err := tag.DecodeScriptData(bytes.NewReader(data.Payload), &script); err != nil {
		return nil
	}
	meta, ok := script.Objects["onMetaData"]
	if !ok {
		return nil
	}
	h.session.HandleMetadata(map[string]interface{}(meta))
	return nil
}

func (h *Handler) OnAudio(timestamp uint32, payload io.Reader) error {
	if h.session == nil {
		return nil
	}
	var audio tag.AudioData
	if err := tag.DecodeAudioData(payload, &audio); err != nil {
		return err
	}
	if audio.SoundFormat != tag.SoundFormatAAC {
		if h.cfg.Policy.RejectIfAudioNotAAC {
			return fmt.Errorf("audio codec not supported")
		}
		return nil
	}
	body := new(bytes.Buffer)
	if _, err := io.Copy(body, audio.Data); err != nil {
		return err
	}

	switch audio.AACPacketType {
	case tag.AACPacketTypeSequenceHeader:
		cfg, err := util.ParseAudioSpecificConfig(body.Bytes())
		if err != nil {
			return err
		}
		return h.session.HandleAudioConfig(cfg)
	case tag.AACPacketTypeRaw:
		return h.session.HandleAudioSample(int64(timestamp), body.Bytes())
	default:
		return nil
	}
}

func (h *Handler) OnVideo(timestamp uint32, payload io.Reader) error {
	if h.session == nil {
		return nil
	}
	var video tag.VideoData
	if err := tag.DecodeVideoData(payload, &video); err != nil {
		return err
	}
	if video.CodecID != tag.CodecIDAVC {
		if h.cfg.Policy.RejectIfVideoNotH264 {
			return fmt.Errorf("video codec not supported")
		}
		return nil
	}
	body := new(bytes.Buffer)
	if _, err := io.Copy(body, video.Data); err != nil {
		return err
	}

	switch video.AVCPacketType {
	case tag.AVCPacketTypeSequenceHeader:
		cfg, err := util.ParseAVCDecoderConfig(body.Bytes())
		if err != nil {
			return err
		}
		return h.session.HandleVideoConfig(cfg)
	case tag.AVCPacketTypeNALU:
		data := body.Bytes()
		isKey := video.FrameType == tag.FrameTypeKeyFrame || avc.IsIDRSample(data)
		return h.session.HandleVideoSample(int64(timestamp), int64(video.CompositionTime), data, isKey)
	default:
		return nil
	}
}

func (h *Handler) OnDeleteStream(timestamp uint32, cmd *rtmpmsg.NetStreamDeleteStream) error {
	if h.session != nil {
		h.session.Close(context.Background())
		h.manager.Remove(h.streamKey, h.streamName)
		h.session = nil
	}
	return nil
}

func (h *Handler) OnClose() {
	if h.session != nil {
		h.session.Close(context.Background())
		h.manager.Remove(h.streamKey, h.streamName)
		h.session = nil
	}
}

func sanitizeStreamKey(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Trim(name, "/")
	if name == "" {
		return ""
	}
	if idx := strings.Index(name, "?"); idx != -1 {
		query := name[idx+1:]
		name = name[:idx]
		for _, pair := range strings.Split(query, "&") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 && parts[0] == "key" && parts[1] != "" {
				return parts[1]
			}
		}
	}
	return filepath.Base(name)
}

func (h *Handler) validateApp() error {
	expected := normalizeApp(h.cfg.RTMP.App)
	if expected == "" {
		return nil
	}
	actual := normalizeApp(h.app)
	if actual == "" || actual != expected {
		return fmt.Errorf("invalid app")
	}
	return nil
}

func normalizeApp(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if idx := strings.Index(name, "?"); idx != -1 {
		name = name[:idx]
	}
	name = strings.Trim(name, "/")
	if name == "" {
		return ""
	}
	return name
}

func sanitizeStreamID(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Trim(name, "/")
	if name == "" {
		return ""
	}
	if idx := strings.Index(name, "?"); idx != -1 {
		name = name[:idx]
	}
	base := filepath.Base(name)
	if base == "." || base == ".." {
		return ""
	}
	return base
}
