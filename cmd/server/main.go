package main

import (
	"io"
	"log"
	"net"
	"time"

	logrus "github.com/sirupsen/logrus"
	"github.com/yutopp/go-rtmp"

	"tokuly-live-rtmp-server/pkg/archive"
	"tokuly-live-rtmp-server/pkg/config"
	rtmpsrv "tokuly-live-rtmp-server/pkg/rtmp"
	"tokuly-live-rtmp-server/pkg/policy"
	"tokuly-live-rtmp-server/pkg/storage"
)

func main() {
	cfg := config.Load()

	st := storage.New(cfg.Storage.RootDir, cfg.Storage.RewindRoot, cfg.Storage.EnableRewind)
	manager := rtmpsrv.NewStreamManager(cfg.Limits.MaxConcurrentStreams, st, 30*time.Second)

	pol := &policy.HTTPPolicy{
		AuthURL:       cfg.Auth.AuthURL,
		StreamEndURL:  cfg.Auth.StreamEndURL,
		APIKey:        cfg.Auth.APIKey,
		Version:       cfg.Auth.Version,
		Timeout:       cfg.Auth.AuthTimeout,
		HTTPUserAgent: cfg.Auth.HTTPUserAgent,
		DebugSkip:     cfg.DebugRTMP,
		Config: policy.Config{
			MaxWidth:             cfg.Policy.MaxWidth,
			MaxHeight:            cfg.Policy.MaxHeight,
			FirstKeyframeTimeout: cfg.Policy.FirstKeyframeTimeout,
			MaxGOPSeconds:        cfg.Policy.MaxGOPSeconds,
			AllowNoAudio:         cfg.Policy.AllowNoAudio,
			OnGOPTooLong:         cfg.Policy.OnGOPTooLong,
			RequireAACLC:         cfg.Policy.RequireAACLC,
			RejectIfVideoNotH264: cfg.Policy.RejectIfVideoNotH264,
			RejectIfAudioNotAAC:  cfg.Policy.RejectIfAudioNotAAC,
		},
	}
	archiveManager := archive.NewManager(cfg.Archive, pol, cfg.Policy.AllowNoAudio)

	listener, err := net.Listen("tcp", cfg.RTMP.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("rtmp listening on %s", cfg.RTMP.ListenAddr)

	logger := logrus.New()
	server := rtmp.NewServer(&rtmp.ServerConfig{
		OnConnect: func(conn net.Conn) (io.ReadWriteCloser, *rtmp.ConnConfig) {
			h := rtmpsrv.NewHandler(cfg, pol, st, manager, archiveManager, conn)
			return conn, &rtmp.ConnConfig{
				Handler: h,
				ControlState: rtmp.StreamControlStateConfig{
					DefaultBandwidthWindowSize: 6 * 1024 * 1024 / 8,
				},
				Logger: logger,
			}
		},
	})

	if err := server.Serve(listener); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
