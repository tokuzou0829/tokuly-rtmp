package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"tokuly-live-rtmp-server/pkg/inspect"
)

type Decision int

const (
	DecisionAccept Decision = iota
	DecisionReject
	DecisionDegraded
)

const (
	ReasonKeyInvalid        = "KEY_INVALID"
	ReasonCodecUnsupported  = "CODEC_UNSUPPORTED"
	ReasonResolutionTooBig  = "RESOLUTION_TOO_LARGE"
	ReasonGOPTooLong        = "GOP_TOO_LONG"
	ReasonNoKeyframeTimeout = "NO_KEYFRAME_TIMEOUT"
	ReasonAudioUnsupported  = "AUDIO_UNSUPPORTED"
)

type Result struct {
	Decision Decision
	Reason   string
	Message  string
	StreamName string
	AllowRewind *bool
}

type Policy interface {
	Authorize(ctx context.Context, streamKey, remoteIP, userAgent, app string) (Result, error)
	Evaluate(ctx context.Context, result inspect.Result) Result
	NotifyStreamEnd(ctx context.Context, streamKey string) error
	NotifyVideoInfo(ctx context.Context, streamKey string, result inspect.Result) error
	NotifyArchiveStatus(ctx context.Context, streamKey string, status bool) error
}

type HTTPPolicy struct {
	AuthURL       string
	StreamEndURL  string
	APIKey        string
	Version       string
	Timeout       time.Duration
	HTTPUserAgent string
	DebugSkip     bool
	Config        Config
}

const videoInfoURL = "https://api.tokuly.com/live/stream/videoinfo"
const archiveStatusURL = "https://api.tokuly.com/live/stream/archive/status"

type Config struct {
	MaxWidth             int
	MaxHeight            int
	FirstKeyframeTimeout time.Duration
	MaxGOPSeconds        float64
	AllowNoAudio         bool
	OnGOPTooLong         string
	RequireAACLC         bool
	RejectIfVideoNotH264 bool
	RejectIfAudioNotAAC  bool
}

func (p *HTTPPolicy) Authorize(ctx context.Context, streamKey, remoteIP, userAgent, app string) (Result, error) {
	if p.DebugSkip || p.AuthURL == "" {
		return Result{Decision: DecisionAccept}, nil
	}
	reqURL, err := url.Parse(p.AuthURL)
	if err != nil {
		return Result{Decision: DecisionReject, Reason: ReasonKeyInvalid, Message: "auth url invalid"}, err
	}
	form := url.Values{}
	form.Set("key", streamKey)
	if p.APIKey != "" {
		form.Set("APIkey", p.APIKey)
	}
	if p.Version != "" {
		form.Set("version", p.Version)
	}
	bodyString := form.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewBufferString(bodyString))
	if err != nil {
		return Result{Decision: DecisionReject, Reason: ReasonKeyInvalid, Message: "auth request failed"}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if p.HTTPUserAgent != "" {
		req.Header.Set("User-Agent", p.HTTPUserAgent)
	}
	if remoteIP != "" {
		req.Header.Set("X-Forwarded-For", remoteIP)
	}
	if userAgent != "" {
		req.Header.Set("X-RTMP-User-Agent", userAgent)
	}
	if app != "" {
		req.Header.Set("X-RTMP-App", app)
	}

	client := &http.Client{Timeout: p.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Decision: DecisionReject, Reason: ReasonKeyInvalid, Message: "auth request error"}, err
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return Result{Decision: DecisionReject, Reason: ReasonKeyInvalid, Message: fmt.Sprintf("auth status %d", resp.StatusCode)}, nil
	}

	authResp, err := parseAuthResponse(bodyBytes)
	if err != nil {
		return Result{Decision: DecisionAccept, Message: "auth response parse error"}, nil
	}
	return Result{Decision: DecisionAccept, StreamName: authResp.StreamName, AllowRewind: authResp.AllowRewind}, nil
}

func (p *HTTPPolicy) Evaluate(ctx context.Context, result inspect.Result) Result {
	if p.Config.RejectIfVideoNotH264 && result.VideoCodec != "H264" {
		return Result{Decision: DecisionReject, Reason: ReasonCodecUnsupported, Message: "video codec not supported"}
	}
	if result.Width > 0 && result.Height > 0 {
		if result.Width > p.Config.MaxWidth || result.Height > p.Config.MaxHeight {
			return Result{Decision: DecisionReject, Reason: ReasonResolutionTooBig, Message: "resolution too large"}
		}
	}
	if !result.KeyframeReceived {
		return Result{Decision: DecisionReject, Reason: ReasonNoKeyframeTimeout, Message: "first keyframe timeout"}
	}
	if result.AudioCodec == "" && !p.Config.AllowNoAudio {
		return Result{Decision: DecisionReject, Reason: ReasonAudioUnsupported, Message: "audio required"}
	}
	if p.Config.RejectIfAudioNotAAC && result.AudioCodec != "" && result.AudioCodec != "AAC" {
		return Result{Decision: DecisionReject, Reason: ReasonAudioUnsupported, Message: "audio codec not supported"}
	}
	if p.Config.MaxGOPSeconds > 0 && result.GOPSeconds > 0 {
		if result.GOPSeconds > p.Config.MaxGOPSeconds {
			if p.Config.OnGOPTooLong == "reject" {
				return Result{Decision: DecisionReject, Reason: ReasonGOPTooLong, Message: "gop too long"}
			}
			return Result{Decision: DecisionDegraded, Reason: ReasonGOPTooLong, Message: "gop too long"}
		}
	}
	return Result{Decision: DecisionAccept}
}

func (p *HTTPPolicy) NotifyStreamEnd(ctx context.Context, streamKey string) error {
	if p.DebugSkip || p.StreamEndURL == "" {
		return nil
	}
	reqURL, err := url.Parse(p.StreamEndURL)
	if err != nil {
		return err
	}
	form := url.Values{}
	form.Set("key", streamKey)
	if p.APIKey != "" {
		form.Set("APIkey", p.APIKey)
	}
	if p.Version != "" {
		form.Set("version", p.Version)
	}
	bodyString := form.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewBufferString(bodyString))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if p.HTTPUserAgent != "" {
		req.Header.Set("User-Agent", p.HTTPUserAgent)
	}
	client := &http.Client{Timeout: p.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (p *HTTPPolicy) NotifyVideoInfo(ctx context.Context, streamKey string, result inspect.Result) error {
	if p.DebugSkip || p.APIKey == "" {
		return nil
	}
	payload := map[string]string{
		"name":      streamKey,
		"size_w":    strconv.Itoa(result.Width),
		"size_h":    strconv.Itoa(result.Height),
		"video_fps": formatFPS(result.VideoFPS),
		"key":       p.APIKey,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, videoInfoURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.HTTPUserAgent != "" {
		req.Header.Set("User-Agent", p.HTTPUserAgent)
	}
	client := &http.Client{Timeout: p.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("video info status %d", resp.StatusCode)
	}
	return nil
}

func (p *HTTPPolicy) NotifyArchiveStatus(ctx context.Context, streamKey string, status bool) error {
	if p.DebugSkip || p.APIKey == "" {
		return nil
	}
	payload := map[string]interface{}{
		"name":   streamKey,
		"status": status,
		"key":    p.APIKey,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, archiveStatusURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.HTTPUserAgent != "" {
		req.Header.Set("User-Agent", p.HTTPUserAgent)
	}
	client := &http.Client{Timeout: p.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("archive status %d", resp.StatusCode)
	}
	return nil
}

func formatFPS(value float64) string {
	if value <= 0 {
		return "0"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

type authResponse struct {
	StreamName  string
	AllowRewind *bool
}

func parseAuthResponse(data []byte) (authResponse, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return authResponse{}, nil
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return authResponse{}, err
	}
	resp := authResponse{}
	if value, ok := raw["stream_name"]; ok {
		if name, ok := value.(string); ok {
			resp.StreamName = name
		}
	}
	if value, ok := raw["allow_rewind"]; ok {
		if allow, ok := parseBoolValue(value); ok {
			resp.AllowRewind = &allow
		}
	}
	return resp, nil
}

func parseBoolValue(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return false, false
		}
		return parsed, true
	case float64:
		return v != 0, true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i != 0, true
		}
		if f, err := v.Float64(); err == nil {
			return f != 0, true
		}
	}
	return false, false
}
