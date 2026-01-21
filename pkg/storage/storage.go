package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Storage struct {
	RootDir      string
	RewindRoot   string
	EnableRewind bool
}

func New(rootDir, rewindRoot string, enableRewind bool) *Storage {
	return &Storage{
		RootDir:      rootDir,
		RewindRoot:   rewindRoot,
		EnableRewind: enableRewind,
	}
}

func (s *Storage) StreamDir(streamID string) string {
	return filepath.Join(s.RootDir, streamID)
}

func (s *Storage) RewindDir(streamID string) string {
	return filepath.Join(s.RewindRoot, streamID)
}

func (s *Storage) EnsureStreamDirs(streamID string) (string, string, error) {
	liveDir := s.StreamDir(streamID)
	if err := os.MkdirAll(liveDir, 0755); err != nil {
		return "", "", err
	}
	var rewindDir string
	if s.EnableRewind {
		rewindDir = s.RewindDir(streamID)
		if err := os.MkdirAll(rewindDir, 0755); err != nil {
			return "", "", err
		}
	}
	return liveDir, rewindDir, nil
}

func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func WriteFileAtomicReader(path string, r io.Reader) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "."+filepath.Base(path)+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, r)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, path)
}

func RemoveFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func CopyOrLink(src, dst string) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func MustJoin(root, name string) string {
	if root == "" {
		panic(fmt.Sprintf("empty root for %s", name))
	}
	return filepath.Join(root, name)
}

func (s *Storage) RemoveStreamDirs(streamID string) error {
	if streamID == "" {
		return fmt.Errorf("empty streamID")
	}
	if err := os.RemoveAll(s.StreamDir(streamID)); err != nil {
		return err
	}
	if s.EnableRewind {
		if err := os.RemoveAll(s.RewindDir(streamID)); err != nil {
			return err
		}
	}
	return nil
}
