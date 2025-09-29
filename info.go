// Package torrent provides functionality to create and verify torrent files.
package torrent

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/f4n4t/go-release"
	"github.com/f4n4t/go-release/pkg/progress"
	"github.com/f4n4t/go-release/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	Module = "torrent"
	// HashSize defines the size of an SHA-1 hash in bytes, used for consistent hash representation throughout the system.
	HashSize = sha1.Size
	// sleepTime is the time to sleep between progress updates.
	sleepTime = time.Millisecond * 5
)

// ParallelFileRead is a type that defines if the files should be read in parallel.
type ParallelFileRead int

const (
	// ParallelFileReadDisabled disables parallel file reading (better for hdds).
	ParallelFileReadDisabled ParallelFileRead = iota
	// ParallelFileReadEnabled enables parallel reading of files (improves performance on ssds).
	ParallelFileReadEnabled
	// ParallelFileReadAuto enables it if system is linux and ssd is detected.
	ParallelFileReadAuto
)

// shouldReadParallel determines whether files should be read using a parallel method based on the specified mode.
func (pr ParallelFileRead) shouldReadParallel(path string) bool {
	logger := log.Logger.With().Str("module", Module).Logger()
	switch pr {
	case ParallelFileReadDisabled:
		logger.Debug().Msg("disabling parallel method for reading files")
		return false
	case ParallelFileReadEnabled:
		logger.Debug().Msg("forcing parallel method for reading files")
		return true
	case ParallelFileReadAuto:
		if utils.IsSSD(path) {
			logger.Debug().Msg("detected ssd, using faster parallel method for reading files")
			return true
		} else {
			logger.Debug().Msg("could not detect ssd, using traditional method for reading files")
			return false
		}
	default:
		logger.Debug().Msg("defaulting to traditional method for reading files")
		return false
	}
}

type Service struct {
	showProgress     bool
	createdBy        string
	log              zerolog.Logger
	parallelFileRead ParallelFileRead
	hashThreads      int
}

type ServiceBuilder struct {
	service Service
}

// NewServiceBuilder creates a new ServiceBuilder with default values.
func NewServiceBuilder() *ServiceBuilder {
	return &ServiceBuilder{
		Service{
			createdBy:        "go-torrent",
			log:              log.Logger.With().Str("module", Module).Logger(),
			parallelFileRead: ParallelFileReadAuto,
			hashThreads:      0,
		},
	}
}

// WithCreatedBy sets metadata field `created by` to the given string.
func (s *ServiceBuilder) WithCreatedBy(createdBy string) *ServiceBuilder {
	s.service.createdBy = createdBy
	return s
}

// WithSetProgress sets the showProgress flag to enable or disable the progress bar.
func (s *ServiceBuilder) WithSetProgress(showProgress bool) *ServiceBuilder {
	s.service.showProgress = showProgress
	return s
}

// WithHashThreads sets the number of threads to use for hashing.
func (s *ServiceBuilder) WithHashThreads(i int) *ServiceBuilder {
	s.service.hashThreads = max(0, i)
	return s
}

// WithParallelFileRead sets the parallelFileRead flag to enable or disable parallel file read mode.
func (s *ServiceBuilder) WithParallelFileRead(i int) *ServiceBuilder {
	switch ParallelFileRead(i) {
	case ParallelFileReadAuto:
		s.service.parallelFileRead = ParallelFileReadAuto
	case ParallelFileReadEnabled:
		s.service.parallelFileRead = ParallelFileReadEnabled
	case ParallelFileReadDisabled:
		s.service.parallelFileRead = ParallelFileReadDisabled
	default:
		// should never happen when we use the constants
		panic(fmt.Sprintf("invalid parallel file read mode: %d", i))
	}

	return s
}

// Build creates a new Service with the provided configuration.
func (s *ServiceBuilder) Build() *Service {
	return &Service{
		showProgress:     s.service.showProgress,
		createdBy:        s.service.createdBy,
		log:              s.service.log,
		parallelFileRead: s.service.parallelFileRead,
		hashThreads:      s.service.hashThreads,
	}
}

type MetaInfo struct {
	InfoBytes    bencode.Bytes `bencode:"info,omitempty"`
	Announce     string        `bencode:"announce,omitempty"`
	CreationDate int64         `bencode:"creation date,omitempty,ignore_unmarshal_type_error"`
	Comment      string        `bencode:"comment,omitempty"`
	CreatedBy    string        `bencode:"created by,omitempty"`
}

// Write bencodes and writes to a writer
func (mi MetaInfo) Write(w io.Writer) error {
	return bencode.NewEncoder(w).Encode(mi)
}

// Info is the struct that holds the meta-info for a torrent file.
type Info struct {
	Name        string `bencode:"name"`
	Private     *bool  `bencode:"private,omitempty"`
	PieceLength int64  `bencode:"piece length"`
	Pieces      []byte `bencode:"pieces"`
	Files       Files  `bencode:"files,omitempty"`  // Multi file
	Length      int64  `bencode:"length,omitempty"` // Single file
	Source      string `bencode:"source,omitempty"`
	Publisher   string `bencode:"publisher,omitempty"`
}

// FileInfo represents a file inside a Torrent.
type FileInfo struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
	// FullPath is the full path to the file on the filesystem.
	// Note: Value is only used internally for piece generation.
	FullPath string `bencode:"-"`
	// Offset represents the starting position of the file within the concatenated data of all files in a torrent.
	Offset int64 `bencode:"-"`
}

// Files is a helper type to use methods on this type.
type Files []FileInfo

// TotalLength calculates the total size of all files inside Files.
func (files Files) TotalLength() int64 {
	var total int64
	for _, f := range files {
		total += f.Length
	}
	return total
}

// BuildFullPath constructs the full file path by joining the provided root paths with the FileInfo's internal Path slice.
func (fi FileInfo) BuildFullPath(root ...string) string {
	return filepath.Join(append(root, fi.Path...)...)
}

// FilesFromReleaseInfo extracts file information from a release.Info object and constructs a Files slice.
// It returns an error if any issues occur while determining relative file paths.
func FilesFromReleaseInfo(rel *release.Info) (Files, error) {
	var (
		files         Files
		currentOffset int64
	)
	for _, f := range rel.Root.GetFiles() {
		relPath, err := filepath.Rel(rel.Root.FullPath, f.FullPath)
		if err != nil {
			return nil, fmt.Errorf("get relative path: %w", err)
		}

		files = append(files, FileInfo{
			Length:   f.Info.Size,
			Path:     strings.Split(relPath, string(filepath.Separator)),
			FullPath: f.FullPath,
			Offset:   currentOffset,
		})
		currentOffset += f.Info.Size
	}
	// sort files by path
	sort.Slice(files, func(i, j int) bool {
		return strings.Join(files[i].Path, "") < strings.Join(files[j].Path, "")
	})
	return files, nil
}

// FilesFromPath retrieves all files from the specified root directory recursively and returns them as a Files object.
// It skips directories and calculates the absolute path for each file, handling errors during the process.
// An error is returned if the root directory cannot be accessed or traversed.
func FilesFromPath(root string) (Files, error) {
	// check if the root has another subdirectory
	subFolder := filepath.Join(root, filepath.Base(root))

	if fileInfo, err := os.Stat(subFolder); err == nil && fileInfo.IsDir() {
		root = subFolder
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	fileInfo, err := os.Stat(absRoot)
	if err == nil && !fileInfo.IsDir() {
		return Files{{
			Length:   fileInfo.Size(),
			Path:     []string{fileInfo.Name()},
			FullPath: absRoot,
		}}, nil
	}

	var files Files

	walkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(absRoot, path)
		if err != nil {
			return fmt.Errorf("error getting relative path: %s", err)
		}
		if !info.IsDir() {
			files = append(files, FileInfo{
				Length:   info.Size(),
				Path:     strings.Split(relPath, string(filepath.Separator)),
				FullPath: path,
			})
		}
		return nil
	}

	// sort files by path
	sort.Slice(files, func(i, j int) bool {
		return strings.Join(files[i].Path, "") < strings.Join(files[j].Path, "")
	})

	if err := filepath.Walk(absRoot, walkFunc); err != nil {
		return nil, err
	}

	return files, nil
}

// PieceCount calculates the number of pieces needed for all files inside Files.
func (files Files) PieceCount(pieceLength int64) int {
	return int((files.TotalLength() + pieceLength - int64(1)) / pieceLength)
}

// Create creates torrent for a given release.Release and returns the content as a byte slice.
func (s Service) Create(rel *release.Info, announce ...string) ([]byte, error) {
	// Start time for duration calculation
	startTime := time.Now()

	files, err := FilesFromReleaseInfo(rel)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files available")
	}

	pieceLength := GetPieceLength(rel.Size)

	s.log.Info().Str("size", utils.Bytes(files.TotalLength())).
		Str("pieceLength", utils.Bytes(pieceLength)).
		Int("totalPieces", files.PieceCount(pieceLength)).Msg("generating pieces")

	piecesTask, err := GeneratePieces(files, pieceLength, s.parallelFileRead, s.hashThreads)
	if err != nil {
		return nil, fmt.Errorf("generate pieces: %w", err)
	}

	bar := progress.NewProgressBar(s.showProgress, files.TotalLength(), true)

	for !piecesTask.IsFinished() {
		_ = bar.Set64(piecesTask.GetBytesHashed())
		time.Sleep(sleepTime)
	}

	if err := piecesTask.GetError(); err != nil {
		bar.Cancel()
		return nil, fmt.Errorf("generate pieces: %w", err)
	}

	_ = bar.Finish()

	pieces, err := piecesTask.GetPieces()
	if err != nil {
		return nil, fmt.Errorf("get pieces: %w", err)
	}

	private := true

	info := &Info{
		Name:        rel.Name,
		Private:     &private,
		PieceLength: pieceLength,
		Pieces:      pieces,
	}

	switch {
	case len(rel.Root.Children) > 0:
		// multi file torrent
		info.Files = files

	default:
		// single file torrent
		info.Name = rel.Root.Info.Name
		info.Length = rel.Root.Info.Size
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("error bencoding torrent info: %w", err)
	}

	mi := MetaInfo{
		InfoBytes:    infoBytes,
		CreationDate: time.Now().Unix(),
		CreatedBy:    s.createdBy,
	}

	if len(announce) > 0 && announce[0] != "" {
		mi.Announce = announce[0]
	}

	var b bytes.Buffer

	if err := mi.Write(&b); err != nil {
		return nil, fmt.Errorf("error bencoding torrent info: %w", err)
	}

	s.log.Info().Str("dur", time.Since(startTime).String()).Msg("torrent created")

	return b.Bytes(), nil
}

// Verify verifies the files in the sourcePath against the torrent file.
func (s Service) Verify(tFile *File, sourcePath string) error {
	files, err := FilesFromPath(sourcePath)
	if err != nil {
		return err
	}

	var (
		torrentFiles  = tFile.Info.UpvertedFiles()
		filteredFiles = make(Files, len(torrentFiles))
		startTime     = time.Now()
	)

	for i, f := range torrentFiles {
		if f.Path == nil {
			// single-file Torrent
			f.Path = []string{tFile.Info.Name}
		}

		found := false
		for _, file := range files {
			if equalPath(f.Path, file.Path) {
				found = true
				filteredFiles[i] = file
				break
			}
		}
		if !found {
			return fmt.Errorf("file %s is not in the torrent", f.Path)
		}
	}

	// add Offset to each file
	var currentOffset int64
	for i, f := range filteredFiles {
		filteredFiles[i].Offset = currentOffset
		currentOffset += f.Length
	}

	s.log.Info().
		Int("fileCount", len(torrentFiles)).
		Str("size", utils.Bytes(filteredFiles.TotalLength())).
		Int("pieces", len(tFile.Info.Pieces)/HashSize).Msg("verifying pieces...")

	piecesTask, err := VerifyPieces(filteredFiles, tFile.Info.PieceLength, tFile.Info.Pieces,
		s.parallelFileRead, s.hashThreads)
	if err != nil {
		return fmt.Errorf("verify pieces: %w", err)
	}

	bar := progress.NewProgressBar(s.showProgress, filteredFiles.TotalLength(), true)

	for !piecesTask.IsFinished() {
		_ = bar.Set64(piecesTask.GetBytesHashed())
		time.Sleep(sleepTime)
	}

	if err := piecesTask.GetError(); err != nil {
		bar.Cancel()
		return fmt.Errorf("verify pieces: %w", err)
	}

	_ = bar.Finish()

	log.Info().
		Str("dur", time.Since(startTime).String()).
		Msg("verification completed")

	return nil
}

// equalPath compares two slices of strings and returns true if they are equal.
func equalPath(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, p := range a {
		if p != b[i] {
			return false
		}
	}

	return true
}

// UpvertedFiles returns the files in the Info struct or a single file if no files are present.
func (info *Info) UpvertedFiles() []FileInfo {
	if len(info.Files) == 0 {
		return []FileInfo{{
			Length: info.Length,
			// Callers should determine that Info.Name is the basename, and
			// thus a regular file.
			Path: nil,
		}}
	}
	return info.Files
}

func (info *Info) IsDir() bool {
	return len(info.Files) > 0
}
