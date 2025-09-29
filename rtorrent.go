package torrent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anacrolix/torrent/bencode"
)

type Resume struct {
	Bitfield                 int          `bencode:"bitfield"`
	Files                    []ResumeFile `bencode:"files"`
	UncertainPiecesTimestamp int64        `bencode:"uncertain_pieces.timestamp"`
}

type ResumeFile struct {
	Completed int64 `bencode:"completed"`
	Mtime     int64 `bencode:"mtime"`
	Priority  int   `bencode:"priority"`
}

type RTorrent struct {
	ChunksDone        int    `bencode:"chunks_done"`
	ChunksWanted      int    `bencode:"chunks_wanted"`
	Complete          int    `bencode:"complete"`
	Directory         string `bencode:"directory"`
	Hashing           int    `bencode:"hasing"`
	State             int    `bencode:"state"`
	StateChanged      int64  `bencode:"state_changed"`
	StateCounter      int    `bencode:"state_counter"`
	TiedToFile        string `bencode:"tied_to_file"`
	TimestampFinished int64  `bencode:"timestamp.finished"`
	TimestampStarted  int64  `bencode:"timestamp.started"`
}

// IncludeFastResume is used to incorporate fast resume data into the torrent file, allowing rtorrent to skip hash checks.
// basically the same as this perl script: https://github.com/rakshasa/rtorrent/blob/master/doc/rtorrent_fast_resume.pl
func (s Service) IncludeFastResume(torrentContent []byte, baseDir string) ([]byte, error) {
	// first load known fields
	var tFile File

	if err := bencode.Unmarshal(torrentContent, &tFile); err != nil {
		return nil, fmt.Errorf("failed to decode torrent content: %w", err)
	}

	type fileInfo struct {
		ModTime time.Time
		Length  int64
	}

	var (
		files       = make([]fileInfo, len(tFile.Info.UpvertedFiles()))
		totalLength int64
	)

	for i, f := range tFile.Info.UpvertedFiles() {
		fPath := strings.Join(append([]string{tFile.Info.Name}, f.Path...), string(os.PathSeparator))

		fileStat, err := os.Stat(filepath.Join(baseDir, fPath))
		if err != nil {
			return nil, err
		}

		files[i] = fileInfo{
			ModTime: fileStat.ModTime(),
			Length:  f.Length,
		}

		totalLength += f.Length
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("error no files available")
	}

	totalChunks := int((totalLength + tFile.Info.PieceLength - 1) / tFile.Info.PieceLength)

	if totalChunks*20 != len(tFile.Info.Pieces) {
		return nil, fmt.Errorf("failed to add resume data, inconsistent piece information")
	}

	var resumeData Resume

	resumeData.Bitfield = totalChunks

	// Initiale state no rest
	var pmod int64 = 0

	for _, file := range files {
		var (
			fileSize         = file.Length
			fileChunks int64 = 0
		)

		// If we have a remainder from before, a chunk is added
		if pmod != 0 {
			fileChunks = 1
			if pmod >= fileSize {
				// The remainder is bigger than the current file, so the file is completely covered
				pmod -= fileSize
				fileSize = 0
			} else {
				// Subtract the remainder from the file size
				fileSize -= pmod
				pmod = 0
			}
		}

		// Calculate the number of complete chunks
		fileChunks += (fileSize + tFile.Info.PieceLength - 1) / tFile.Info.PieceLength

		// Update `pmod` for the next file
		if fileSize%tFile.Info.PieceLength != 0 {
			pmod = tFile.Info.PieceLength - (fileSize % tFile.Info.PieceLength)
		}

		// Add file to resume data
		resumeData.Files = append(resumeData.Files, ResumeFile{
			Completed: fileChunks,
			Mtime:     file.ModTime.Unix(),
			Priority:  0, // Don't download; we already have the file...
		})
	}

	resumeData.UncertainPiecesTimestamp = time.Now().Unix()

	var directory string

	if len(tFile.Info.Files) > 0 {
		// multifile torrent
		directory = filepath.Join(baseDir, tFile.Info.Name)
	} else {
		// singlefile
		directory = baseDir
	}

	rtorrentData := RTorrent{
		ChunksDone:        totalChunks,
		ChunksWanted:      0,
		Complete:          1,
		Directory:         directory,
		Hashing:           0, // no hashing
		State:             1, // started
		StateChanged:      time.Now().Unix(),
		StateCounter:      1,
		TimestampFinished: 0,
		TimestampStarted:  time.Now().Unix(),
	}

	// now load the torrent again, and add the new fields
	var unknownContent map[string]any

	if err := bencode.Unmarshal(torrentContent, &unknownContent); err != nil {
		return nil, fmt.Errorf("failed to decode torrent content: %w", err)
	}

	unknownContent["rtorrent"] = rtorrentData
	unknownContent["libtorrent_resume"] = resumeData

	return bencode.Marshal(unknownContent)
}
