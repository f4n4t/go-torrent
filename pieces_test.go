package torrent

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestPieceHasher_Concurrent tests the hasher with various real-world scenarios.
// Test cases are designed to cover common torrent types and sizes:
//   - Piece sizes: 64KB for small files (standard minimum)
//     4MB for large files
//   - Worker counts: 1 (sequential baseline)
//     2 (minimal concurrency)
//     4 (typical desktop CPU)
//     8 (high-end desktop/server)
func TestPieceHasher_Concurrent(t *testing.T) {
	tests := []struct {
		name      string
		numFiles  int
		fileSize  int64
		pieceLen  int64
		numPieces int
	}{
		{
			name:      "single small file",
			numFiles:  1,
			fileSize:  1 << 20, // 1 MB
			pieceLen:  1 << 16, // 64 KB
			numPieces: 16,
		},
		{
			name:      "1080p episode",
			numFiles:  1,
			fileSize:  4 << 30, // 4 GB
			pieceLen:  1 << 22, // 4 MB
			numPieces: 1024,
		},
		{
			name:      "multi-file album",
			numFiles:  12,       // typical album length
			fileSize:  40 << 20, // 40MB per track
			pieceLen:  1 << 16,  // 64KB pieces
			numPieces: 7680,     // ~480MB total
		},
	}

	zerolog.SetGlobalLevel(zerolog.Disabled)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalSize := tt.fileSize * int64(tt.numFiles)
			if totalSize > 1<<30 {
				// skip large tests on Windows to avoid CI slowdowns
				if runtime.GOOS == "windows" {
					t.Skipf("skipping large file test %s on Windows", tt.name)
				}
				// also skip in short mode if total size > 1GB
				if testing.Short() {
					t.Skipf("skipping large file test %s in short mode", tt.name)
				}
			}

			files, expectedHashes := createTestFilesFast(t, tt.numFiles, tt.fileSize, tt.pieceLen)

			// test with different worker counts
			workerCounts := []int{1, 2, 4, 8}
			for _, parallelRead := range []ParallelFileRead{ParallelFileReadEnabled, ParallelFileReadDisabled} {
				for _, workers := range workerCounts {
					t.Run(fmt.Sprintf("parallelRead_%d-workers_%d", parallelRead, workers), func(t *testing.T) {
						task, err := GeneratePieces(files, tt.pieceLen, parallelRead, workers)
						if err != nil {
							t.Fatalf("GeneratePieces failed parallelRead: %d, workers: %d, err: %v",
								parallelRead, workers, err)
						}
						for !task.IsFinished() {
							time.Sleep(10 * time.Millisecond)
						}
						if task.GetError() != nil {
							t.Fatalf("GeneratePieces failed parallelRead: %d, workers: %d, err: %v",
								parallelRead, workers, task.error)
						}
						verifyHashes(t, task.generatedHashes, concatHashes(expectedHashes))
					})
				}
			}
		})
	}
}

func concatHashes(hashes [][]byte) []byte {
	result := make([]byte, 0, len(hashes)*HashSize)
	for _, h := range hashes {
		result = append(result, h[:]...)
	}
	return result
}

// createTestFilesFast creates test files more efficiently using sparse files
func createTestFilesFast(t *testing.T, numFiles int, fileSize, pieceLen int64) ([]FileInfo, [][]byte) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "hasher_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	var files []FileInfo
	var expectedHashes [][]byte
	var offset int64

	// create a repeatable pattern that's more representative of real data
	pattern := make([]byte, pieceLen)
	for i := range pattern {
		// create a pseudo-random but deterministic pattern
		pattern[i] = byte((i*7 + 13) % 251) // prime numbers help create distribution
	}

	for i := range numFiles {
		path := filepath.Join(tempDir, fmt.Sprintf("test_file_%d", i))

		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		// write a pattern at the start and end of a file to simulate real data
		// while keeping the file sparse in the middle
		if _, err := f.Write(pattern); err != nil {
			f.Close()
			t.Fatalf("failed to write pattern: %v", err)
		}

		if _, err := f.Seek(fileSize-int64(len(pattern)), io.SeekStart); err != nil {
			f.Close()
			t.Fatalf("failed to seek: %v", err)
		}

		if _, err := f.Write(pattern); err != nil {
			f.Close()
			t.Fatalf("failed to write pattern: %v", err)
		}

		if err := f.Truncate(fileSize); err != nil {
			f.Close()
			t.Fatalf("failed to truncate file: %v", err)
		}
		f.Close()

		files = append(files, FileInfo{
			FullPath: path,
			Length:   fileSize,
			Offset:   offset,
		})
		offset += fileSize

		// calculate expected hashes with the pattern
		h := sha1.New()
		for pos := int64(0); pos < fileSize; pos += pieceLen {
			h.Reset()
			if pos == 0 || pos >= fileSize-pieceLen {
				h.Write(pattern) // use a pattern for first and last pieces
			} else {
				h.Write(make([]byte, pieceLen)) // zero bytes for middle pieces
			}
			expectedHashes = append(expectedHashes, h.Sum(nil))
		}
	}

	return files, expectedHashes
}

func verifyHashes(t *testing.T, got, want []byte) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("piece count mismatch: got %d, want %d", len(got), len(want))
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("piece hash mismatch")
	}
}

// createTestFilesWithPattern creates test files filled with a deterministic pattern.
func createTestFilesWithPattern(t *testing.T, tempDir string, fileSizes []int64, pieceLen int64) ([]FileInfo, [][]byte) {
	t.Helper()

	var files []FileInfo
	var allExpectedHashes [][]byte
	var offset int64
	var globalBuffer bytes.Buffer

	// create a repeatable pattern that's more representative of real data
	pattern := make([]byte, pieceLen)
	for i := range pattern {
		pattern[i] = byte((i*11 + 17) % 253) // different primes for variety
	}

	for i, fileSize := range fileSizes {
		path := filepath.Join(tempDir, fmt.Sprintf("boundary_test_file_%d", i))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("failed to create file %s: %v", path, err)
		}

		// write the pattern repeatedly to fill the file
		written := int64(0)
		for written < fileSize {
			toWrite := pieceLen
			if written+toWrite > fileSize {
				toWrite = fileSize - written
			}
			n, err := f.Write(pattern[:toWrite])
			if err != nil {
				f.Close()
				t.Fatalf("failed to write pattern to %s: %v", path, err)
			}
			// also write to global buffer for hash calculation
			globalBuffer.Write(pattern[:toWrite])
			written += int64(n)
		}
		f.Close()

		files = append(files, FileInfo{
			FullPath: path,
			Length:   fileSize,
			Offset:   offset,
		})
		offset += fileSize
	}

	// calculate expected hashes from the global buffer
	globalData := globalBuffer.Bytes()
	totalSize := int64(len(globalData))
	numPieces := (totalSize + pieceLen - 1) / pieceLen

	h := sha1.New()
	for i := range numPieces {
		start := i * pieceLen
		end := min(start+pieceLen, totalSize)
		h.Reset()
		h.Write(globalData[start:end])
		allExpectedHashes = append(allExpectedHashes, h.Sum(nil))
	}

	return files, allExpectedHashes
}

// TestPieceHasher_EdgeCases tests various edge cases and error conditions
func TestPieceHasher_EdgeCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hasher_test_edge")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	tests := []struct {
		name      string
		setup     func() []FileInfo
		pieceLen  int64
		numPieces int
		wantErr   bool
		skipOnWin bool
	}{
		{
			name: "non-existent file",
			setup: func() []FileInfo {
				return []FileInfo{{
					FullPath: filepath.Join(tempDir, "nonexistent"),
					Length:   1024,
					Offset:   0,
				}}
			},
			pieceLen:  64,
			numPieces: 16,
			wantErr:   true,
		},
		{
			name: "empty file",
			setup: func() []FileInfo {
				path := filepath.Join(tempDir, "empty")
				if err := os.WriteFile(path, []byte{}, 0644); err != nil {
					t.Fatalf("failed to create empty file: %v", err)
				}
				return []FileInfo{{
					FullPath: path,
					Length:   0,
					Offset:   0,
				}}
			},
			pieceLen:  64,
			numPieces: 1,
			wantErr:   false,
		},
		{
			name: "unreadable file",
			setup: func() []FileInfo {
				path := filepath.Join(tempDir, "unreadable")
				if err := os.WriteFile(path, []byte("test"), 0000); err != nil {
					t.Fatalf("failed to create unreadable file: %v", err)
				}
				return []FileInfo{{
					FullPath: path,
					Length:   4,
					Offset:   0,
				}}
			},
			pieceLen:  64,
			numPieces: 1,
			wantErr:   true,
			skipOnWin: true,
		},
	}

	zerolog.SetGlobalLevel(zerolog.Disabled)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipOnWin && runtime.GOOS == "windows" {
				t.Skip("skipping unreadable file test on Windows")
			}
			files := tt.setup()

			workers := 2

			for _, parallelRead := range []ParallelFileRead{ParallelFileReadEnabled, ParallelFileReadDisabled} {
				task, err := GeneratePieces(files, tt.pieceLen, parallelRead, workers)
				require.NoError(t, err)

				for !task.IsFinished() {
					time.Sleep(10 * time.Millisecond)
				}

				err = task.GetError()

				if (err != nil) != tt.wantErr {
					t.Errorf("GeneratePieces() parallelRead = %v, error = %v, wantErr %v", parallelRead, err, tt.wantErr)
				}
			}
		})
	}
}

// TestPieceHasher_RaceConditions tests concurrent access using a FLAC album scenario.
// FLAC albums are ideal for race testing because:
// - multiple small-to-medium files (12 tracks, 40MB each)
// - small piece size (64KB) creates more concurrent operations
func TestPieceHasher_RaceConditions(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	// use the multi-file album test case from TestPieceHasher_Concurrent
	// but run multiple hashers concurrently to stress test race conditions
	numFiles := 12
	fileSize := int64(40 << 20) // 40MB per track
	pieceLen := int64(1 << 16)  // 64KB pieces

	tempDir, err := os.MkdirTemp("", "hasher_race_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	files, expectedHashes := createTestFilesFast(t, numFiles, fileSize, pieceLen)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			task, err := GeneratePieces(files, pieceLen, ParallelFileReadEnabled, 4)
			require.NoError(t, err)
			for !task.IsFinished() {
				time.Sleep(10 * time.Millisecond)
			}

			if err := task.GetError(); err != nil {
				t.Errorf("GeneratePieces() error: %v", err)
			}

			verifyHashes(t, task.generatedHashes, concatHashes(expectedHashes))
		})
	}
	wg.Wait()

	for range 4 {
		wg.Go(func() {
			task, err := GeneratePieces(files, pieceLen, ParallelFileReadDisabled, 4)
			require.NoError(t, err)

			for !task.IsFinished() {
				time.Sleep(10 * time.Millisecond)
			}

			if err := task.GetError(); err != nil {
				t.Errorf("GeneratePieces() error: %v", err)
			}

			verifyHashes(t, task.generatedHashes, concatHashes(expectedHashes))
		})
	}
	wg.Wait()
}

func TestPieceHasher_ZeroWorkers(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	tempDir, err := os.MkdirTemp("", "hasher_zero_workers")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	files := []FileInfo{{
		FullPath: filepath.Join(tempDir, "test"),
		Length:   1 << 16,
		Offset:   0,
	}}

	// Create the actual test file
	filePath := files[0].FullPath
	fileSize := files[0].Length
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create test file %s: %v", filePath, err)
	}
	if err := f.Truncate(fileSize); err != nil {
		f.Close()
		t.Fatalf("failed to truncate test file %s: %v", filePath, err)
	}
	f.Close()

	for _, parallelRead := range []ParallelFileRead{ParallelFileReadEnabled, ParallelFileReadDisabled} {
		task, err := GeneratePieces(files, 1<<16, parallelRead, 0)
		require.NoError(t, err)

		for !task.IsFinished() {
			time.Sleep(10 * time.Millisecond)
		}

		if err := task.GetError(); err != nil {
			t.Errorf("GeneratePieces() error: %v", err)
			t.Errorf("GeneratePieces() with 0 workers should not return an error (should optimize or default to 1 worker), but got: %v", err)
		}
	}
}

func TestPieceHasher_CorruptedData(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	tempDir, err := os.MkdirTemp("", "hasher_corrupt_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	files, expectedHashes := createTestFilesFast(t, 1, 1<<16, 1<<16) // 1 file, 64KB

	// corrupt the file by modifying a byte
	corruptedPath := files[0].FullPath
	data, err := os.ReadFile(corruptedPath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	data[0] ^= 0xFF // flip bits of the first byte
	if err := os.WriteFile(corruptedPath, data, 0644); err != nil {
		t.Fatalf("failed to write corrupted file: %v", err)
	}

	for _, parallelRead := range []ParallelFileRead{ParallelFileReadEnabled, ParallelFileReadDisabled} {
		task, err := GeneratePieces(files, 1<<16, parallelRead, 1)
		require.NoError(t, err)

		for !task.IsFinished() {
			time.Sleep(10 * time.Millisecond)
		}

		if err := task.GetError(); err != nil {
			t.Fatalf("GeneratePieces() error: %v", err)
		}

		if bytes.Equal(task.generatedHashes[0:HashSize], expectedHashes[0]) {
			t.Errorf("expected hash mismatch due to corrupted data, but hashes matched")
		}
	}
}

// TestPieceHasher_BoundaryConditions tests scenarios where file boundaries
// align exactly with piece boundaries.
func TestPieceHasher_BoundaryConditions(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	tempDir, err := os.MkdirTemp("", "hasher_boundary_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	pieceLen := int64(1 << 16) // 64 KB

	tests := []struct {
		name      string
		fileSizes []int64 // Sizes of consecutive files
	}{
		{
			name:      "Exact Piece Boundary",
			fileSizes: []int64{pieceLen, pieceLen * 2, pieceLen}, // Files end exactly on piece boundaries
		},
		{
			name:      "Mid-Piece Boundary",
			fileSizes: []int64{pieceLen / 2, pieceLen, pieceLen * 2, pieceLen / 3}, // Boundaries within pieces
		},
		{
			name:      "Multiple Small Files within One Piece",
			fileSizes: []int64{pieceLen / 4, pieceLen / 4, pieceLen / 4, pieceLen / 4},
		},
		{
			name:      "Large File Followed By Small",
			fileSizes: []int64{pieceLen * 3, pieceLen / 2},
		},
		{
			name:      "Small File Followed By Large",
			fileSizes: []int64{pieceLen / 2, pieceLen * 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files, expectedHashes := createTestFilesWithPattern(t, tempDir, tt.fileSizes, pieceLen)

			var totalSize int64
			for _, size := range tt.fileSizes {
				totalSize += size
			}

			for _, parallelRead := range []ParallelFileRead{ParallelFileReadEnabled, ParallelFileReadDisabled} {
				// Test with 1 and multiple workers
				workerCounts := []int{1, 4}
				for _, workers := range workerCounts {
					t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
						// Need to create a new hasher instance for each run if pieces are modified in place
						task, err := GeneratePieces(files, pieceLen, parallelRead, workers)
						require.NoError(t, err)

						for !task.IsFinished() {
							time.Sleep(10 * time.Millisecond)
						}

						if err := task.GetError(); err != nil {
							t.Fatalf("GeneratePieces failed parallelRead: %d, workers: %d, err: %v",
								parallelRead, workers, err)
						}

						verifyHashes(t, task.generatedHashes, concatHashes(expectedHashes))
					})
				}
			}
			// Clean up files for this subtest run
			for _, f := range files {
				_ = os.Remove(f.FullPath)
			}
		})
	}
}
