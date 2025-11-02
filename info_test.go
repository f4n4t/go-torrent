package torrent_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/f4n4t/go-dtree"
	"github.com/f4n4t/go-torrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type file struct {
	name    string
	content []byte
}

func createTestDir(files ...file) (string, func(), error) {
	tempDir, err := os.MkdirTemp("", "go-torrent")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	testDir := filepath.Join(tempDir, "testDir")
	if err := os.Mkdir(testDir, 0755); err != nil {
		return "", cleanup, err
	}

	for _, file := range files {
		f, err := os.Create(filepath.Join(testDir, file.name))
		if err != nil {
			return "", cleanup, err
		}

		if _, err := f.Write(file.content); err != nil {
			return "", cleanup, err
		}
	}

	return testDir, cleanup, nil
}

func removeDate(input []byte) []byte {
	// Suche nach "13:creation datei"
	if idx := bytes.Index(input, []byte("13:creation datei")); idx != -1 {
		return append(input[:idx], input[idx+28:]...)
	}
	return input
}

func Test_InfoCreate(t *testing.T) {
	t.Run("SingleFile", func(t *testing.T) {
		testFile := file{
			name:    "test1.txt",
			content: []byte("Hello\nWorld\n"),
		}

		dirName, cleanup, err := createTestDir(testFile)
		require.NoError(t, err)
		defer cleanup()

		torrentService := torrent.NewServiceBuilder().WithCreatedBy("go-torrent").Build()

		rootNode, err := dtree.Collect(filepath.Join(dirName, testFile.name))
		require.NoError(t, err)

		got, err := torrentService.Create(rootNode)
		assert.NoError(t, err)

		expectedTorrent, err := os.ReadFile("./testdata/testFile.torrent")
		assert.NoError(t, err)

		got = removeDate(got)
		expectedTorrent = removeDate(expectedTorrent)

		assert.Equal(t, got, expectedTorrent)

	})

	t.Run("MultiFile", func(t *testing.T) {
		var files []file
		files = append(files, file{
			name:    "test1.txt",
			content: []byte("Hello\nWorld\n"),
		})
		files = append(files, file{
			name:    "test2.txt",
			content: []byte("This\nIs\nMe!\n"),
		})

		dirName, cleanup, err := createTestDir(files...)
		require.NoError(t, err)
		defer cleanup()

		torrentService := torrent.NewServiceBuilder().WithCreatedBy("go-torrent").Build()

		rootNode, err := dtree.Collect(dirName)
		require.NoError(t, err)

		got, err := torrentService.Create(rootNode)
		assert.NoError(t, err)

		expectedTorrent, err := os.ReadFile("./testdata/testDir.torrent")
		assert.NoError(t, err)

		got = removeDate(got)
		expectedTorrent = removeDate(expectedTorrent)

		assert.Equal(t, got, expectedTorrent)
	})

	t.Run("CheckCancellationWithParallelRead", func(t *testing.T) {
		testFile := file{
			name:    "test1.txt",
			content: []byte("Hello\nWorld\n"),
		}

		dirName, cleanup, err := createTestDir(testFile)
		require.NoError(t, err)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())

		torrentService := torrent.NewServiceBuilder().
			WithParallelFileRead(torrent.ParallelFileReadEnabled).WithCreatedBy("go-torrent").WithContext(ctx).Build()

		rootNode, err := dtree.Collect(filepath.Join(dirName, testFile.name))
		require.NoError(t, err)

		cancel()

		_, err = torrentService.Create(rootNode)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("CheckCancellationWithoutParallelRead", func(t *testing.T) {
		testFile := file{
			name:    "test1.txt",
			content: []byte("Hello\nWorld\n"),
		}

		dirName, cleanup, err := createTestDir(testFile)
		require.NoError(t, err)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())

		torrentService := torrent.NewServiceBuilder().
			WithParallelFileRead(torrent.ParallelFileReadEnabled).WithCreatedBy("go-torrent").WithContext(ctx).Build()

		rootNode, err := dtree.Collect(filepath.Join(dirName, testFile.name))
		require.NoError(t, err)

		cancel()

		_, err = torrentService.Create(rootNode)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestMarshalInfo(t *testing.T) {
	var info torrent.Info
	b, err := bencode.Marshal(info)
	assert.NoError(t, err)
	assert.EqualValues(t, "d4:name0:12:piece lengthi0e6:pieces0:e", string(b))
}
