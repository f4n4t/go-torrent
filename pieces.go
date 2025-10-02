package torrent

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime"
	"sync"
)

const (
	verifyWorker workerType = iota
	generateWorker
)

var (
	// ErrPieceHashMismatch indicates a mismatch between the computed and expected hash of a piece during validation.
	ErrPieceHashMismatch = errors.New("piece hash mismatch")

	// ErrTaskStillRunning indicates that an attempted operation on a task failed because the task is still running.
	ErrTaskStillRunning = errors.New("task is still running")
)

// hashPool is a pool of SHA-1 hashers to reduce the overhead of creating new hashers.
var hashPool = sync.Pool{
	New: func() any {
		return sha1.New()
	},
}

type workerType int

// piece represents a single data piece to be sent to the result channel.
// each piece contains its index, length, and an SHA-1 hash of its content.
type piece struct {
	number int
	len    int64
	hash   [HashSize]byte
}

// PiecesProcessingTask is used to track the progress of the piece generation process.
type PiecesProcessingTask struct {
	// mutex is used to synchronize access to the task's fields.
	mutex           *sync.Mutex
	ctx             context.Context
	bytesHashed     int64
	totalSize       int64
	totalPieces     int
	piecesProcessed int
	isFinished      bool
	numWorkers      int
	generatedHashes []byte
	expectedHashes  []byte
	error           error
	workerType      workerType
}

// newGenerateTask initializes a PiecesProcessingTask to track progress during the piece generation process.
func newGenerateTask(ctx context.Context, totalPieces int, totalSize int64) *PiecesProcessingTask {
	return &PiecesProcessingTask{
		mutex:       &sync.Mutex{},
		totalPieces: totalPieces,
		totalSize:   totalSize,
		// Final byte slice containing all hashes
		generatedHashes: make([]byte, totalPieces*HashSize),
		workerType:      generateWorker,
		ctx:             ctx,
	}
}

// newVerifyTask creates and initializes a new PiecesProcessingTask for verifying pieces with the given parameters.
func newVerifyTask(ctx context.Context, totalPieces int, totalSize int64, expectedHashes []byte) *PiecesProcessingTask {
	return &PiecesProcessingTask{
		mutex:          &sync.Mutex{},
		totalPieces:    totalPieces,
		totalSize:      totalSize,
		expectedHashes: expectedHashes,
		workerType:     verifyWorker,
		ctx:            ctx,
	}
}

// GetProgress returns the progress of the piece generation process.
func (t *PiecesProcessingTask) GetProgress() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// In case GetProgress is called before calculating piece count
	if t.totalPieces == 0 {
		return 0
	}

	return t.piecesProcessed * 100 / t.totalPieces
}

// GetPieces returns the generated piece hashes and any error associated with the task, ensuring thread-safe access.
func (t *PiecesProcessingTask) GetPieces() ([]byte, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.workerType == verifyWorker {
		return nil, errors.New("cannot get pieces from verify task")
	}
	if !t.isFinished {
		return nil, ErrTaskStillRunning
	}
	return t.generatedHashes, t.error
}

// GetError retrieves the error associated with the task in a thread-safe manner.
func (t *PiecesProcessingTask) GetError() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.error
}

// GetBytesHashed returns the number of bytes hashed so far.
func (t *PiecesProcessingTask) GetBytesHashed() int64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.bytesHashed
}

// GetNumWorkers returns the number of total workers.
func (t *PiecesProcessingTask) GetNumWorkers() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.numWorkers
}

// updateResult updates the generatedHashes byte slice with the hash of the given piece.
// It also increments the piecesProcessed and bytesHashed counters.
func (t *PiecesProcessingTask) updateResult(p piece) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	copy(t.generatedHashes[p.number*HashSize:], p.hash[:])
	t.piecesProcessed++
	t.bytesHashed += p.len
}

// updateResultVerify increments the piecesProcessed and bytesHashed counters.
func (t *PiecesProcessingTask) updateResultVerify(hashed int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.piecesProcessed++
	t.bytesHashed += hashed
}

// IsCanceled checks if the task has been canceled by evaluating the context's Done channel.
func (t *PiecesProcessingTask) IsCanceled() bool {
	select {
	case <-t.ctx.Done():
		return true
	default:
		return false
	}
}

// IsFinished checks if the piece generation process has been completed. It is thread-safe and uses a mutex for locking.
func (t *PiecesProcessingTask) IsFinished() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.isFinished
}

// setFinished marks the task as finished by setting the isFinished flag to true in a thread-safe manner.
func (t *PiecesProcessingTask) setFinished() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.isFinished = true
}

// setError sets the error field in a thread-safe manner using a mutex lock.
func (t *PiecesProcessingTask) setError(err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.error = err
}

// setContext assigns a new context to the task in a thread-safe manner using a mutex lock.
func (t *PiecesProcessingTask) setContext(ctx context.Context) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.ctx = ctx
}

// workerPool is a pool of workers that process jobs concurrently.
type workerPool struct {
	workers       int
	jobs          chan func() piece
	jobsVerify    chan func() int64
	results       chan piece
	resultsVerify chan int64
	// errors is a channel used to propagate errors encountered during job execution.
	// execution will stop as soon as an error is encountered.
	errors     chan error
	piecesTask *PiecesProcessingTask
}

// newWorkerPool creates a new worker pool with the specified number of workers.
func newWorkerPool(workers int, wt workerType) workerPool {
	switch wt {
	case verifyWorker:
		return workerPool{
			workers:       workers,
			jobsVerify:    make(chan func() int64, workers),
			resultsVerify: make(chan int64, workers),
			errors:        make(chan error),
		}
	case generateWorker:
		return workerPool{
			workers: workers,
			jobs:    make(chan func() piece, workers),
			results: make(chan piece, workers),
			errors:  make(chan error),
		}
	default:
		panic("invalid worker type")
	}
}

// start starts the worker pool by creating the specified number of workers.
func (wp *workerPool) start() {
	for range wp.workers {
		go func() {
			for job := range wp.jobs {
				wp.results <- job()
			}
		}()
	}
}

// startVerify initializes worker goroutines to process verification jobs concurrently from the jobsVerify channel.
func (wp *workerPool) startVerify() {
	for range wp.workers {
		go func() {
			for job := range wp.jobsVerify {
				wp.resultsVerify <- job()
			}
		}()
	}
}

// collectResults collects the results from the worker pool in a non-blocking way.
func (wp *workerPool) collectResults() {
	// we are finished whether we have an error or not
	defer wp.piecesTask.setFinished()

	for wp.piecesTask.GetBytesHashed() < wp.piecesTask.totalSize {
		select {
		case p, ok := <-wp.results:
			if !ok {
				return
			}
			wp.piecesTask.updateResult(p)

		case err, ok := <-wp.errors:
			if !ok {
				return
			}
			wp.piecesTask.setError(err)
			return

		case <-wp.piecesTask.ctx.Done():
			// operation has been canceled
			return
		}
	}
}

// collectVerifyResults processes verification results non-blocking until the total length is checked
// or canceled.
func (wp *workerPool) collectVerifyResults() {
	// we are finished whether we have an error or not
	defer wp.piecesTask.setFinished()

	for wp.piecesTask.GetBytesHashed() < wp.piecesTask.totalSize {
		select {
		case n, ok := <-wp.resultsVerify:
			if !ok {
				return
			}
			wp.piecesTask.updateResultVerify(n)

		case err, ok := <-wp.errors:
			if !ok {
				return
			}
			wp.piecesTask.setError(err)
			return

		case <-wp.piecesTask.ctx.Done():
			// operation has been canceled
			return
		}
	}
}

// buildJobs reads from the pipe and sends jobs to the worker pool.
func buildJobs(files Files, pieceLength int64, jobs chan<- func() piece, errChan chan<- error) {
	defer close(jobs)

	bufferPool := &sync.Pool{
		New: func() any {
			buf := make([]byte, pieceLength)
			return buf
		},
	}

	pipeReader := setupPipeReaderWriter(files, pieceLength, errChan)

	var pieceIdx int

	// Read from the pipe and send jobs to workers
	for {
		buffer := bufferPool.Get().([]byte)
		putBuffer := func() { bufferPool.Put(buffer) }
		n, err := io.ReadFull(pipeReader, buffer)
		if n > 0 {
			sendJob(jobs, pieceIdx, buffer[:n], putBuffer)
			pieceIdx++
		}
		if err != nil {
			// Handle the end of stream or unexpected errors
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				errChan <- fmt.Errorf("read from pipe: %w", err)
			}
			break
		}
	}
}

// setupPipeReaderWriter sets up a pipe to stream data from files to the worker pool.
func setupPipeReaderWriter(files Files, pieceLength int64, errChan chan<- error) *io.PipeReader {
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		buffer := make([]byte, pieceLength)

		// Process each file and write to pipe
		for _, fileInfo := range files {
			if err := processFile(fileInfo, pipeWriter, buffer); err != nil {
				// Report error and close writer
				_ = pipeWriter.CloseWithError(err)
				errChan <- fmt.Errorf("process file %s: %w", fileInfo.FullPath, err)
				return
			}
		}
	}()

	return pipeReader
}

// processFile reads the file and writes its content to the pipe.
func processFile(fileInfo FileInfo, pipeWriter io.Writer, buf []byte) error {
	file, err := os.Open(fileInfo.FullPath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", fileInfo.FullPath, err)
	}
	defer file.Close()

	n, err := io.CopyBuffer(pipeWriter, file, buf)
	if n != fileInfo.Length {
		return fmt.Errorf("copy %s to pipe: expected %d bytes, got %d",
			fileInfo.FullPath, fileInfo.Length, n)
	}

	return err
}

// sendJob creates and sends a job to the worker pool.
func sendJob(jobs chan<- func() piece, pieceIdx int, data []byte, putBuffer func()) {
	// Create a job for processing the current piece of data
	jobs <- func() piece {
		hasher := hashPool.Get().(hash.Hash)
		defer hashPool.Put(hasher)
		defer putBuffer()

		hasher.Reset()
		hasher.Write(data)

		return piece{
			number: pieceIdx,
			len:    int64(len(data)),
			hash:   [HashSize]byte(hasher.Sum(nil)),
		}
	}
}

// verifyJobs verifies the integrity of pieces in a torrent by comparing computed hashes with given piece hashes.
func verifyJobs(files Files, pieceLength int64, expectedHashes []byte, jobs chan<- func() int64, errChan chan<- error) {
	defer close(jobs)

	bufferPool := &sync.Pool{
		New: func() any {
			buf := make([]byte, pieceLength)
			return buf
		},
	}

	pipeReader := setupPipeReaderWriter(files, pieceLength, errChan)

	var pieceIdx int

	// Read from the pipe and send jobs to workers
	for {
		buffer := bufferPool.Get().([]byte)
		putBuffer := func() { bufferPool.Put(buffer) }
		n, err := io.ReadFull(pipeReader, buffer)
		if n > 0 {
			sendVerifyJob(jobs, pieceIdx, buffer[:n], putBuffer, expectedHashes, errChan)
			pieceIdx++
		}
		if err != nil {
			// Ignore the end of the stream or unexpected errors
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				errChan <- fmt.Errorf("read from pipe: %w", err)
			}
			break
		}
	}
}

// sendVerifyJob creates and sends a job to the worker pool for verifying a piece.
func sendVerifyJob(jobs chan<- func() int64, pieceIndex int, data []byte, putBuffer func(), expectedHashes []byte, errChan chan<- error) {
	// Create a job for processing the current piece of data
	jobs <- func() int64 {
		hasher := hashPool.Get().(hash.Hash)
		defer hashPool.Put(hasher)
		defer putBuffer()

		hasher.Reset()
		hasher.Write(data)

		actualHash := hasher.Sum(nil)
		expectedHash := expectedHashes[pieceIndex*HashSize : (pieceIndex+1)*HashSize]

		if !bytes.Equal(actualHash, expectedHash) {
			errChan <- fmt.Errorf("%w: piece %d", ErrPieceHashMismatch, pieceIndex)
		}

		return int64(len(data))
	}
}

// Optimized parallel processing implementation
// I changed my old implementation to https://github.com/autobrr/mkbrr/blob/main/internal/torrent/hasher.go

// parallelProcessor provides optimized parallel processing with progress tracking
type parallelProcessor struct {
	pieceLength int64
	files       Files
	bufferPool  *sync.Pool
	task        *PiecesProcessingTask
}

// fileReader tracks open file state to avoid repeated seeks
type fileReader struct {
	*os.File
	position int64
	length   int64
}

// performanceParams defines configuration parameters for optimizing file processing performance, including
// read size and worker count.
type performanceParams struct {
	readSize        int
	numWorkers      int
	useParallelRead bool
}

// optimalPerformanceParams calculates optimal read size and worker count based on file characteristics and processing needs.
func optimalPerformanceParams(files Files, totalPieces int, totalSize int64, parallelRead ParallelFileRead) performanceParams {
	if len(files) == 0 || totalPieces == 0 {
		return performanceParams{}
	}

	parallelFileRead := parallelRead.shouldReadParallel(files[0].FullPath)

	if totalPieces == 1 {
		return performanceParams{
			readSize:        1 << 20,
			numWorkers:      1,
			useParallelRead: parallelFileRead,
		}
	}

	avgFileSize := totalSize / int64(len(files))
	maxProcs := runtime.GOMAXPROCS(0)

	// optimize buffer size and worker count based on file characteristics
	switch {
	case len(files) == 1:
		if totalSize < 1<<20 {
			return performanceParams{
				readSize:        64 << 10, // 64 KiB for very small files
				numWorkers:      1,
				useParallelRead: parallelFileRead,
			}
		} else if totalSize < 1<<30 { // < 1 GiB
			return performanceParams{
				readSize:        4 << 20, // 4 MiB
				numWorkers:      maxProcs,
				useParallelRead: parallelFileRead,
			}
		} else {
			return performanceParams{
				readSize:        8 << 20,      // 8 MiB for large files
				numWorkers:      maxProcs * 2, // over-subscription for better I/O utilization
				useParallelRead: parallelFileRead,
			}
		}

	case avgFileSize < 1<<20: // avg < 1 MiB
		return performanceParams{
			readSize:        256 << 10, // 256 KiB
			numWorkers:      maxProcs,
			useParallelRead: parallelFileRead,
		}

	case avgFileSize < 10<<20: // avg < 10 MiB
		return performanceParams{
			readSize:        1 << 20, // 1 MiB
			numWorkers:      maxProcs,
			useParallelRead: parallelFileRead,
		}

	case avgFileSize < 1<<30: // avg < 1 GiB
		return performanceParams{
			readSize:        4 << 20, // 4 MiB
			numWorkers:      maxProcs * 2,
			useParallelRead: parallelFileRead,
		}

	default: // avg >= 1 GiB
		return performanceParams{
			readSize:        8 << 20, // 8 MiB
			numWorkers:      maxProcs * 2,
			useParallelRead: parallelFileRead,
		}
	}
}

// createProcessor creates a new processor instance with initialized buffer pool
func createProcessor(files Files, pieceLength int64, task *PiecesProcessingTask, perfParams performanceParams) *parallelProcessor {
	// initialize buffer pool
	bufferPool := &sync.Pool{
		New: func() any {
			buf := make([]byte, perfParams.readSize)
			return &buf
		},
	}

	return &parallelProcessor{
		pieceLength: pieceLength,
		task:        task,
		files:       files,
		bufferPool:  bufferPool,
	}
}

// processInBackground runs the processing algorithm in the background
func (p *parallelProcessor) processInBackground(numWorkers int) {
	defer func() {
		// set as finished whether we have an error or not
		p.task.setFinished()
	}()

	var (
		piecesPerWorker = (p.task.totalPieces + numWorkers - 1) / numWorkers
		wg              sync.WaitGroup
		errorOnce       sync.Once
	)

	// add child context to cancel all running go routines
	ctx, cancel := context.WithCancel(p.task.ctx)
	defer cancel()

	p.task.setContext(ctx)

	// spawn worker goroutines to process piece ranges in parallel
	for i := range numWorkers {
		start := i * piecesPerWorker
		end := min(start+piecesPerWorker, p.task.totalPieces)

		wg.Go(func() {
			if err := p.processPieceRange(start, end); err != nil {
				errorOnce.Do(func() {
					p.task.setError(err)
					cancel()
				})
			}
		})
	}

	wg.Wait()
}

// processPieceRange processes a specific range of pieces assigned to a worker
func (p *parallelProcessor) processPieceRange(startPiece, endPiece int) error {
	// reuse buffer and hasher from the pool to minimize allocations
	bufferPtr := p.bufferPool.Get().(*[]byte)
	buf := *bufferPtr
	defer p.bufferPool.Put(bufferPtr)

	hasher := hashPool.Get().(hash.Hash)
	defer hashPool.Put(hasher)

	// track open file handles to avoid reopening the same file
	// files will be closed in hashPieceData
	fileMap := make(map[string]*fileReader)

	defer func() {
		// close files that are still opened
		for _, r := range fileMap {
			_ = r.Close()
		}
	}()

	for pieceIndex := startPiece; pieceIndex < endPiece; pieceIndex++ {
		if err := p.checkCancellation(); err != nil {
			return err
		}

		pieceOffset := int64(pieceIndex) * p.pieceLength
		pieceLength := p.pieceLength

		// handle the last piece which may be shorter than others
		if pieceIndex == p.task.totalPieces-1 {
			remaining := p.task.totalSize - pieceOffset
			if remaining < pieceLength {
				pieceLength = remaining
			}
		}

		hasher.Reset()

		if err := p.hashPieceData(pieceOffset, pieceLength, hasher, buf, fileMap); err != nil {
			return err
		}

		if err := p.processPiece(pieceIndex, pieceLength, hasher); err != nil {
			return err
		}
	}

	return nil
}

// hashPieceData performs the actual hashing of piece data from files
func (p *parallelProcessor) hashPieceData(pieceOffset, pieceLength int64,
	hasher hash.Hash, buf []byte, fileMap map[string]*fileReader) error {
	remainingPiece := pieceLength

	for _, file := range p.files {
		if err := p.checkCancellation(); err != nil {
			return err
		}

		// skip files that don't contain data for this piece
		if pieceOffset >= file.Offset+file.Length {
			continue
		}
		if remainingPiece <= 0 {
			break
		}

		// calculate read boundaries within the current file
		readStart := max(pieceOffset-file.Offset, 0)
		readLength := min(file.Length-readStart, remainingPiece)

		// reuse or create a new file reader
		reader, ok := fileMap[file.FullPath]
		if !ok {
			f, err := os.Open(file.FullPath)
			if err != nil {
				return fmt.Errorf("open file %s: %w", file.FullPath, err)
			}
			reader = &fileReader{
				File:     f,
				position: 0,
				length:   file.Length,
			}
			fileMap[file.FullPath] = reader
		}

		// ensure the correct file position before reading
		if reader.position != readStart {
			if _, err := reader.Seek(readStart, io.SeekStart); err != nil {
				return fmt.Errorf("seek file %s: %w", file.FullPath, err)
			}
			reader.position = readStart
		}

		// read file data in chunks to avoid large memory allocations
		remaining := readLength
		for remaining > 0 {
			if err := p.checkCancellation(); err != nil {
				return err
			}

			toRead := min(int(remaining), len(buf))

			read, err := io.ReadFull(reader, buf[:toRead])
			if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("read file %s: %w", file.FullPath, err)
			}

			hasher.Write(buf[:read])
			remaining -= int64(read)
			remainingPiece -= int64(read)
			reader.position += int64(read)
			pieceOffset += int64(read)
		}

		// check if we can close the file
		if pieceOffset >= file.Offset+file.Length {
			if _, ok := fileMap[file.FullPath]; ok {
				_ = fileMap[file.FullPath].Close()
				delete(fileMap, file.FullPath)
			}
		}
	}

	return nil
}

// processPiece handles the generation or verification of a specific piece based on the worker type of the task.
func (p *parallelProcessor) processPiece(pieceIndex int, pieceLength int64, hasher hash.Hash) error {
	switch p.task.workerType {
	case generateWorker:
		// store the piece hash
		p.task.updateResult(
			piece{
				number: pieceIndex,
				len:    pieceLength,
				hash:   [HashSize]byte(hasher.Sum(nil)),
			},
		)

	case verifyWorker:
		// verify the piece hash against the expected hash
		computedHash := hasher.Sum(nil)
		expectedHashStart := pieceIndex * HashSize
		expectedHash := p.task.expectedHashes[expectedHashStart : expectedHashStart+HashSize]

		if !bytes.Equal(computedHash, expectedHash) {
			return fmt.Errorf("%w: piece %d", ErrPieceHashMismatch, pieceIndex)
		}

		// update progress (for verifying we only track progress, not store hashes)
		p.task.updateResultVerify(pieceLength)
	}

	return nil
}

// checkCancellation checks if the processing context has been canceled and returns the corresponding error if so.
func (p *parallelProcessor) checkCancellation() error {
	select {
	case <-p.task.ctx.Done():
		return p.task.ctx.Err()
	default:
		return nil
	}
}

// GeneratePieces is a convenience wrapper for GeneratePiecesWithContext using context.Background().
// See GeneratePiecesWithContext for detailed documentation and usage examples.
func GeneratePieces(files Files, pieceLength int64, parallelRead ParallelFileRead, hashThreads int) (*PiecesProcessingTask, error) {
	return GeneratePiecesWithContext(context.Background(), files, pieceLength, parallelRead, hashThreads)
}

// GeneratePiecesWithContext starts asynchronous generation of SHA-1 hashes for torrent pieces.
//
// This function starts the hash generation process IMMEDIATELY in the background and returns directly.
// The caller is responsible for monitoring the returned PiecesProcessingTask to determine
// when the generation is complete and to retrieve the generated hashes.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - files: Files to generate hashes from (Files slice)
//   - pieceLength: Length of each piece in bytes
//   - parallelRead: Configuration for parallel file reading
//   - hashThreads: Number of hash workers (0 = automatic optimization)
//
// Returns:
//   - *PiecesProcessingTask: Task handle for progress tracking and hash retrieval
//   - error: Error during initialization (not during processing)
//
// IMPORTANT: The task is started IMMEDIATELY!
// The caller MUST:
//  1. Check task.IsFinished() regularly, or
//  2. Listen for context cancellation
//  3. Use task.GetPieces() to retrieve generated hashes when IsFinished() returns true
//  4. Check task.GetError() for any processing errors
//
// Usage example:
//
//	task, err := GeneratePiecesWithContext(ctx, files, 32768, parallelRead, 0)
//	if err != nil {
//	    return nil, fmt.Errorf("failed to start hash generation: %w", err)
//	}
//
//	// Wait until task is finished
//	for !task.IsFinished() {
//	    fmt.Printf("Progress: %d%% (%s hashed)\n",
//	        task.GetProgress(),
//	        utils.Bytes(int(task.GetBytesHashed())))
//	    time.Sleep(100 * time.Millisecond)
//	}
//
//	// Retrieve generated hashes
//	hashes, err := task.GetPieces()
//	if err != nil {
//	    return nil, fmt.Errorf("hash generation failed: %w", err)
//	}
//
//	fmt.Printf("Generated %d piece hashes (%d bytes)\n",
//	    len(hashes)/HashSize, len(hashes))
//	return hashes, nil
//
// Error handling:
//   - Initialization errors are returned immediately
//   - Processing errors are available via task.GetError()
//   - ErrTaskStillRunning is returned by GetPieces() if called before completion
//   - Context cancellation stops processing cleanly
func GeneratePiecesWithContext(ctx context.Context, files Files, pieceLength int64, parallelRead ParallelFileRead, hashThreads int) (*PiecesProcessingTask, error) {
	totalPieces := files.PieceCount(pieceLength)
	totalSize := files.TotalLength()

	task := newGenerateTask(ctx, totalPieces, totalSize)

	if totalPieces == 0 {
		// nothing to process
		task.setFinished()
		return task, nil
	}

	perfParams := optimalPerformanceParams(files, totalPieces, totalSize, parallelRead)

	if hashThreads > 0 {
		// Prioritize user input
		perfParams.numWorkers = hashThreads
	} else if perfParams.numWorkers <= 0 {
		// should never occur, but force at least 1 worker
		perfParams.numWorkers = 1
	} else if perfParams.numWorkers >= 64 {
		// limit to 8 workers on cpus with lots of cores could be a shared seedbox
		perfParams.numWorkers = 8
	}

	task.numWorkers = perfParams.numWorkers

	if perfParams.useParallelRead {
		processor := createProcessor(files, pieceLength, task, perfParams)

		// Start the hashing process in the background
		go processor.processInBackground(perfParams.numWorkers)
	} else {
		wp := newWorkerPool(perfParams.numWorkers, generateWorker)
		wp.piecesTask = task
		wp.start()

		go buildJobs(files, pieceLength, wp.jobs, wp.errors)
		go wp.collectResults()
	}

	return task, nil
}

// VerifyPieces is a convenience wrapper for VerifyPiecesWithContext using context.Background().
// See VerifyPiecesWithContext for detailed documentation and usage examples.
func VerifyPieces(files Files, pieceLength int64, expectedHashes []byte,
	parallelRead ParallelFileRead, hashThreads int) (*PiecesProcessingTask, error) {
	return VerifyPiecesWithContext(context.Background(), files, pieceLength, expectedHashes, parallelRead, hashThreads)
}

// VerifyPiecesWithContext starts asynchronous verification of torrent piece hashes against expected hashes.
//
// This function starts the verification process IMMEDIATELY in the background and returns directly.
// The caller is responsible for monitoring the returned PiecesProcessingTask to determine
// when the verification is complete.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - files: Files to be verified (Files slice)
//   - pieceLength: Length of each piece in bytes
//   - expectedHashes: Expected SHA-1 hashes (20 bytes per piece)
//   - parallelRead: Configuration for parallel file reading
//   - hashThreads: Number of hash workers (0 = automatic optimization)
//
// Returns:
//   - *PiecesProcessingTask: Task handle for progress tracking and result querying
//   - error: Error during initialization (not during processing)
//
// IMPORTANT: The task is started IMMEDIATELY!
// The caller MUST:
//  1. Check task.IsFinished() regularly, or
//  2. Listen for context cancellation
//  3. Check task.GetError() when IsFinished() returns true
//
// Usage example:
//
//	task, err := VerifyPiecesWithContext(ctx, files, 32768, expectedHashes, parallelRead, 0)
//	if err != nil {
//	    return fmt.Errorf("failed to start verification: %w", err)
//	}
//
//	// Wait until task is finished
//	for !task.IsFinished() {
//	    fmt.Printf("Progress: %d%%\n", task.GetProgress())
//	    time.Sleep(100 * time.Millisecond)
//	}
//
//	// Check result
//	if err := task.GetError(); err != nil {
//	    if errors.Is(err, ErrPieceHashMismatch) {
//	        return fmt.Errorf("hash verification failed: %w", err)
//	    }
//	    return fmt.Errorf("verification error: %w", err)
//	}
//
//	fmt.Println("All pieces verified successfully!")
//
// Error handling:
//   - Initialization errors are returned immediately
//   - Processing errors are available via task.GetError()
//   - ErrPieceHashMismatch is set on hash mismatches
//   - Context cancellation stops processing cleanly
func VerifyPiecesWithContext(ctx context.Context, files Files, pieceLength int64, expectedHashes []byte,
	parallelRead ParallelFileRead, hashThreads int) (*PiecesProcessingTask, error) {
	totalPieces := files.PieceCount(pieceLength)
	totalSize := files.TotalLength()

	// Validate expected hashes length
	if len(expectedHashes) != totalPieces*HashSize {
		return nil, fmt.Errorf("expected hashes length mismatch: got %d, expected %d",
			len(expectedHashes), totalPieces*HashSize)
	}

	task := newVerifyTask(ctx, totalPieces, totalSize, expectedHashes)

	if totalPieces == 0 {
		// nothing to process
		task.setFinished()
		return task, nil
	}

	perfParams := optimalPerformanceParams(files, totalPieces, totalSize, parallelRead)

	if hashThreads > 0 {
		// Prioritize user input
		perfParams.numWorkers = hashThreads
	} else if perfParams.numWorkers <= 0 {
		// should never occur
		perfParams.numWorkers = 1
	} else if perfParams.numWorkers >= 64 {
		// limit to 8 workers on cpus with lots of cores could be a shared seedbox
		perfParams.numWorkers = 8
	}

	task.numWorkers = perfParams.numWorkers

	if perfParams.useParallelRead {
		processor := createProcessor(files, pieceLength, task, perfParams)

		// Start the verification process in the background
		go processor.processInBackground(perfParams.numWorkers)
	} else {
		wp := newWorkerPool(perfParams.numWorkers, verifyWorker)
		wp.piecesTask = task
		wp.startVerify()

		go verifyJobs(files, pieceLength, expectedHashes, wp.jobsVerify, wp.errors)
		go wp.collectVerifyResults()
	}

	return task, nil
}
