package main

import "bytes"
import "os"
import "flag"
import "fmt"
import "time"
import "sync"

var parallism = flag.Int("p", 1, "Parallism count.")

var metrics = NewIoMetrics(1)

var (
	kMaxWriteBatchLen = 256 * 1024
)

type DumbStore struct {
	// Pending channel which buffers incoming requests
	p chan *WriteCtx
	flusher *DataFlusher
	stop bool
	flushTick <- chan time.Time
}

type WriteResult struct {
	err error
	offset int64  // File offset which the data written to
}

type WriteCtx struct {
	data []byte
	notify chan *WriteResult
}

func NewDumbStore(fpath string) (s *DumbStore, err error) {
	store := &DumbStore {}
	store.p = make(chan *WriteCtx)
	var flusher *DataFlusher
  if flusher, err = NewDataFlusher(fpath); err != nil {
    return
	}
	store.flusher = flusher
	store.stop = false
	store.flushTick = time.Tick(100 * time.Millisecond)
	go store.Persist()
	return store, nil
}

func (s *DumbStore) Write(data []byte) (int64, error) {
	start := time.Now()
	defer func() {
		metrics.Update(int64(len(data)), time.Now().Sub(start).Milliseconds())
	}()

	if len(data) == 0 {
		return s.flusher.WriteOffset(), nil
	}
	ctx := &WriteCtx{data, make(chan *WriteResult, 1)}
	s.p <- ctx
	res := <- ctx.notify
	return res.offset, res.err
}

func (s *DumbStore) Persist() {
	for !s.stop {
		select {
		case ctx, ok := <- s.p:
			if !ok { break }
			s.flusher.Push(ctx)
		case _ = <- s.flushTick:
			s.flusher.Flush()
		default:
			break
		}
	}
}

func (s *DumbStore) Stop() {
	s.stop = true
}

// ----------------------------------------------------------------------------------------------------

type DataFlusher struct {
	dataLen int               // Total data size in flying_tasks
	flyingTasks []*WriteCtx   // Unwritten tasks
	f *os.File
	writeOffset int64
	mu *sync.Mutex
}

func NewDataFlusher(fpath string) (flusher *DataFlusher, err error) {
	var f *os.File
	if f, err = os.OpenFile(fpath, os.O_RDWR | os.O_CREATE, 0755); err != nil {
		return nil, err
	}
	flusher = &DataFlusher{0, nil, f, 0, &sync.Mutex{}}
	if fi, err := f.Stat(); err != nil {
		return nil, err
	} else {
		flusher.writeOffset = fi.Size()
	}
	return flusher, nil
}

func (b *DataFlusher) WriteOffset() int64 {
	return b.writeOffset
}

// return false if the buffer is full
func (b *DataFlusher) Push(ctx *WriteCtx) (err error) {
	if len(ctx.data) + b.dataLen > kMaxWriteBatchLen {
		// Make room for new coming data
		b.Flush()
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flyingTasks = append(b.flyingTasks, ctx)
	b.dataLen += len(ctx.data)
	return
}

func (b *DataFlusher) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	var batch *BatchOne = NewBatchOne(b.f, b.writeOffset)
	b.CutInto(batch)
	go batch.Flush()
}

func (b *DataFlusher) CutInto(batch *BatchOne) {
	batch.tasks = b.flyingTasks
	batch.writeOffset = b.writeOffset
	b.writeOffset += int64(b.dataLen)
	b.flyingTasks = nil
	b.dataLen = 0
}

// ----------------------------------------------------------------------------------------------------

type BatchOne struct {
	tasks []*WriteCtx   // Unwritten tasks
	f *os.File
	writeOffset int64
}

func NewBatchOne(f *os.File, writeOffset int64) *BatchOne {
	return &BatchOne{nil, f, writeOffset}
}

func (b *BatchOne) Flush() {
	var err error
	batch := b.MakeBuffer()
	if _, err = b.f.WriteAt(batch.Bytes(), b.writeOffset); err == nil {
		b.f.Sync()
	}
	for _, task := range b.tasks {
		task.notify <- &WriteResult{err, b.writeOffset}
		close(task.notify)
		b.writeOffset += int64(len(task.data))
	}
}

func (b *BatchOne) MakeBuffer() (buf *bytes.Buffer) {
	buf = bytes.NewBuffer(nil)
	for _, task := range b.tasks {
		buf.Write(task.data)
	}
	return
}

func main() {
	flag.Parse()
	store, err := NewDumbStore("./data.log")
	if err != nil {
		fmt.Println("NewDumbStore err:", err)
		return;
	}
	stop := make(chan bool)
	b := make([]byte, 100)
	for i:=0; i<*parallism; i+=1 {
		go func(store *DumbStore) {
			for {
				// Write is a blocking API
				store.Write(b)
			}
		} (store)
	}
	<- stop
}
