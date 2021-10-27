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
	kMaxWriteBatchLen int64 = 64 * 1024
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
	result *WriteResult
}

func (ctx *WriteCtx) Notify() {
	ctx.notify <- ctx.result
	close(ctx.notify)
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
	ctx := &WriteCtx{data, make(chan *WriteResult, 1), nil}
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
			s.flusher.Write(true)
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
	waitToFlush []*BatchOne
	muForFlush *sync.Mutex
}

func NewDataFlusher(fpath string) (flusher *DataFlusher, err error) {
	var f *os.File
	if f, err = os.OpenFile(fpath, os.O_RDWR | os.O_CREATE, 0755); err != nil {
		return nil, err
	}
	flusher = &DataFlusher{0, nil, f, 0, &sync.Mutex{}, nil, &sync.Mutex{}}
	if fi, err := f.Stat(); err != nil {
		return nil, err
	} else {
		flusher.writeOffset = fi.Size()
	}
	go flusher.FlushToDisk()
	return flusher, nil
}

func (b *DataFlusher) WriteOffset() int64 {
	return b.writeOffset
}

func (b *DataFlusher) Push(ctx *WriteCtx) (err error) {
	b.mu.Lock()
	b.flyingTasks = append(b.flyingTasks, ctx)
	b.dataLen += len(ctx.data)
	b.mu.Unlock()
	if int64(len(ctx.data) + b.dataLen) > kMaxWriteBatchLen {
		// Make room for new coming data
		b.Write(false)
	}
	return
}

func (b *DataFlusher) Write(forceFlush bool) {
	var batch *BatchOne = NewBatchOne(b, b.f, b.writeOffset)
	b.CutInto(batch)
	go batch.Write(forceFlush)
}

// Hope we can Sync for multiple batches so we can diminish the cost
func (b *DataFlusher) FlushToDisk() {
	for {
		b.muForFlush.Lock()
		batchList := b.waitToFlush
		b.waitToFlush = nil
		b.muForFlush.Unlock()
		err := b.f.Sync()
		for _, batch := range batchList {
			batch.NotifyAll(err)
		}
	}
}

func (b *DataFlusher) CutInto(batch *BatchOne) {
	b.mu.Lock()
	defer b.mu.Unlock()
	batch.tasks = b.flyingTasks
	batch.writeOffset = b.writeOffset
	b.writeOffset += int64(b.dataLen)
	b.flyingTasks = nil
	b.dataLen = 0
}

func (b *DataFlusher) PushToFlush(batch *BatchOne) {
	b.muForFlush.Lock()
	defer b.muForFlush.Unlock()
	b.waitToFlush = append(b.waitToFlush, batch)
}

// ----------------------------------------------------------------------------------------------------

type BatchOne struct {
	tasks []*WriteCtx   // Unwritten tasks
	f *os.File
	writeOffset int64
	flusher *DataFlusher
}

func NewBatchOne(flusher *DataFlusher, f *os.File, writeOffset int64) *BatchOne {
	return &BatchOne{nil, f, writeOffset, flusher}
}

func (b *BatchOne) BatchSize() (sz int64) {
	for _, task := range b.tasks {
		sz += int64(len(task.data))
	}
	return
}

func (b *BatchOne) Write(forceFlush bool) {
	var err error
	batch := b.MakeBuffer()
	_, err = b.f.WriteAt(batch.Bytes(), b.writeOffset)
	for _, task := range b.tasks {
		task.result = &WriteResult{err, b.writeOffset}
		b.writeOffset += int64(len(task.data))
	}
	if err != nil || forceFlush {
		if err == nil {
			err = b.f.Sync()
		}
		b.NotifyAll(err)
	} else {
		b.flusher.PushToFlush(b)
	}
}

func (b *BatchOne) NotifyAll(err error) {
	for _, task := range b.tasks {
		if err != nil {
			task.result.err = err
		}
		task.Notify()
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
