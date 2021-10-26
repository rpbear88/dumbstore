package main

import "bytes"
import "os"
import "flag"
import "fmt"
import "path/filepath"
import "time"
import "sync"
import "strconv"

var parallism = flag.Int("p", 1, "Parallism count.")
var batch = flag.Bool("batch", false, "Enable batch or not.")

var metrics = NewIoMetrics(1)

var (
	kMaxWriteBatchLen = 64 * 1024
	kAlignmentSize int64 = 4096
	kLoggingParallel = 1						// 存储引擎同时允许写入的文件数量
)

func paddingSize(len int64) int64 {
	return (len + kAlignmentSize - 1) / kAlignmentSize * kAlignmentSize
}

type DumbStore struct {
	// Pending channel which buffers incoming requests
	p chan *WriteCtx
	flushers []*DataFlusher
	select_seed uint64
}

type WriteResult struct {
	err error
	offset int64  // File offset which the data written to
}

type WriteCtx struct {
	data []byte
	notify chan *WriteResult
}

func NewDumbStore(fpath string, enable_batch bool) (s *DumbStore, err error) {
	store := &DumbStore {}
	store.p = make(chan *WriteCtx)
	for i:=0; i<kLoggingParallel; i+=1 {
		var flusher *DataFlusher
		if flusher, err = NewDataFlusher(filepath.Join(fpath, strconv.Itoa(i)), enable_batch); err != nil {
			return
		}
		store.flushers = append(store.flushers, flusher)
	}
	go store.Persist()
	return store, nil
}

func (s *DumbStore) Write(data []byte) error {
	ctx := &WriteCtx{data, make(chan *WriteResult, 1)}
	s.p <- ctx
	res := <- ctx.notify
	return res.err
}

func (s *DumbStore) Persist() {
	for {
		select {
		case ctx, ok := <- s.p:
			if !ok { break }
			s.SelectFlusher().Push(ctx)
		default:
			break
		}
	}
}

func (s *DumbStore) SelectFlusher() *DataFlusher {
	s.select_seed += 1
	return s.flushers[s.select_seed % uint64(len(s.flushers))]
}

// ----------------------------------------------------------------------------------------------------

type DataFlusher struct {
	enable_batch bool
	datalen int                // Total data size in flying_tasks
	flying_tasks []*WriteCtx   // Unwritten tasks
	f *os.File
	write_offset int64
	mu *sync.Mutex
}

func NewDataFlusher(fpath string, enable_batch bool) (flusher *DataFlusher, err error) {
	var f *os.File
	if f, err = os.OpenFile(fpath, os.O_RDWR | os.O_CREATE, 0755); err != nil {
		return nil, err
	}
	flusher = &DataFlusher{enable_batch, 0, nil, f, 0, &sync.Mutex{}}
	if fi, err := f.Stat(); err != nil {
		return nil, err
	} else {
		flusher.write_offset = fi.Size()
	}
	return flusher, nil
}

// return false if the buffer is full
func (b *DataFlusher) Push(ctx *WriteCtx) (err error) {
	if len(ctx.data) + b.datalen > kMaxWriteBatchLen {
		// Make room for new coming data
		if err = b.Flush(); err != nil {
			return
		}
	}
	b.mu.Lock()
	b.flying_tasks = append(b.flying_tasks, ctx)
	b.datalen += len(ctx.data)
	b.mu.Unlock()
	return
}

func (b *DataFlusher) Flush() (err error) {
	if b.enable_batch && b.datalen < kMaxWriteBatchLen {
		return nil
	}
	b.mu.Lock()
	var batch *BatchOne = NewBatchOne(b.f, b.write_offset)
	b.CutInto(batch)
	b.mu.Unlock()
	go batch.Flush()
	return nil
}

func (b *DataFlusher) CutInto(batch *BatchOne) {
	batch.tasks = b.flying_tasks
	batch.write_offset = b.write_offset
	b.write_offset += paddingSize(int64(b.datalen))
	b.flying_tasks = nil
	b.datalen = 0
}

// ----------------------------------------------------------------------------------------------------

type BatchOne struct {
	tasks []*WriteCtx   // Unwritten tasks
	f *os.File
	write_offset int64
}

func NewBatchOne(f *os.File, write_offset int64) *BatchOne {
	return &BatchOne{nil, f, write_offset}
}

func (b *BatchOne) Flush() {
	batch := b.MakeBuffer()
	start := time.Now()
	_, err := b.f.WriteAt(batch.Bytes(), b.write_offset)
	b.f.Sync()
	metrics.Update(int64(batch.Len()), time.Now().Sub(start).Milliseconds())
	var total int64 = 0
	for _, task := range b.tasks {
		total += int64(len(task.data))
		task.notify <- &WriteResult{err, b.write_offset + total}
	}
}

func (b *BatchOne) MakeBuffer() (buf *bytes.Buffer) {
	buf = bytes.NewBuffer(nil)
	for _, task := range b.tasks {
		buf.Write(task.data)
	}
	delta := paddingSize(int64(buf.Len())) - int64(buf.Len())
	if delta != 0 {
		buf.Write(make([]byte, delta))
	}
	return
}

func main() {
	flag.Parse()
	store, err := NewDumbStore("./data", *batch)
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
			stop <- true
		} (store)
	}
	<- stop
}