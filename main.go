package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
)

// 使用 4GB 数据作为仿真，最大内存为 64MB（对应 1TB ~ 16GB）
const (
	maxMemSize    = 64 * 1000 * 1000 // 最大使用内存大小 64MB
	inputBufSize  = 4 * 1000 * 1000  // 输入数据缓冲区大小 4MB
	inputCSize    = 1                // 输入 Channel 缓冲区大小
	outputBufSize = 4 * 1000 * 1000  // 输出数据缓冲区大小 4MB
	outputCSize   = 1                // 输出 Channel 缓冲区大小
	coreNum       = 2                // CPU 核心数
	typeBytesLen  = 8                // 使用 uint64 一个数据单位 8字节
)

// 复用的字节数组池
var (
	inputBufPool = sync.Pool{
		New: func() interface{} {
			newBuf := make([]byte, inputBufSize)
			return newBuf
		},
	}
	outputBufPool = sync.Pool{
		New: func() interface{} {
			newBuf := make([]byte, outputBufSize)
			return newBuf
		},
	}
	// 输出输入完成标识符
	finishedTag = []byte{}
)

func isFinishedTag(data []byte) bool {
	return bytes.Equal(finishedTag, data)
}

////////////////////////////////////////////////////////////////////////////////////////////
// 磁盘读任务，一个合并任务可能有多个
////////////////////////////////////////////////////////////////////////////////////////////
type ReadTask struct {
	file   *os.File
	offset int64
	size   int64
}

func NewReadTask(file *os.File, offset, size int64) *ReadTask {
	return &ReadTask{file: file, offset: offset, size: size}
}

func (rt *ReadTask) Start() <-chan []byte {
	// 设置缓冲区为 1 的 channel，以便可以在没有 merge 完之前就提供下一个 dataBlock
	outputC := make(chan []byte, inputCSize)
	go func() {
		for rt.size > 0 {
			data := inputBufPool.Get().([]byte)
			if rt.size < inputBufSize {
				data = data[:rt.size]
			}
			readSize, err := rt.file.ReadAt(data, rt.offset)
			if readSize != len(data) || err != nil {
				log.Fatal(err)
			}
			fmt.Println("ReadOffset:", rt.offset, "Len:", len(data))
			rt.size -= int64(readSize)
			rt.offset += int64(readSize)
			outputC <- data
		}
		fmt.Println("ReadOffsetDone:", rt.offset)
		outputC <- finishedTag
	}()
	return outputC
}

////////////////////////////////////////////////////////////////////////////////////////////
// 磁盘写任务，一个合并任务只有一个
////////////////////////////////////////////////////////////////////////////////////////////
type WriteTask struct {
	file   *os.File
	offset int64
	doneC  chan struct{}
}

func NewWriteTask(file *os.File, offset int64) *WriteTask {
	return &WriteTask{file: file, offset: offset, doneC: make(chan struct{})}
}

func (wt *WriteTask) Start() chan<- []byte {
	inputC := make(chan []byte, outputCSize)
	go func() {
		for {
			select {
			case data := <-inputC:
				if isFinishedTag(data) {
					fmt.Println("WriteOffsetDone:", wt.offset)
					wt.doneC <- struct{}{}
					return
				}
				fmt.Println("WriteOffset:", wt.offset, "Len:", len(data))
				writeSize, err := wt.file.WriteAt(data, wt.offset)
				if writeSize != len(data) || err != nil {
					log.Fatal(err)
				}
				wt.offset += int64(writeSize)
				outputBufPool.Put(data[:outputBufSize])
			}
		}
	}()
	return inputC
}

func (wt *WriteTask) Wait() {
	<-wt.doneC
}

////////////////////////////////////////////////////////////////////////////////////////////
// 表示一个内存合并任务，有n路输入和一路输出
////////////////////////////////////////////////////////////////////////////////////////////
type MergeTask struct {
	readTasks         []*ReadTask
	writeTask         *WriteTask
	finishedReadTasks map[*ReadTask]bool
}

func NewMergeTask(readTasks []*ReadTask, writeTask *WriteTask) *MergeTask {
	flags := make(map[*ReadTask]bool)
	fmt.Println("NewMergeTask")
	for idx, _ := range readTasks {
		flags[readTasks[idx]] = false
		fmt.Println("ReadTask:", idx, "Offset:", readTasks[idx].offset, "Len:", readTasks[idx].size)
	}
	fmt.Println("WriteTask:", "Offset:", writeTask.offset)
	return &MergeTask{readTasks: readTasks, writeTask: writeTask, finishedReadTasks: flags}
}

func (mt *MergeTask) isFinished(task *ReadTask) bool {
	return mt.finishedReadTasks[task]
}

func (mt *MergeTask) isAllFinished() bool {
	for _, finished := range mt.finishedReadTasks {
		if !finished {
			return false
		}
	}
	return true
}

func (mt *MergeTask) finished(task *ReadTask) {
	mt.finishedReadTasks[task] = true
}

func (mt *MergeTask) Start() {

	inputCs := make(map[*ReadTask]<-chan []byte)
	inputBufs := make(map[*ReadTask][]byte)
	inputBufsOffset := make(map[*ReadTask]int)

	// 开启所有读写任务
	for _, rt := range mt.readTasks {
		inputCs[rt] = rt.Start()
		inputBufs[rt] = nil
		inputBufsOffset[rt] = 0
	}
	outputC := mt.writeTask.Start()

	// 内存 MergeSort
	outputBuf := outputBufPool.Get().([]byte)
	outputBufOffset := 0
	for {

		// 获取所有可能的读 Buffer
		for rt, ch := range inputCs {
			if !mt.isFinished(rt) && inputBufs[rt] == nil {
				readBuf := <-ch
				if isFinishedTag(readBuf) {
					mt.finished(rt)
				} else {
					inputBufs[rt] = readBuf
					inputBufsOffset[rt] = 0
				}
			}
		}

		// 如果所有都结束了则跳出
		if mt.isAllFinished() {
			break
		}

		// 剩余一个以上的时候，对可用的所有 Buffer 读取块排序
		for outputBufOffset < outputBufSize {
			var consumedRt *ReadTask = nil
			var currentData uint64

			for rt, iBuf := range inputBufs {
				if iBuf == nil {
					continue
				}

				iBufOffset := inputBufsOffset[rt]
				iData := binary.BigEndian.Uint64(iBuf[iBufOffset : iBufOffset+typeBytesLen])
				//fmt.Println(rt, iData)

				if consumedRt == nil || currentData > iData {
					consumedRt = rt
					currentData = iData
					binary.BigEndian.PutUint64(outputBuf[outputBufOffset:outputBufOffset+typeBytesLen], currentData)
				}
			}
			if consumedRt == nil {
				// 这段代码不会执行
				log.Fatal("consumedRt: unknown err")
			}
			//fmt.Println(currentData)

			outputBufOffset += typeBytesLen
			inputBufsOffset[consumedRt] += typeBytesLen
			if inputBufsOffset[consumedRt] == len(inputBufs[consumedRt]) {
				// 已经消耗完成，直接回收这部分缓冲区，重置 Channel 缓冲区
				inputBufPool.Put(inputBufs[consumedRt][:inputBufSize])
				inputBufs[consumedRt] = nil
				inputBufsOffset[consumedRt] = 0
				break
			}
		}

		// 如果输出缓冲区满了，重新分配
		if outputBufOffset == outputBufSize {
			outputC <- outputBuf
			outputBuf = outputBufPool.Get().([]byte)
			outputBufOffset = 0
		}
	}

	if outputBufOffset != 0 {
		outputC <- outputBuf[0:outputBufOffset]
	} else {
		outputBufPool.Put(outputBuf[:outputBufSize])
	}

	outputC <- finishedTag

	// 等待所有写任务完成
	mt.writeTask.Wait()
}

////////////////////////////////////////////////////////////////////////////////////////////
// 表示一轮文件全局的排序的执行器，里面执行多个子内存合并任务
////////////////////////////////////////////////////////////////////////////////////////////
type ParallelMergeTask struct {
	taskC chan *MergeTask
	doneC chan struct{}
}

func NewParallelMergeTask() *ParallelMergeTask {
	return &ParallelMergeTask{
		taskC: make(chan *MergeTask),
		doneC: make(chan struct{}),
	}
}

func (gmt *ParallelMergeTask) Start() chan<- *MergeTask {
	go func() {
		// 限制最大可以用时执行 核心数 个任务
		poolC := make(chan struct{}, coreNum)
		for i := 0; i < coreNum; i++ {
			poolC <- struct{}{}
		}
		for {
			select {
			case task := <-gmt.taskC:
				if task == nil {
					// 等待所有合并任务完成
					for i := 0; i < coreNum; i++ {
						<-poolC
					}
					gmt.doneC <- struct{}{}
					return
				}
				<-poolC
				go func(task *MergeTask) {
					// 释放可用
					defer func() { poolC <- struct{}{} }()
					task.Start()
				}(task)
			}
		}
	}()

	return gmt.taskC
}

func (gmt *ParallelMergeTask) TransferTasksDone() {
	// 传递结束标记
	gmt.taskC <- nil
}

func (gmt *ParallelMergeTask) Wait() {
	<-gmt.doneC
}

////////////////////////////////////////////////////////////////////////////////////////////
// 存储文件的元数据
// Size(数据块数目)是为了往外存写，因为现在目前不写Metadata到外存，所以删掉也可以(可以直接用len(Blocks))
////////////////////////////////////////////////////////////////////////////////////////////
type Metadata struct {
	Size   int64   // 数据块数目
	Blocks []int64 // 数据块大小信息（字节为单位）
}

func (md *Metadata) TotalBlocksSize() (result int64) {
	result = 0
	for _, bsize := range md.Blocks {
		result += bsize
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////////////////
// 以下是核心函数，根据核心数创建多次合并任务
// 根据最大可使用的内存进行 Merge 管理
////////////////////////////////////////////////////////////////////////////////////////////
func StartMerge(metadata *Metadata, inputFile *os.File, outputFile *os.File) *Metadata {
	blockSize := int(metadata.Size)
	// 当前合并任务所预计使用的内存量：输入Channel数量 * 一个输入Channel使用的内存 + 输出Channel使用的内存
	predictOneMergeMem := metadata.Size * inputBufSize * (inputCSize + 1) + outputBufSize * (outputCSize + 1)

	// 刚好一次任务可以完成
	if predictOneMergeMem <= maxMemSize {
		wt := NewWriteTask(outputFile, 0)
		rts := make([]*ReadTask, blockSize)
		var iOffset int64 = 0
		for idx, _ := range rts {
			rts[idx] = NewReadTask(inputFile, iOffset, metadata.Blocks[idx])
			iOffset += metadata.Blocks[idx]
		}
		mt := NewMergeTask(rts, wt)
		mt.Start()

		return &Metadata{Size: 1, Blocks: []int64{iOffset}}

		// 需要多次任务完成
	} else {
		// 一个核心最多使用的读 Channel 数目
		readTaskNum := (maxMemSize / coreNum - outputBufSize * (outputCSize + 1)) / (inputBufSize * (inputCSize + 1))
		if readTaskNum <= 1 {
			log.Fatal("可用内存过少")
		}

		mergeTaskNum := blockSize / readTaskNum
		if blockSize % readTaskNum != 0 {
			mergeTaskNum++
		}

		newBlocks := make([]int64, mergeTaskNum)
		mergeTasks := make([]*MergeTask, mergeTaskNum)

		// 当前读取 Block 索引
		currentBlockIdx := 0
		var iOffset int64 = 0
		for idx, _ := range mergeTasks {
			rtsNum := readTaskNum
			if rtsNum+currentBlockIdx > blockSize {
				rtsNum = blockSize - currentBlockIdx
				if rtsNum <= 0 {
					// 这段代码不会执行
					log.Fatal("rtsNum: unknown err")
				}
			}

			wOffset := iOffset
			wt := NewWriteTask(outputFile, wOffset)

			rts := make([]*ReadTask, rtsNum)
			for idx, _ := range rts {
				rts[idx] = NewReadTask(inputFile, iOffset, metadata.Blocks[currentBlockIdx])
				iOffset += metadata.Blocks[currentBlockIdx]
				currentBlockIdx++
			}

			mt := NewMergeTask(rts, wt)
			mergeTasks[idx] = mt
			newBlocks[idx] = iOffset - wOffset
		}

		// 开始并行任务
		pmt := NewParallelMergeTask()
		taskC := pmt.Start()

		for _, task := range mergeTasks {
			taskC <- task
		}

		// 所有任务传输成功
		pmt.TransferTasksDone()

		// 等待最后一个合并任务完成
		pmt.Wait()

		return &Metadata{Size: int64(len(newBlocks)), Blocks: newBlocks}
	}
}

func main() {
	// 请移步 main_test.go 的 TestStartMerge 函数
}
