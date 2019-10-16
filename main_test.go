package main

import (
    "encoding/binary"
    "math/rand"
    "os"
    "sort"
    "sync"
    "testing"
    "time"
)

const (
    inputFilename = "blocks"
    outputFileName = "blocks.swap"
)

type SortableUint64Array []uint64

func (arr SortableUint64Array) Len() int {
    return len(arr)
}

func (arr SortableUint64Array) Less(i, j int) bool {
    return arr[i] < arr[j]
}

func (arr SortableUint64Array) Swap(i, j int) {
    arr[i], arr[j] = arr[j], arr[i]
}

func generateTestData(t *testing.T, filename string, blockNum, numSize int64) (*Metadata, *os.File) {
    if _, err := os.Stat(filename); os.IsExist(err) {
        t.Fatal(filename + " is exist")
    }
    if blockNum == 0 || numSize == 0 {
        t.Fatal("blockNum or numSize is zero")
    }
    if numSize % typeBytesLen != 0 {
        numSize += typeBytesLen - numSize % typeBytesLen
    }
    blocks := make([]int64, blockNum)
    for idx, _ := range blocks {
        blocks[idx] = numSize
    }
    metadata := &Metadata{Size: blockNum, Blocks: blocks}

    file, err := os.Create(filename)
    if err != nil {
        t.Fatal(err)
    }

    wg := sync.WaitGroup{}
    wg.Add(int(blockNum))

    var blockIdx int64 = 0

    for ; blockIdx < blockNum; blockIdx++ {

        // make sorted block
        go func(offset int64) {

            uint64Size := numSize / typeBytesLen

            uint64Data := make(SortableUint64Array, uint64Size)

            for i := 0; i < len(uint64Data); i++ {
                uint64Data[i] = rand.Uint64()
            }

            sort.Sort(uint64Data)

            // 测试用，实际不应该直接这样写入
            dataSize := numSize
            if dataSize < outputBufSize {
                dataSize = outputBufSize
            }
            byteData := make([]byte, dataSize)

            for i := 0; i < len(uint64Data); i++ {
                binary.BigEndian.PutUint64(byteData[i*typeBytesLen : (i+1)*typeBytesLen], uint64Data[i])
            }

            wt := NewWriteTask(file, offset)

            outputC := wt.Start()
            outputC <- byteData[:numSize]
            outputC <- finishedTag

            wt.Wait()

            wg.Done()

        }(blockIdx * numSize)
    }

    wg.Wait()
    return metadata, file
}

func clearFile(t *testing.T, filename string) {
    if _, err := os.Stat(filename); os.IsExist(err) {
        err = os.Remove(filename)
        if err != nil {
            t.Fatal(err)
        }
    }
}

func closeFile(t *testing.T, file *os.File)  {
    err := file.Close()
    if err != nil {
        t.Fatal(err)
    }
}

func assertResult(t *testing.T, file *os.File, metadata *Metadata) {
    rt := NewReadTask(file, 0, metadata.TotalBlocksSize())
    dataC := rt.Start()

    var currentData uint64
    isFirst := true
    for {
        select {
        case dataBlock := <-dataC:
            if isFinishedTag(dataBlock) {
                return
            }
            offset := 0
            for offset < len(dataBlock) {
                iData := binary.BigEndian.Uint64(dataBlock[offset:offset+typeBytesLen])
                if isFirst {
                    isFirst = false
                    currentData = iData
                } else if currentData > iData {
                    t.Fatal("排序错误")
                }
                currentData = iData
                offset += typeBytesLen
            }
        }
    }
}

func TestStartMerge(t *testing.T) {
    clearFile(t, inputFilename)
    ////////////////////////////////////////////////////////////////////////////////////////////
    // blockNum 和 numSize 可以改，如:
    // metadata, inputFile := generateTestData(t, inputFilename, 13, inputBufSize / 2)
    ////////////////////////////////////////////////////////////////////////////////////////////
    metadata, inputFile := generateTestData(t, inputFilename, 20, inputBufSize)
    defer closeFile(t, inputFile)

    clearFile(t, outputFileName)
    outputFile, err := os.Create(outputFileName)
    if err != nil {
        t.Fatal(err)
    }
    defer closeFile(t, outputFile)
    t.Log(metadata)
    before := time.Now()
    ////////////////////////////////////////////////////////////////////////////////////////////
    // 关键执行代码，通过两个文件互相倒，最终完成文件的输出
    ////////////////////////////////////////////////////////////////////////////////////////////
    newMetadata := StartMerge(metadata, inputFile, outputFile)
    t.Log(newMetadata)
    for newMetadata.Size > 1 {
        inputFile, outputFile = outputFile, inputFile
        newMetadata = StartMerge(newMetadata, inputFile, outputFile)
        t.Log(newMetadata)
    }
    ////////////////////////////////////////////////////////////////////////////////////////////
    // 关键代码结束
    ////////////////////////////////////////////////////////////////////////////////////////////
    t.Log("合并花费时间:", time.Since(before).Seconds(), "s")
    t.Log("最终输出的排序后的文件名：", outputFile.Name())
    assertResult(t, outputFile, newMetadata)

}

func init() {
    rand.Seed(time.Now().UnixNano())
}