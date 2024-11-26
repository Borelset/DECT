//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_WRITEFILEPIPELINE_H
#define ODESSNEW_WRITEFILEPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "../MetadataManager/MetadataManager.h"
#include "../Utility/FileOperator.h"
#include <zstd.h>

uint64_t WriteDuration = 0;

DEFINE_string(ChunkFilePath, "./chunkFiles/%lu", "chunk path");
DEFINE_string(LogicFilePath, "./logicFiles/%lu", "logic path");
DEFINE_int32(ContainerSize, 4096, "Unit:KB");
DEFINE_bool(SampleFlag, false, "whether sampling delta chunks");

std::string LogicFilePath = FLAGS_LogicFilePath;
std::string ChunkFilePath = FLAGS_ChunkFilePath;

#define SAMPLING_RATIO 32

#define MemoryMatadata
#define DEBUG

#define SAMPLING_AVERAGE
// #define SAMPLING_RANDOM

bool likely(bool input) {
  return __builtin_expect(input, 1);
}

bool unlikely(bool input) {
  return __builtin_expect(input, 0);
}

const int ContainerSize = FLAGS_ContainerSize * 1024;
uint64_t BufferCapacity = (uint64_t) (FLAGS_ContainerSize * 1024 * 1.2);

struct WriteBuffer {
    uint64_t totalLength;
    uint64_t used;
    uint8_t *buffer = nullptr;
    uint8_t *compressBuffer = nullptr;

    void init() {
      buffer = (uint8_t *) malloc(BufferCapacity);
      compressBuffer = (uint8_t *) malloc(BufferCapacity);
        totalLength = ContainerSize;
        used = 0;
    }

    void write(uint8_t *buf, uint64_t len) {
        memcpy(buffer + used, buf, len);
        used += len;
    }

    void clear() {
      // fucking why? __memset_sse2 takes too much time in docker.
//      memset(buffer, 0, BufferCapacity);
//      memset(compressBuffer, 0, BufferCapacity);
      used = 0;
    }

    void release() {
      free(buffer);
      free(compressBuffer);
    }
};

class WriteFilePipeline {
public:
    WriteFilePipeline() : fid(0), runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
      worker = new std::thread(std::bind(&WriteFilePipeline::writeFileCallback, this));
    }

    int addTask(const WriteTask &writeTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        recieveList.push_back(writeTask);
        taskAmount++;
        condition.notify();
      return 0;
    }

    ~WriteFilePipeline() {
      // todo worker destruction
//        delete chunkFileOperator;
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
#ifdef DEBUG
      printf("write duration:%lu\n", duration);
//        printf("[Local] Workload:%lu, AfterDedup:%lu, Dedup Ratio:%f\n", localWLSize,
//               localAfterDedupSize, 1.0 - (float)localAfterDedupSize/localWLSize);
      printf("[Global] Workload:%lu, AfterDedup:%lu, Dedup Ratio:%f\n", globalWLSize,
             globalAfterDedupSize, 1.0 - (float) globalAfterDedupSize / globalWLSize);
#endif
    }

private:
    void writeFileCallback() {
      WriteHead writeHead;
      BlockHead blockHead;
      Location location;
      uint64_t total = 0;
      uint64_t newChunk = 0;
      struct timeval t0, t1, tCompress0, tCompress1;

//      uint64_t compressionT = 0;

      uint32_t locLength, oriLength;

      writeBuffer.init();

      while (likely(runningFlag)) {
        gettimeofday(&t0, NULL);
        {
          MutexLockGuard mutexLockGuard(mutexLock);
          while (!taskAmount) {
            condition.wait();
            if (unlikely(!runningFlag)) break;
          }
          if (unlikely(!runningFlag)) continue;
          taskAmount = 0;
          condition.notify();
          taskList.swap(recieveList);
        }



            for (auto &writeTask : taskList) {
              localWLSize += writeTask.bufferLength;
              switch (writeTask.type) {
                case 0:

                  break;
                case 1:
//                  writeHead.type = 1;
//                  writeHead.length = sizeof(Location);
//                  writeHead.fp = writeTask.sha1Fp;
//                  writeHead.bfp = writeTask.baseFP;
//
//                  blockHead.type = 1;
//                        blockHead.sha1Fp = writeTask.sha1Fp;
//                  blockHead.length = writeTask.deltaBufferLength + sizeof(BlockHead);
//                  blockHead.baseFP = writeTask.baseFP;
//
                  if (FLAGS_SampleFlag) {
                    writeBuffer.write((uint8_t *) &blockHead, sizeof(BlockHead));
                    writeBuffer.write(writeTask.deltaBuffer, writeTask.deltaBufferLength);
                  }
//                  free(writeTask.deltaBuffer);
//
                  localAfterDedupSize += writeTask.bufferLength;
                  localAfterDeltaSize += writeTask.bufferLength;
                  break;
                case 2:
                        blockHead.type = 0;
                        blockHead.sha1Fp = writeTask.sha1Fp;
                        blockHead.length = writeTask.bufferLength + sizeof(BlockHead);
                  writeBuffer.write((uint8_t *) &blockHead, sizeof(BlockHead));
                  writeBuffer.write(writeTask.buffer + writeTask.pos, writeTask.bufferLength);

                  localAfterDedupSize += writeTask.bufferLength;
                  localAfterDeltaSize += writeTask.bufferLength;

                  newChunk++;
                  break;
              }

                if (writeTask.countdownLatch) {
                  printf("write done\n");
                  writeTask.countdownLatch->countDown();
                  free(writeTask.buffer);

                  if (fid % SAMPLING_RATIO == 0) {

                    gettimeofday(&tCompress0, NULL);
//                    size_t compressedSize = BufferCapacity;
//                    int r = compress(writeBuffer.compressBuffer, &compressedSize, writeBuffer.buffer, writeBuffer.used);
                    size_t compressedSize = ZSTD_compress(writeBuffer.compressBuffer, BufferCapacity,
                                                          writeBuffer.buffer,
                                                          writeBuffer.used, 1);
                    gettimeofday(&tCompress1, NULL);
                    sizeBeforeCompression += writeBuffer.used;
                    sizeAfterCompression += compressedSize;
//                    assert(!ZSTD_isError(compressedSize));
                    localAfterCompressionSize += compressedSize;
                  }
                  writeBuffer.clear();
                  fid++;

                  printf("BeforeCompression:%lu, AfterCompression:%lu, CompressionReduce:%lu, CompressionRatio:%f, ReductionRatio:%f\n",
                         sizeBeforeCompression, sizeAfterCompression, sizeBeforeCompression - sizeAfterCompression,
                         ((float) sizeBeforeCompression / (sizeAfterCompression + 1)),
                         1.0 - ((float) sizeAfterCompression / (sizeBeforeCompression + 1)));

//                  compressionT +=
//                          (tCompress1.tv_sec - tCompress0.tv_sec) * 1000000 + (tCompress1.tv_usec - tCompress0.tv_usec);
//                  printf("Compression :%lu\n", compressionT);
//                  compressionT = 0;

                  globalWLSize += localWLSize;
                  globalAfterDedupSize += localAfterDedupSize;
                  globalAfterDeltaSize += localAfterDeltaSize;
                  globalAfterCompressionSize += localAfterCompressionSize;

                  getStatistics();

                  localWLSize = 0, localAfterDedupSize = 0, localAfterDeltaSize = 0, localAfterCompressionSize = 0;
                  duration = 0;

                }

                if (writeBuffer.used >= ContainerSize) {
                  if (fid % SAMPLING_RATIO == 0) {
                    gettimeofday(&tCompress0, NULL);
//                    size_t compressedSize = BufferCapacity;
//                    int r = compress(writeBuffer.compressBuffer, &compressedSize, writeBuffer.buffer, writeBuffer.used);
                    size_t compressedSize = ZSTD_compress(writeBuffer.compressBuffer, BufferCapacity,
                                                          writeBuffer.buffer,
                                                          writeBuffer.used, 1);
                    gettimeofday(&tCompress1, NULL);
                    sizeBeforeCompression += writeBuffer.used;
                    sizeAfterCompression += compressedSize;
//                    assert(!ZSTD_isError(compressedSize));
                    localAfterCompressionSize += compressedSize;

//                    compressionT += (tCompress1.tv_sec - tCompress0.tv_sec) * 1000000 +
//                                    (tCompress1.tv_usec - tCompress0.tv_usec);
//                    printf("Compression :%lu\n", compressionT);
                  }
                  writeBuffer.clear();
                  fid++;
                }
            }
            taskList.clear();
#ifdef DEBUG
            total++;
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            WriteDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
#endif
        }

        writeBuffer.release();
    }

    char buffer[256];
    uint64_t fid;
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<WriteTask> taskList;
    std::list<WriteTask> recieveList;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;
    uint64_t globalWLSize = 0, globalAfterDedupSize = 0, globalAfterDeltaSize = 0, globalAfterCompressionSize = 0;
    uint64_t localWLSize = 0, localAfterDedupSize = 0, localAfterDeltaSize = 0, localAfterCompressionSize = 0;

    WriteBuffer writeBuffer;
    uint64_t sizeBeforeCompression = 0, sizeAfterCompression = 0;
};

static WriteFilePipeline *GlobalWriteFilePipelinePtr;

#endif //ODESSNEW_WRITEFILEPIPELINE_H
