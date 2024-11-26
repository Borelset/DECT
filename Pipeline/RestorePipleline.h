//
// Created by BorelsetR on 2019/7/31.
//

#ifndef ODESSNEW_RESTOREPIPLELINE_H
#define ODESSNEW_RESTOREPIPLELINE_H

#include "../MetadataManager/MetadataManager.h"
#include "../MetadataManager/ChunkCache.h"

extern std::string LogicFilePath;
extern std::string ChunkFilePath;

extern const int ContainerSize;

struct RestoreWrite{
    uint8_t* buffer = nullptr;
    uint64_t type:1;
    uint64_t endFlag:1;
    uint64_t length:63;
    uint64_t originLength;
    SHA1FP FP;
    SHA1FP baseFP;
    Location location;

    RestoreWrite(uint8_t* buf, uint64_t t, uint64_t l, uint64_t ol, const SHA1FP& fp, const SHA1FP& bfp, const Location& loc){
        buffer = (uint8_t*)malloc(l);
        memcpy(buffer, buf, l);
        originLength = ol;
        type = t;
        length = l;
        baseFP = bfp;
        location = loc;
        FP = fp;
        endFlag = 0;
    }

    RestoreWrite(uint64_t ef){
        endFlag = ef;
    }

    ~RestoreWrite(){
        if(buffer){
            free(buffer);
        }
    }
};

class RestorePipeline {
public:
    RestorePipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                                          condition(mutexLock) {

    }

    int addTask(RestoreWrite *restoreWrite) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreWrite);
        taskAmount++;
        condition.notify();
      return 0;
    }

    int runTask(RestoreTask *r){
        workerRead = new std::thread(std::bind(&RestorePipeline::ReadFileCallback, this, r->recipeID));
//        workerWrite = new std::thread(std::bind(&RestorePipeline::writeFileCallback, this));
        restoreTask = r;
      return 0;
    }

    ~RestorePipeline() {
        // todo worker destruction
//        runningFlag = false;
//        condition.notifyAll();
//        workerWrite->join();
        workerRead->join();
    }

private:
    void ReadFileCallback(uint64_t recipeID){
        char recipePath[256];
        sprintf(recipePath, LogicFilePath.c_str(), recipeID);
        FileOperator recipeFO(recipePath, FileOpenType::Read);
        uint64_t recipeSize = recipeFO.getSize();
        uint8_t* recipeBuffer = (uint8_t*)malloc(recipeSize);
        recipeFO.read(recipeBuffer, recipeSize);

        FileOperator restoreFile((char*)restoreTask->outputPath.c_str(), FileOpenType::ReadWrite);
        uint8_t* decodeBuffer = (uint8_t*)malloc(65536 * 2);
        usize_t decodeSize;
        uint64_t size = 0;

        uint64_t count = recipeSize / (sizeof(WriteHead) + sizeof(Location));
        printf("%lu chunks\n", count);

        RecipeUnit *recipeUnit = (RecipeUnit *) recipeBuffer;

        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        std::unordered_set<uint64_t> usedContainers;

        for (int i = 0; i < count; i++) {
            CachedChunk processingChunk;
            WriteHead &wh = recipeUnit[i].writeHead;
            Location &loc = recipeUnit[i].location;
            int r;
            if (loc.bfid == -1) {
                r = chunkCache.getRecord(loc.fid, wh.fp, &processingChunk, nullptr);
            } else {
                r = chunkCache.getRecord(loc.fid, wh.fp, &processingChunk, &loc.baseFP);
            }
            if (!r) {
                chunkCache.PrefetchingContainer(loc.fid);
                usedContainers.insert(loc.fid);
                if (loc.bfid == -1) {
                    r = chunkCache.getRecord(loc.fid, wh.fp, &processingChunk, nullptr);
                } else {
                    r = chunkCache.getRecord(loc.fid, wh.fp, &processingChunk, &loc.baseFP);
                }
                assert(r);
            }
            if(processingChunk.type){
              CachedChunk baseChunk;
              r = chunkCache.getRecord(loc.bfid, wh.bfp, &baseChunk, nullptr);
              if (!r || baseChunk.type == 1) {
                chunkCache.PrefetchingContainer(loc.bfid);
                usedContainers.insert(loc.bfid);
                r = chunkCache.getRecord(loc.bfid, wh.bfp, &baseChunk, nullptr);
                assert(baseChunk.type == 0);
                assert(r);
              }
//                r = xd3_decode_memory(processingChunk.buffer, processingChunk.length, baseChunk.buffer,
//                                      baseChunk.length, decodeBuffer, &decodeSize, 65536 * 2,
//                                      XD3_COMPLEVEL_1 | XD3_NOCOMPRESS);

              assert(r == 0);
              assert(decodeSize == loc.oriLength);
              //chunkCache.addRecord_original_replace(restoreWrite->FP, decodeBuffer, decodeSize);
              restoreFile.write(decodeBuffer, decodeSize);
              size += decodeSize;
            }else{
                restoreFile.write(processingChunk.buffer, processingChunk.length);
                size += processingChunk.length;
            };
            //RestoreWrite* restoreWrite = new RestoreWrite(processingChunk.buffer, processingChunk.type, processingChunk.length, loc.oriLength, wh.fp, wh.bfp, loc);
//            addTask(restoreWrite);
        }
//        RestoreWrite* restoreWriteEnd = new RestoreWrite(true);
//        addTask(restoreWriteEnd);
        restoreFile.fsync();
        free(decodeBuffer);
        restoreTask->countdownLatch->countDown();
        gettimeofday(&t1, NULL);
        uint64_t total = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        printf("restore speed: %f MB/s\n", (float)size / total);
        printf("read amplification: %f\n", (float) chunkCache.getPreloadSize() / size);
        printf("used containers:%lu\n", usedContainers.size());
    }


//    void writeFileCallback() {
////        FileOperator lengthLog("length.log", FileOpenType::ReadWrite);
////        uint8_t* logBuffer = (uint8_t*)malloc(1024);
//        struct timeval t0, t1;
//        gettimeofday(&t0, NULL);
//        RestoreWrite *restoreWrite;
//        uint8_t* decodeBuffer = (uint8_t*)malloc(65536 * 2);
//        usize_t decodeSize;
//        uint64_t size = 0;
//
//        FileOperator restoreFile((char*)restoreTask->outputPath.c_str(), FileOpenType::ReadWrite);
//
//        while (runningFlag) {
//            {
//                MutexLockGuard mutexLockGuard(mutexLock);
//                while (!taskAmount) {
//                    condition.wait();
//                    if (!runningFlag) break;
//                }
//                if (!runningFlag) continue;
//                taskAmount--;
//                restoreWrite = taskList.front();
//                taskList.pop_front();
//            }
//            if(restoreWrite->endFlag){
//                delete restoreWrite;
//                break;
//            }else{
//                if(restoreWrite->type){
//                    CachedChunk cachedChunk;
//                    int r = chunkCache.getRecord(restoreWrite->baseFP, &cachedChunk);
//                    if(!r || cachedChunk.type == 1){
//                        PrefetchingCache(restoreWrite->location.bfid);
//                        r = chunkCache.getRecord(restoreWrite->baseFP, &cachedChunk);
//                        assert(cachedChunk.type == 0);
//                        assert(r);
//                    }
//                    r = xd3_decode_memory(restoreWrite->buffer, restoreWrite->length, cachedChunk.buffer, cachedChunk.length, decodeBuffer, &decodeSize, 65536*2, XD3_COMPLEVEL_1);
//                    assert(r == 0);
//                    assert(decodeSize == restoreWrite->originLength);
//                    //chunkCache.addRecord_original_replace(restoreWrite->FP, decodeBuffer, decodeSize);
//                    restoreFile.write(decodeBuffer, decodeSize);
//                    size += decodeSize;
////                    sprintf((char*)logBuffer, "%lu\n", decodeSize);
////                    lengthLog.write(logBuffer, strlen((char*)logBuffer));
//                }else{
//                    restoreFile.write(restoreWrite->buffer, restoreWrite->length);
//                    size += restoreWrite->length;
////                    sprintf((char*)logBuffer, "%lu\n", restoreWrite->length);
////                    lengthLog.write(logBuffer, strlen((char*)logBuffer));
//                }
//            }
//            delete restoreWrite;
//        }
//        restoreFile.fsync();
//        free(decodeBuffer);
//        restoreTask->countdownLatch->countDown();
//        gettimeofday(&t1, NULL);
//        uint64_t total = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
//        printf("restore speed: %f MB/s\n", (float)size / total);
//        printf("random seeks:%lu\n", prefetchingTimes);
//        printf("read amplification: %f\n", (float)prefetchingSize / size);
//        printf("average prefetching time:%f\n", (float)prefetchingDuration/prefetchingTimes);
//    }

//    void PrefetchingCache(int fid){
//        struct timeval t0, t1;
//         gettimeofday(&t0, NULL);
//        prefetchingTimes++;
//
//        uint8_t* preloadBuffer = (uint8_t*)malloc(ContainerSize*2);
//        int readSize = -1;
//        {
//            FileOperator* fileOperatorPtr = ChunkFileManager::get(fid);
//            uint64_t cSize = fileOperatorPtr->getSize();
//            readSize = fileOperatorPtr->read(preloadBuffer, cSize);
//            fileOperatorPtr->releaseBufferedData();
//            delete fileOperatorPtr;
//        }
//
//        prefetchingSize += readSize;
//        int preLoadPos = 0;
//        BlockHead *headPtr;
//        //assert(readSize>=ContainerSize && readSize<=ContainerSize*2);
//
//        while (preLoadPos < readSize) {
//            headPtr = (BlockHead *) (preloadBuffer + preLoadPos);
//            assert(preLoadPos + sizeof(BlockHead) < readSize-1);
//            chunkCache.addRecord(headPtr->sha1Fp, preloadBuffer + preLoadPos + sizeof(BlockHead),
//                                 headPtr->length - sizeof(BlockHead), headPtr->type);
//            preLoadPos += headPtr->length;
//        }
//        assert(preLoadPos == readSize);
//
//        free(preloadBuffer);
//        gettimeofday(&t1, NULL);
//        prefetchingDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
//    }

    ChunkCache chunkCache;

    RestoreTask* restoreTask;
    bool runningFlag;
    std::thread *workerRead;
    std::thread *workerWrite;
    uint64_t taskAmount;
    std::list<RestoreWrite *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t prefetchingSize = 0;
    uint64_t prefetchingTimes = 0;
    uint64_t prefetchingDuration = 0;
};

static RestorePipeline *GlobalRestorePipelinePtr;


#endif //ODESSNEW_RESTOREPIPLELINE_H
