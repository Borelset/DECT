//
// Created by BorelsetR on 2019/8/31.
//

#ifndef ODESSSTORAGE_HASHINGPIPELINE_H
#define ODESSSTORAGE_HASHINGPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "isa-l_crypto/mh_sha1.h"
#include "openssl/sha.h"
#include "DeduplicationPipeline.h"
#include <assert.h>



class HashingPipeline {
public:
    HashingPipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&HashingPipeline::hashingWorkerCallback, this));
    }

    int addTask(const DedupTask &dedupTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        /*
        if (receiceList.empty()) {
            taskAmount += dedupList->size();
            receiceList.swap(*dedupList);
            condition.notifyAll();
        } else {
            for (const auto &dedupTask: *dedupList) {
                receiceList.push_back(dedupTask);
            }
            taskAmount += dedupList->size();
            dedupList->clear();
            condition.notifyAll();
        }
         */
        receiceList.push_back(dedupTask);
        taskAmount++;
        condition.notifyAll();

      return 0;
    }

    ~HashingPipeline() {

        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
#ifdef DEBUG
        printf("hash duration:%lu\n", HashDuration);
#endif
    }

private:
    void hashingWorkerCallback() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        struct timeval t0, t1, t2;

//        FileOperator lengthLog("length.log", FileOpenType::ReadWrite);
//        uint8_t* logBuffer = (uint8_t*)malloc(1024);


        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount = 0;
                taskList.swap(receiceList);
            }

            gettimeofday(&t0, NULL);
            for (auto &dedupTask : taskList) {
//                sprintf((char*)logBuffer, "%lu\n", dedupTask.length);
//                lengthLog.write(logBuffer, strlen((char*)logBuffer));
              //SHA1_Init(&ctx);
              //SHA1_Update(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
              //SHA1_Final((unsigned char *) &dedupTask.fp, &ctx);
              mh_sha1_init(&ctx);
              mh_sha1_update_avx(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
              mh_sha1_finalize_avx(&ctx, &dedupTask.fp);\
                if (dedupTask.countdownLatch) {
                dedupTask.countdownLatch->countDown();
              }
              GlobalDeduplicationPipelinePtr->addTask(dedupTask);
            }
            taskList.clear();
            gettimeofday(&t1, NULL);
            HashDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    std::thread *worker;
    std::list<DedupTask> taskList;
    std::list<DedupTask> receiceList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;
    uint64_t HashDuration = 0;
};

static HashingPipeline *GlobalHashingPipelinePtr;


#endif //ODESSSTORAGE_HASHINGPIPELINE_H
