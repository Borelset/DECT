//
// Created by BorelsetR on 2019/8/18.
//

#include <iostream>
#include <gflags/gflags.h>

#include "../Pipeline/ReadFilePipeline.h"
#include "../Pipeline/RestorePipleline.h"

int main(int argc, char **argv) {

    std::string bPath("/home/zxy/OdessTestset/linux-4.0.1.tar");
    std::string dPath("/home/zxy/OdessTestset/linux-4.0.2.tar");
    //std::string bPath("/home/zxy/OdessTestset/CMakeLists0.txt");
    //std::string dPath("/home/zxy/OdessTestset/CMakeLists1.txt");

    EncodingMethod *encodingMethod = new EDelta(HashType::Gear);
    GlobalReadPipelinePtr = new ReadFilePipeline();
    GlobalChunkingPipelinePtr = new ChunkingPipeline();
    GlobalDeduplicationPipelinePtr = new DeduplicationPipeline(encodingMethod);
    GlobalWriteFilePipelinePtr = new WriteFilePipeline();
    GlobalMetadataManagerPtr = new MetadataManager();
    GlobalRestorePipelinePtr = new RestorePipeline(encodingMethod);

    StorageTask storageTask;

    struct timeval t0, t1;
    gettimeofday(&t0, NULL);
    CountdownLatch countdownLatch(4);
    storageTask.path = bPath;
    storageTask.countdownLatch = &countdownLatch;
    storageTask.fileID = 123123123;
    GlobalReadPipelinePtr->addTask(&storageTask);
    countdownLatch.wait();
    gettimeofday(&t1, NULL);

    float totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
    printf("Total duration:%fs, Speed:%fMB/s\n", totalDuration,
           (float) storageTask.length / totalDuration / 1024 / 1024);

    printf("read duration:%lu, chunk duration:%lu, dedup duration:%lu, write duration:%lu\n",
           storageTask.stageTimePoint.read, storageTask.stageTimePoint.chunk, storageTask.stageTimePoint.dedup,
           storageTask.stageTimePoint.write);
    GlobalDeduplicationPipelinePtr->getStatistics();

    printf("first done\n");

    gettimeofday(&t0, NULL);
    countdownLatch.setCount(4);
    storageTask.path = dPath;
    storageTask.fileID = 123123124;
    GlobalReadPipelinePtr->addTask(&storageTask);
    countdownLatch.wait();
    gettimeofday(&t1, NULL);

    totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
    printf("Total duration:%fs, Speed:%fMB/s\n", totalDuration,
           (float) storageTask.length / totalDuration / 1024 / 1024);

    printf("read duration:%lu, chunk duration:%lu, dedup duration:%lu, write duration:%lu\n",
           storageTask.stageTimePoint.read, storageTask.stageTimePoint.chunk, storageTask.stageTimePoint.dedup,
           storageTask.stageTimePoint.write);
    GlobalDeduplicationPipelinePtr->getStatistics();

    countdownLatch.setCount(1);
    RestoreTask restoreTask = {
            std::string("/home/zxy/OdessHome/logicFiles/123123124"),
            std::string("/home/zxy/OdessHome/restoreFiles/1"),
            &countdownLatch,
    };
    GlobalRestorePipelinePtr->addTask(&restoreTask);
    countdownLatch.wait();

    delete encodingMethod;
    delete GlobalReadPipelinePtr;
    delete GlobalChunkingPipelinePtr;
    delete GlobalDeduplicationPipelinePtr;
    delete GlobalWriteFilePipelinePtr;
    delete GlobalMetadataManagerPtr;
    delete GlobalRestorePipelinePtr;
}
