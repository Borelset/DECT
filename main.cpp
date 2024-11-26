#include <iostream>

#include "Pipeline/ReadFilePipeline.h"
#include "Pipeline/RestorePipleline.h"
#include "gflags/gflags.h"
#include <fstream>
//#include "MetadataManager/ChunkCache.h"

DEFINE_string(Path, "", "storage path");
DEFINE_string(RestorePath, "", "restore path");
DEFINE_string(task, "", "task type");
DEFINE_string(BatchFilePath, "", "batch process file path");
DEFINE_int32(RecipeID, 0, "recipe id");

extern uint64_t ChunkDuration;
extern uint64_t DedupDuration;
extern uint64_t HashDuration;
extern uint64_t ReadDuration;
extern uint64_t WriteDuration;

#define MemoryMetadata

std::string getFilename(const std::string &path) {
  std::size_t pos = path.find_last_of("/\\");
  return path.substr(pos + 1);
}

std::vector<std::string> FilePaths;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string putStr("put");
  std::string getStr("get");
  std::string batchStr("batch");
  std::string statusStr("status");


  if (FLAGS_task == putStr) {
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        StorageTask storageTask;
        CountdownLatch countdownLatch(4);
        storageTask.path = FLAGS_Path;
        storageTask.countdownLatch = &countdownLatch;
        storageTask.fileID = 123123123;
        GlobalReadPipelinePtr->addTask(&storageTask);
        countdownLatch.wait();

        gettimeofday(&t1, NULL);
        float totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
        printf("Total duration:%fs, Speed:%fMB/s\n", totalDuration,
               (float) storageTask.length / totalDuration / 1024 / 1024);
        printf("read duration:%luus, chunk duration:%luus, dedup duration:%luus, write duration:%lu\nus",
               storageTask.stageTimePoint.read, storageTask.stageTimePoint.chunk, storageTask.stageTimePoint.dedup,
               storageTask.stageTimePoint.write);

    } else if (FLAGS_task == getStr) {
    GlobalRestorePipelinePtr = new RestorePipeline();
    CountdownLatch countdownLatch(1);
    RestoreTask restoreTask;
    restoreTask.recipeID = FLAGS_RecipeID;
    restoreTask.outputPath = FLAGS_RestorePath;
    restoreTask.countdownLatch = &countdownLatch;

    GlobalRestorePipelinePtr->runTask(&restoreTask);
    countdownLatch.wait();
    delete GlobalRestorePipelinePtr;
  } else if (FLAGS_task == statusStr) {
        printf("Not implemented\n");
    } else if (FLAGS_task == batchStr) {
        GlobalReadPipelinePtr = new ReadFilePipeline();
        GlobalChunkingPipelinePtr = new ChunkingPipeline();
        GlobalHashingPipelinePtr = new HashingPipeline();
        GlobalDeduplicationPipelinePtr = new DeduplicationPipeline();
        GlobalWriteFilePipelinePtr = new WriteFilePipeline();
#ifdef MemoryMetadata
        GlobalMetadataManagerPtr = new MetadataManager();
#else
        GlobalMetadataManagerPersistPtr = new MetadataManagerPersist();
#endif
        GlobalDeduplicationPipelinePtr->setMaxChunkSize(FLAGS_ExpectSize * 8 * 1.2);

        char copyCommand[128];
        std::string format = "cp %s /dev/shm/temp";

    struct timeval total0, total1;
    uint64_t totalSize = 0;
    gettimeofday(&total0, NULL);
    std::ifstream infile;
    infile.open(FLAGS_BatchFilePath);
    std::string subPath;
    uint64_t counter = 0;
    std::cout << "Batch path: " << FLAGS_BatchFilePath << std::endl;
    while (std::getline(infile, subPath)) {
      FilePaths.push_back(subPath);
    }
    for (const auto &item: FilePaths) {
      printf("----------------------------------------------\n");
//      std::cout << "Task: " << item << std::endl;
//      sprintf(copyCommand, format.c_str(), item.c_str());
//      system(copyCommand);

      struct timeval t0, t1;
      gettimeofday(&t0, NULL);

      StorageTask storageTask;
      CountdownLatch countdownLatch(5);
      storageTask.path = item;
      storageTask.countdownLatch = &countdownLatch;
      storageTask.fileID = counter;
      GlobalReadPipelinePtr->addTask(&storageTask);
      countdownLatch.wait();

//            /***********************************************/
//            uint64_t end = StopCounter;
//            uint64_t start;
//            if(end > 19){
//                start = end - 19;
//            }else{
//                start = 0;
//            }
//            GlobalMetadataManagerPtr->validSize(start, end);
//            /***********************************************/
      ++counter;

      gettimeofday(&t1, NULL);
      float totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
      printf("Task duration:%fs, Task Size:%lu, Speed:%fMB/s\n", totalDuration, storageTask.length,
             (float) storageTask.length / totalDuration / 1024 / 1024);
      printf("read duration:%luus, chunk duration:%luus, dedup duration:%luus, write duration:%luus\n",
             storageTask.stageTimePoint.read, storageTask.stageTimePoint.chunk, storageTask.stageTimePoint.dedup,
             storageTask.stageTimePoint.write);
      GlobalReadPipelinePtr->getStatistics();
      GlobalChunkingPipelinePtr->getStatistics();
      GlobalDeduplicationPipelinePtr->getStatistics();
      GlobalMetadataManagerPtr->SimilarHitStatistics();
      totalSize += storageTask.length;
      printf("----------------------------------------------\n");
    }
        gettimeofday(&total1, NULL);
        float duration = (float) (total1.tv_sec - total0.tv_sec) + (float) (total1.tv_usec - total0.tv_usec) / 1000000;

        printf("==============================================\n");
        printf("Total duration:%fs, Total Size:%lu, Speed:%fMB/s\n", duration, totalSize,
               (float) totalSize / duration / 1024 / 1024);
        GlobalDeduplicationPipelinePtr->getStatistics();
        GlobalMetadataManagerPtr->SimilarHitStatistics();
        printf("done\n");
        //printf("ChunkDuration:%lu\nDedupDuration:%lu\nHashDuration:%lu\nReadDuration:%lu\nWriteDuration:%lu\n", ChunkDuration, DedupDuration, HashDuration, ReadDuration, WriteDuration);
        printf("==============================================\n");

        delete GlobalReadPipelinePtr;
        delete GlobalChunkingPipelinePtr;
        delete GlobalHashingPipelinePtr;
        delete GlobalDeduplicationPipelinePtr;
        delete GlobalWriteFilePipelinePtr;
        delete GlobalMetadataManagerPtr;
    } else {
        printf("Usage: Odess [run type] [args..]\n");
        printf("Put file: Odess --task=put --Path=[path]\n");
        printf("Get file: Odess --task=get --basePath=[basepath] --RestorePath=[restore path]\n");
        printf("Status: Odess --task=status\n");
        printf("use --ChunkFilePath=[chunk file path] to specify the chunk files storage path, default path is /home/zxy/OdessHome/chunkFiles\n");
        printf("use --LogicFilePath=[logic file path] to specify the logic files storage path, default path is /home/zxy/OdessHome/logicFiles\n");
    }
}