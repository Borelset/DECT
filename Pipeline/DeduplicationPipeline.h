//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_DEDUPLICATIONPIPELINE_H
#define ODESSNEW_DEDUPLICATIONPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "../MetadataManager/MetadataManager.h"
//#include "../MetadataManager/MetadataManagerPersist.h"
#include "WriteFilePipeline.h"
#include "../FeatureMethod/FinesseFeature.h"
#include "../FeatureMethod/NFeature.h"
#include "../FeatureMethod/NFeatureSkip.h"
#include "../FeatureMethod/NFeatureSample.h"
#include "../EncodingMethod/EDelta.h"
#include "../EncodingMethod/EncodingMethod.h"
#include "../EncodingMethod/dDelta/dDelta.h"
#include "../EncodingMethod/xdelta3.h"
#include "../EncodingMethod/zdlib.h"
#include <assert.h>
#include "../FeatureMethod/FinesseSkip.h"
#include "../Utility/ChunkFileManager.h"
#include "../MetadataManager/ChunkCache.h"
#include "../MetadataManager/HarContainerManager.h"
#include "../Analysis/FDistributionSolver.h"
#include "../Analysis/SFDistributionSolver.h"
#include "../MetadataManager/BloomFilter.h"


struct DeltaArgs {
    uint32_t BnTLength: 24;
    uint32_t MatchFeature: 8;
};

extern std::vector<std::string> FilePaths;

uint64_t DedupDuration = 0;

DEFINE_string(FeatureMethod, "NTransformSa", "Feature method");
DEFINE_string(FeatureHash, "Gear", "Hash method in feature");
DEFINE_bool(delta, true, "whether delta compression");
DEFINE_bool(dedup, true, "");
DEFINE_double(SimilarRatio, 0.2, "");
DEFINE_int32(Features, 12, "");
DEFINE_string(DeltaEncodingMethod, "xDelta", "");
DEFINE_uint64(CappingThreshold,
              80, "CappingThreshold");

std::string finesseStr("Finesse");
std::string ntransStr("NTransform");
std::string ntransskipStr("NTransformSk");
std::string ntranssampleStr("NTransformSa");
std::string finesseskipStr("FinesseSk");
std::string gearStr("Gear");
std::string rabinStr("Rabin");
std::string xDeltaStr("xDelta");
std::string zDeltaStr("zDelta");

const int PreLoadSize = 2 * 1024 * 1024;

enum class DeltaEncoding {
    xDelta,
    zDelta,
};

bool CappingOrder(const std::pair<uint64_t, uint64_t> first, std::pair<uint64_t, uint64_t> second) {
  return first.second > second.second;
}

//#define BFINDEX

class DeduplicationPipeline {
public:
    DeduplicationPipeline()
            : taskAmount(0),
              runningFlag(true),
              mutexLock(),
              condition(mutexLock),
              flowCondition(flowLock),
              totalBeforeDelta(0),
              totalAfterDelta(0),
              totalRatio(0.0),
              count(0) {

      if (FLAGS_FeatureHash == gearStr) {
        rollHash = new Gear();
      } else if (FLAGS_FeatureHash == rabinStr) {
        rollHash = new Rabin();
      } else {
        printf("invalid hash method\n");
        exit(0);
      }
      detectMethod = new NFeatureSample(FLAGS_Features, rollHash);
      matrix = detectMethod->getMatrix();
      transA = detectMethod->getTransA();
      transB = detectMethod->getTransB();
      worker = new std::thread(std::bind(&DeduplicationPipeline::deduplicationWorkerCallback, this));

      if (FLAGS_DeltaEncodingMethod == zDeltaStr) {
        deltaEncoding = DeltaEncoding::zDelta;
      } else if (FLAGS_DeltaEncodingMethod == xDeltaStr) {
        deltaEncoding = DeltaEncoding::xDelta;
      }

      parameters.projected_element_count = 10000000;
      parameters.false_positive_probability = 0.0001;
      parameters.compute_optimal_parameters();
      FPBF = new bloom_filter(parameters);

      parameters.projected_element_count = 10000000 * SF_NUMBER;
      parameters.compute_optimal_parameters();
      SFBF = new bloom_filter(parameters);

    }

    int addTask(const DedupTask &dedupTask) {
      MutexLockGuard mutexLockGuard(mutexLock);
      receiceList.push_back(dedupTask);
      taskAmount++;
      condition.notifyAll();
      return 0;
    }

    ~DeduplicationPipeline() {

      // todo worker destruction
      delete detectMethod;
      delete rollHash;

      runningFlag = false;
      condition.notifyAll();
      worker->join();

      delete FPBF;
      delete SFBF;
    }

    void getStatistics() {
      printf("Prediction Analysis:\n");
//      for (int i = 0; i < SF_NUMBER; i++) {
//        printf("SF %d fit:\n", i);
//        uint64_t mm = matchNumber[i];
//        double aver = (double) Sum[i] / matchNumber[i];
//        double sqre = (double) (SquareSum[i] - aver * aver * mm) / (mm - 1);
//        printf("Number:%lu, average:%f, sqre:%f\n", mm, aver, sqrt(sqre));
//      }
//      for (int i = 0; i <= SF_NUMBER * F_NUMBER; i++) {
//        printf("matched %d feature: %lu\n", i, matchFeature[i]);
//      }
      for (int i = 0; i < SF_NUMBER; i++) {
        printf("Matched %d SF: %lu\n", i, matchSFFeature[i]);
      }
      uint64_t estimateReduce = 0;
//      FDistributionSolver distributionSolver(SF_NUMBER * F_NUMBER, matchFeature);
      struct timeval tSolver0, tSolver1;
      gettimeofday(&tSolver0, NULL);
      SFDistributionSolver sfDistributionSolver(F_NUMBER * SF_NUMBER, SF_NUMBER, matchSFFeature, 0);
      for (const auto &item: deltaArgsList) {
//        estimateReduce += (double) (item.BnTLength) / (1.0 / distributionSolver.getFtoSim(item.MatchFeature) + 1.0);
        estimateReduce += (double) (item.BnTLength) / (1.0 / sfDistributionSolver.getFtoSim(item.MatchFeature) + 1.0);
      }
      gettimeofday(&tSolver1, NULL);
      printf("Solver :%lu\n", (tSolver1.tv_sec - tSolver0.tv_sec) * 1000000 + (tSolver1.tv_usec - tSolver0.tv_usec));
      printf("Delta Encoding Estimate reduce size: %lu, Delta Encoding Ratio: %f\n", estimateReduce,
             (float) estimateReduce / totalBeforeDelta);
#ifdef BFINDEX
      printf("CLDedup Reduce:%lu\n", CLDedupReduce);
#endif
    }

    int setMaxChunkSize(int chunkSize) {
      MaxChunkSize = chunkSize;
      return 0;
    }

private:
    void deduplicationWorkerCallback() {
      uint64_t segmentLength = 0;
      uint64_t SegmentThreshold = 20 * 1024 * 1024;
      std::list<DedupTask> detectList;

      while (likely(runningFlag)) {
        {
          MutexLockGuard mutexLockGuard(mutexLock);
          while (!taskAmount) {
            condition.wait();
            if (unlikely(!runningFlag)) break;
          }
          if (unlikely(!runningFlag)) continue;
          //printf("get task\n");
          taskAmount = 0;
          condition.notify();
          taskList.swap(receiceList);
        }


        for (const auto &dedupTask: taskList) {
          detectList.push_back(dedupTask);
          segmentLength += dedupTask.length;
          if (segmentLength > SegmentThreshold || dedupTask.countdownLatch) {

            doDedup(detectList);

            segmentLength = 0;
            detectList.clear();
          }
        }


        taskList.clear();
      }

    }

    void doDedup(std::list<DedupTask> &wl) {
      uint8_t *baseBuffer = (uint8_t *) malloc(1024);
      uint64_t baseBufferLength = 1024;

      for (auto &entry: wl) {
        WriteTask writeTask;
        SFSet features;
        SHA1FP cacheSha1;
        FPEntry fpEntry;
        int order;
        memset(&writeTask, 0, sizeof(WriteTask));
        writeTask.fileID = entry.fileID;
        writeTask.stageTimePoint = entry.stageTimePoint;
        writeTask.index = entry.index;
        uint8_t *currentTask = entry.buffer + entry.pos;

        int result = 0;

        totalSize += entry.length;
        totalChunk++;

#ifdef BFINDEX
        // BF CLDedup Check
        int BFResult_C = 0;
        int BFResult_D = 0;

        BFResult_C = FPBF->contains(entry.fp);
        if(BFResult_C){
          CLDedupReduce += entry.length;
        }else{
          FPBF->insert(entry.fp);

          detectMethod->setTotalLength(entry.length);
          detectMethod->detect(currentTask, entry.length);
          detectMethod->getResult(&features);
          for(int i=0; i<SF_NUMBER; i++){
            BFResult_D = SFBF->contains(features.sf[i]);
            if(BFResult_D) {
              matchSFFeature[i]++;
              deltaArgsList.push_back({(uint32_t) (entry.length + totalSize/totalChunk*0.75), (uint32_t) i});  // magic number 0.78
              break;
            }
          }
#ifdef DELTACHAIN
          if(BFResult_C){
#endif
            for(int i=0; i<SF_NUMBER; i++){
              SFBF->insert(features.sf[i]);
            }
#ifdef DELTACHAIN
          }
#endif
        }
        // BF CLDedup Check
#endif

        if (likely(FLAGS_dedup)) {
          result = GlobalMetadataManagerPtr->findRecord(entry.fp, &fpEntry);
#ifdef BFINDEX
          result = BFResult_C;
#endif
        }

        if (result) {
          writeTask.location = fpEntry.location;
          writeTask.sha1Fp = entry.fp;
          writeTask.type = 0;
          writeTask.bufferLength = entry.length;
          writeTask.buffer = entry.buffer;
          dedup++;
        } else {
          memset(featureBuffer, 0, sizeof(uint64_t) * SF_NUMBER * F_NUMBER);
          uint64_t hashValue = 0;
          for (uint64_t i = 0; i < entry.length; i++) {
            hashValue = (hashValue << FLAGS_OdessShiftBits) + matrix[*(currentTask + i)];
            if (!(hashValue & 0x0000400303410000)) {
              for (int j = 0; j < SF_NUMBER * F_NUMBER; j++) {
                uint64_t transResult = (hashValue * transA[j] + transB[j]);
                if (transResult > featureBuffer[j])
                  featureBuffer[j] = transResult;
              }
            }
          }
          for (int i = 0; i < SF_NUMBER; i++) {
            features.sf[i] = XXH64(&featureBuffer[i * F_NUMBER], sizeof(uint64_t) * F_NUMBER, 0x7fcaf1);
          }

          if (likely(FLAGS_delta))
            result = GlobalMetadataManagerPtr->findSimilarity(&features, &writeTask.location, &fpEntry, &cacheSha1,
                                                              &order);
#ifdef BFINDEX
          result = BFResult_D;
#endif
          if (result) {
//            int matched = 0;
//            for (int i = 0; i < SF_NUMBER * F_NUMBER; i++) {
//              if (features.feature[i] == fpEntry.sfSet.feature[i]) matched++;
//            }
//            matchFeature[matched]++;
#ifndef BFINDEX
            int matched = order;
            matchSFFeature[matched]++;
#endif

            writeTask.buffer = entry.buffer;
            writeTask.bufferLength = entry.length;
            writeTask.pos = entry.pos;
//            writeTask.deltaBufferLength = 0;
            writeTask.baseFP = cacheSha1;
            writeTask.sha1Fp = entry.fp;
            for (int i = 0; i < SF_NUMBER; i++) {
              writeTask.sf[i] = features.sf[i];
            }

#ifndef BFINDEX
            deltaArgsList.push_back({(uint32_t) (entry.length + writeTask.location.length), (uint32_t) matched});
#endif

            count++;

#ifdef DELTACHAIN
            // use all chunks as possible bases
            GlobalMetadataManagerPtr->addRecordOriPos(writeTask.sha1Fp, writeTask.location, entry.pos, entry.length,
                                                      entry.fileID, &features);
#endif

#ifndef DELTACHAIN
            // use normal chunks as possible bases
            GlobalMetadataManagerPtr->addRecordNotFeature(writeTask.sha1Fp, writeTask.location, &features);
#endif

            delta++;

            totalBeforeDelta += entry.length;
            totalAfterDelta += entry.length;
            writeTask.type = 1;
            writeTask.buffer = entry.buffer;
            writeTask.pos = entry.pos;
            writeTask.bufferLength = entry.length;
            writeTask.sha1Fp = entry.fp;
            for (int i = 0; i < SF_NUMBER; i++) {
              writeTask.sf[i] = features.sf[i];
            }
            uint32_t locLength = writeTask.bufferLength + sizeof(BlockHead);
            uint32_t oriLength = writeTask.bufferLength;

            Location tempLoc;
            tempLoc.fid = fid;
            tempLoc.pos = currentLength;
            tempLoc.length = locLength;
            tempLoc.oriLength = oriLength;

          } else {
            unique:
            totalBeforeDelta += entry.length;
            totalAfterDelta += entry.length;
            writeTask.type = 2;
            writeTask.buffer = entry.buffer;
            writeTask.pos = entry.pos;
            writeTask.bufferLength = entry.length;
            writeTask.sha1Fp = entry.fp;
            for (int i = 0; i < SF_NUMBER; i++) {
              writeTask.sf[i] = features.sf[i];
            }

            uint32_t locLength = writeTask.bufferLength + sizeof(BlockHead);
            uint32_t oriLength = writeTask.bufferLength;

            Location tempLoc;
            tempLoc.fid = fid;
            tempLoc.pos = currentLength;
            tempLoc.length = locLength;
            tempLoc.oriLength = oriLength;


            GlobalMetadataManagerPtr->addRecordOriPos(writeTask.sha1Fp, tempLoc, entry.pos, entry.length, entry.fileID,
                                                      &features);

            unique++;

          }
        }

        if (unlikely(entry.countdownLatch)) {
          writeTask.countdownLatch = entry.countdownLatch;
          entry.countdownLatch->countDown();
          printf("unique:%lu, dedup:%lu, delta:%lu, denyDedup:%lu, denyDelta:%lu, selectDelta:%lu\n", unique,
                 dedup, delta,
                 denyDedup, denyDelta, selectDelta);
          preloadSkip = 0;
          printf("dedup duration:%lu\n", duration);
//          cache.clear();
          fid++;
          currentLength = 0;
          duration = 0;
          GlobalWriteFilePipelinePtr->addTask(writeTask);
          printf("dedup done\n");
        } else {
          if (writeTask.type == 1) {
            currentLength += writeTask.deltaBufferLength + sizeof(BlockHead);
          } else if (writeTask.type == 2) {
            currentLength += writeTask.bufferLength + sizeof(BlockHead);
          }
          if (currentLength >= ContainerSize) {
            currentLength = 0;
//            cache.localRelease(fid);
            fid++;
          }
          GlobalWriteFilePipelinePtr->addTask(writeTask);
        }
        writeTask.countdownLatch = nullptr;
      }
      free(baseBuffer);
    }

    RollHash *rollHash;
    NFeatureSample *detectMethod;
    std::thread *worker;
    std::list<DedupTask> taskList;
    std::list<DedupTask> receiceList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;
//    ChunkCache cache;

    uint64_t totalBeforeDelta;
    uint64_t totalAfterDelta;
    float totalRatio;
    uint64_t count;

    uint64_t preloadSkip = 0;

    DeltaEncoding deltaEncoding;

    uint64_t unique = 0, dedup = 0, delta = 0;

    uint64_t cacheHit = 0;
    uint64_t cacheMiss = 0;

    MutexLock flowLock;
    Condition flowCondition;

    uint64_t totalSize = 0;
    uint64_t totalChunk = 0;

    int MaxChunkSize;

    uint64_t duration = 0;

    uint64_t fid = 0;
    uint64_t currentLength = 0;

    uint64_t denyDedup = 0, denyDelta = 0, selectDelta = 0;

//    double Sum[SF_NUMBER] = {0.0}, SquareSum[SF_NUMBER] = {0.0};
//    uint64_t matchNumber[SF_NUMBER] = {0};

    int64_t matchFeature[SF_NUMBER * F_NUMBER + 1] = {0};
    int64_t matchSFFeature[SF_NUMBER] = {0};
    std::list<DeltaArgs> deltaArgsList;


    uint64_t CLDedupReduce = 0;

    bloom_parameters parameters;

    bloom_filter *FPBF = nullptr;
    bloom_filter *SFBF = nullptr;

    uint64_t *matrix = nullptr;
    int *transA = nullptr;
    int *transB = nullptr;

    uint64_t featureBuffer[SF_NUMBER * F_NUMBER];


//    double matchReductionSum[SF_NUMBER * F_NUMBER + 1] = {0.0};
//    double matchReductionDSum[SF_NUMBER * F_NUMBER + 1] = {0.0};

};

static DeduplicationPipeline *GlobalDeduplicationPipelinePtr;

#endif //ODESSNEW_DEDUPLICATIONPIPELINE_H
