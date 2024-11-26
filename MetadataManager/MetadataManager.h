//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_MATADATAMANAGER_H
#define ODESSNEW_MATADATAMANAGER_H

#include <map>
#include "../Utility/StorageTask.h"
#include "../FeatureMethod/NFeatureSample.h"
#include <unordered_map>
#include <unordered_set>

extern std::string LogicFilePath;

uint64_t shadMask = 0x7;

int ReplaceThreshold = 10;

struct TupleHasher {
    std::size_t
    operator()(const SHA1FP &key) const {
        return key.fp1;
    }
};

struct TupleEqualer {
    bool operator()(const SHA1FP &lhs, const SHA1FP &rhs) const {
        return lhs.fp1 == rhs.fp1 && lhs.fp2 == rhs.fp2 && lhs.fp3 == rhs.fp3 && lhs.fp4 == rhs.fp4;
    }
};

bool operator==(const SHA1FP &lhs, const SHA1FP &rhs) {
  return lhs.fp1 == rhs.fp1 && lhs.fp2 == rhs.fp2 && lhs.fp3 == rhs.fp3 && lhs.fp4 == rhs.fp4;
}

struct SFEntry {
    SHA1FP fp;
    uint64_t fid;
    uint64_t length;
    uint64_t offset;
};

struct TempEntry {
    SFEntry sfEntry;
    int counter;
};

struct FPEntry {
    Location location;
    SFSet sfSet;
};

class MetadataManager {
public:
    MetadataManager() {

    }

    void getSize() {

    }

    void addRecord(const SHA1FP &sha1Fp, const Location &location, SFSet *sfSet) {
      {
        MutexLockGuard mutexLockGuard(tableLock);
        table[sha1Fp] = {location, *sfSet};
      }

      //assert(location.length > 1024);

      /*
      SFtable[sha1Fp.fp] = {
              sf1, sf2, sf3
      };
       */
      SFEntry sfEntry;
      sfEntry.fp = sha1Fp;
      sfEntry.length = location.length;
      sfEntry.offset = location.pos;
      sfEntry.fid = location.fid;

      for (int i = 0; i < SF_NUMBER; i++) {
        sfTable[i][sfSet->sf[i]].push_back(sfEntry);
      }
    }

    void addRecordOriPos(const SHA1FP &sha1Fp, const Location &location, uint64_t pos, uint64_t length, uint64_t fid,
                         SFSet *sfSet) {
      {
        MutexLockGuard mutexLockGuard(tableLock);
        table[sha1Fp] = {location, *sfSet};
      }

      //assert(location.length > 1024);

      /*
      SFtable[sha1Fp.fp] = {
              sf1, sf2, sf3
      };
       */
      SFEntry sfEntry;
      sfEntry.fp = sha1Fp;
      sfEntry.length = length;
      sfEntry.offset = pos;
      sfEntry.fid = fid;

      for (int i = 0; i < SF_NUMBER; i++) {
        sfTable[i][sfSet->sf[i]].push_back(sfEntry);
      }
    }

    void addRecordNotFeature(const SHA1FP &sha1Fp, const Location &location, SFSet *sfSet) {
      {
        MutexLockGuard mutexLockGuard(tableLock);
        table[sha1Fp] = {location, *sfSet};
      }
    }

    int findRecord(const SHA1FP &sha1Fp, FPEntry *fpEntry) {
      uint64_t mapIndex = shadMask & sha1Fp.fp1;
      MutexLockGuard mutexLockGuard(tableLock);
      auto iter = table.find(sha1Fp);
      if (iter != table.end()) {
        *fpEntry = iter->second;
        return 1;
      } else {
        return 0;
      }
    }

    int findSimilarity(SFSet *sfSet, Location *location, FPEntry *fpEntry, SHA1FP *sha1Fp, int *order) {
      for (int i = 0; i < SF_NUMBER; i++) {
        auto iter = sfTable[i].find(sfSet->sf[i]);
        if (iter != sfTable[i].end()) {
          const SFEntry &se = *iter->second.begin();
          location->fid = se.fid;
          location->length = se.length;
          location->pos = se.offset;
          location->oriLength = se.length - sizeof(BlockHead);

          *sha1Fp = se.fp;

          auto iterfp = table.find(se.fp);
          assert(iterfp != table.end());
          *fpEntry = iterfp->second;

          *order = i;
          return 1;
        }
      }
      return 0;
    }

    int findSimilarity_bestfit(SFSet *sfSet, Location *location, FPEntry *fpEntry, SHA1FP *sha1Fp, int *order) {
      std::unordered_map<SHA1FP, TempEntry, TupleHasher, TupleEqualer> tempCounter;
      for (int i = 0; i < SF_NUMBER; i++) {
        auto iter = sfTable[i].find(sfSet->sf[i]);
        if (iter != sfTable[i].end()) {
          for (auto item: iter->second) {
            auto sfIter = tempCounter.find(item.fp);
            if (sfIter != tempCounter.end()) {
              sfIter->second.counter++;
            } else {
              tempCounter[item.fp] = {item, 1};
            }
          }
        }
      }

      int maxCounter = 0;
      std::unordered_map<SHA1FP, TempEntry, TupleHasher, TupleEqualer>::iterator resultIter;
      for (auto i = tempCounter.begin(); i != tempCounter.end(); i++) {
        if (i->second.counter > maxCounter) {
          maxCounter = i->second.counter;
          resultIter = i;
        }
      }

      if (maxCounter == 0) return 0;

      const SFEntry &se = resultIter->second.sfEntry;
      location->fid = se.fid;
      location->length = se.length;
      location->pos = se.offset;
      location->oriLength = se.length - sizeof(BlockHead);

      *sha1Fp = se.fp;

      auto iterfp = table.find(se.fp);
      assert(iterfp != table.end());
      *fpEntry = iterfp->second;

      *order = maxCounter - 1;
      return 1;
    }

    void SimilarHitStatistics() {

    }

    void validSize(uint64_t start, uint64_t end) {
      uint8_t *pathBuffer = (uint8_t *) malloc(256);
      std::unordered_map<SHA1FP, uint64_t, TupleHasher, TupleEqualer> validTable;
      for (uint64_t i = start; i <= end; i++) {
        sprintf((char *) pathBuffer, LogicFilePath.c_str(), i);
            FileOperator recipeFile((char*)pathBuffer, FileOpenType::Read);
            uint64_t size = recipeFile.getSize();
            uint8_t* recipeBuffer = (uint8_t*)malloc(size);
            recipeFile.read(recipeBuffer, size);
            uint64_t count = size / (sizeof(WriteHead) + sizeof(Location));

            RecipeUnit* recipeUnit = (RecipeUnit*)recipeBuffer;
            for(int n=0; n<count; n++){
                WriteHead& wh = recipeUnit[n].writeHead;
                Location& loc = recipeUnit[n].location;
                 if(wh.type){
                    validTable.emplace(wh.fp, loc.length-sizeof(BlockHead));
                    validTable.emplace(wh.bfp, loc.blength-sizeof(BlockHead));
                }else{
                    validTable.emplace(wh.fp, loc.length-sizeof(BlockHead));
                }
            }

            free(recipeBuffer);
        }
        free(pathBuffer);
        uint64_t totalLength = 0;
        for(const auto& entry: validTable){
            totalLength += entry.second;
        }
        printf("Version from %lu to %lu, size:%lu\n", start, end, totalLength);
    }


private:
    std::unordered_map<SHA1FP, FPEntry, TupleHasher, TupleEqualer> table;
    //std::map<std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t>, SFSet> SFtable;
    std::unordered_map<uint64_t, std::list<SFEntry>> sfTable[12];

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //ODESSNEW_MATADATAMANAGER_H