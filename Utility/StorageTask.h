//
// Created by BorelsetR on 2019/7/23.
//

#ifndef ODESS_COMPRESSIONTASK_H
#define ODESS_COMPRESSIONTASK_H

#include "Chunk.h"
#include "Lock.h"
#include <list>
#include <tuple>

enum class TaskType {
    Compression,
    Decompression,
};

struct WriteChunkHead {
    uint8_t type;
    uint64_t fp;
    uint64_t pos;
    uint64_t length;
};

struct SHA1FP {
    //std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t> fp;
    uint64_t fp1;
    uint32_t fp2, fp3, fp4;

    void print(){
        printf("%lu:%d:%d:%d\n", fp1, fp2, fp3, fp4);
    }
};

struct WriteHead {
    SHA1FP fp;
    SHA1FP bfp;
    uint64_t type:2;
    uint64_t length:62;
};

/*
bool operator == (const SHA1FP& left, const SHA1FP& right)
{
    if(left.fp[0] == right.fp[0] &&
            left.fp[1] == right.fp[1] &&
            left.fp[2] == right.fp[2] &&
            left.fp[3] == right.fp[3] &&
            left.fp[4] == right.fp[4]){
        return true;
    }else{
        return false;
    }
}

bool operator < (const SHA1FP& left, const SHA1FP& right)
{
    for(int i=0; i<5; i++){
        if(left.fp[i] < right.fp[i]){
            return true;
        }
    }
    return false;
}
 */

struct SFSet {
    uint64_t sf[12];
    uint64_t feature[48];
};

struct StageTimePoint {
    uint64_t read = 0;
    uint64_t chunk = 0;
    uint64_t dedup = 0;
    uint64_t write = 0;
};

struct Location {
    uint64_t fid;
    uint64_t pos;
    uint32_t length;
    uint32_t oriLength;
    uint64_t bfid = -1;
    uint64_t bpos = -1;
    uint32_t blength = -1;
    SHA1FP baseFP;
};

struct BlockHead {
    SHA1FP sha1Fp;
    uint8_t type;
    uint32_t length;
    SHA1FP baseFP;
};


struct CacheBlock {
    uint8_t *block;
    uint64_t type: 1;
    uint64_t length: 63;
    SHA1FP baseFP;
};

struct DedupTask {
    uint8_t *buffer;
    uint64_t pos;
    uint64_t length;
    SHA1FP fp;
    uint64_t fileID;
    StageTimePoint *stageTimePoint;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
    uint64_t type;
    bool inCache = false;
    bool rejectDelta = false;
    bool rejectDedup = false;
    uint64_t baseCid;
    uint64_t cid;
};

struct WriteTask {
    int type;
    Location location;
    uint8_t *buffer;
    uint64_t pos;
    uint64_t bufferLength;
    uint8_t *deltaBuffer;
    uint64_t deltaBufferLength;
    uint64_t fileID;
    SHA1FP sha1Fp;
    uint64_t sf[12];
    SHA1FP baseFP;
    StageTimePoint *stageTimePoint;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
};

struct ChunkTask {
    uint8_t *buffer = nullptr;
    uint64_t length;
    uint64_t fileID;
    uint64_t end;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
};

struct StorageTask {
    std::string path;
    uint8_t *buffer = nullptr;
    uint64_t length;
    uint64_t fileID;
    uint64_t end;
    CountdownLatch *countdownLatch = nullptr;
    StageTimePoint stageTimePoint;

    void destruction() {
        if (buffer) free(buffer);
    }
};

struct RestoreTask {
    int recipeID;
    std::string outputPath;
    CountdownLatch *countdownLatch = nullptr;
};


struct RecipeUnit{
    WriteHead writeHead;
    Location location;
};

#endif //ODESS_COMPRESSIONTASK_H
