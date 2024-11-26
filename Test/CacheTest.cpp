//
// Created by Borelset on 2020/2/19.
//
#include <vector>
#include <list>
#include <string.h>
#include <cassert>
#include "../MetadataManager/ChunkCache.h"

int main(){
    ChunkCache cache;
    SHA1FP buffer;
    CacheBlock cacheBlock;
    SHA1FP sha1Fp1 = {
            1, 2, 3, 4,
    };
    SHA1FP sha1Fp2 = {
            5, 6, 7, 8,
    };
    SHA1FP sha1Fp3 = {
            9, 10, 11, 12,
    };
    cache.addRecord(sha1Fp1, (uint8_t*)&buffer, sizeof(buffer));
    cache.addRecord(sha1Fp2, (uint8_t*)&buffer, sizeof(buffer));

    int r = cache.getRecord(sha1Fp1, &cacheBlock);
    printf("r:%d\n", r);
    cache.addRecord(sha1Fp1, (uint8_t*)&buffer, sizeof(buffer));
    r = cache.getRecord(sha1Fp1, &cacheBlock);
    printf("r:%d\n", r);
}
