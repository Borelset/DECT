//
// Created by BorelsetR on 2019/8/2.
//


#include "jemalloc/jemalloc.h"
#include "MetadataManager.h"
#include "../Utility/Lock.h"
#include <set>

#ifndef ODESSSTORAGE_CHUNKCACHE_H
#define ODESSSTORAGE_CHUNKCACHE_H

DEFINE_uint64(CacheSize, 128, "number of containers");

uint64_t mallocSize = 0, freeSize = 0;

struct ContainerCache{
    uint64_t lastVisit;
    std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> chunkTable;
};

struct CachedChunk{
    uint8_t* buffer = nullptr;
    uint64_t type:1;
    uint64_t length:63;

    int set(const CacheBlock& cacheBlock){
        buffer = (uint8_t*)malloc(cacheBlock.length);
        mallocSize += cacheBlock.length;
        memcpy(buffer, cacheBlock.block, cacheBlock.length);
        type = cacheBlock.type;
        length = cacheBlock.length;
      return 0;
    }

    ~CachedChunk(){
        if(buffer) free(buffer);
        freeSize += length;
    }
};

//const uint64_t TotalItemThreshold = 65536;

class ChunkCache {
public:
    uint64_t ContainerThreshold = 0;
    ChunkCache() : totalSize(0), index(0), write(0), read(0) {
        /*
        for(int i=0; i<TotalItemThreshold; i++){
            uint8_t* tempBuffer = (uint8_t*)malloc(65536);
            assert(tempBuffer != nullptr);
            CacheBlock cacheBlock = {
                    tempBuffer, 0, 0
            };
            idleList.push_back(cacheBlock);
        }
         */
        ContainerThreshold = FLAGS_CacheSize;
    }

    void statistics(){
        printf("total size:%lu, index:%lu\n", totalSize, index);
        printf("cache write:%lu, cache read:%lu\n", write, read);
    }

    void clear(){
        localRelease(-1);
        printf("random seeks:%lu\n", prefetchingTimes);
        printf("prefetchingSize: %lu\n", prefetchingSize);
        printf("average prefetching time:%f\n", (float) prefetchingDuration / prefetchingTimes);
        printf("malloc size:%lu, free size:%lu\n", mallocSize, freeSize);
        for (const auto &container: containerCache) {
            for (const auto &entry: container.second.chunkTable) {
                for (auto &item: entry.second) {
                    free(item.block);
                    freeSize += item.length;
                }
            }
        }
        containerCache.clear();
        lruList.clear();
        index = 0;
        totalSize = 0;
        counter = 0;
        prefetchingSize = 0;
        prefetchingTimes = 0;
        prefetchingDuration = 0;
    }

    ~ChunkCache(){
        statistics();
        clear();
    }

    uint64_t getPreloadSize(){
        return prefetchingSize;
    }

    void PrefetchingContainer(int fid){
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);
        prefetchingTimes++;

        uint8_t *preloadBuffer = (uint8_t *) malloc(BufferCapacity);
        uint8_t *decompressBuffer = (uint8_t *) malloc(BufferCapacity);
        mallocSize += BufferCapacity * 2;
        int readSize = -1;
        {
            FileOperator *fileOperatorPtr = ChunkFileManager::get(fid);
            uint64_t cSize = fileOperatorPtr->getSize();
            uint64_t decompressSize = fileOperatorPtr->read(decompressBuffer, cSize);
            fileOperatorPtr->releaseBufferedData();
            delete fileOperatorPtr;

            readSize = ZSTD_decompress(preloadBuffer, BufferCapacity, decompressBuffer, decompressSize);
            assert(!ZSTD_isError(readSize));
            free(decompressBuffer);
        }

        prefetchingSize += readSize;
        int preLoadPos = 0;
        BlockHead *headPtr;

        auto iter = containerCache.find(fid);
        assert(iter == containerCache.end());
        ContainerCache tempCache;
        uint64_t tableSize = 0;

        while (preLoadPos < readSize) {
            headPtr = (BlockHead *) (preloadBuffer + preLoadPos);
            assert(preLoadPos + sizeof(BlockHead) < readSize - 1);

            uint8_t *cacheBuffer = (uint8_t *) malloc(headPtr->length - sizeof(BlockHead));
            mallocSize += headPtr->length - sizeof(BlockHead);
            memcpy(cacheBuffer, preloadBuffer + preLoadPos + sizeof(BlockHead), headPtr->length - sizeof(BlockHead));

            if (headPtr->type) {
                CacheBlock insertItem = {cacheBuffer, headPtr->type, headPtr->length - sizeof(BlockHead),
                                         headPtr->baseFP};
                tempCache.chunkTable[headPtr->sha1Fp].push_back(insertItem);
            } else {
                CacheBlock insertItem = {cacheBuffer, headPtr->type, headPtr->length - sizeof(BlockHead)};
                tempCache.chunkTable[headPtr->sha1Fp].push_back(insertItem);
            }

            tableSize += headPtr->length - sizeof(BlockHead);

            preLoadPos += headPtr->length;
        }
        if(preLoadPos != readSize){
            printf("fid:%d preLoadPos:%lu readSize:%lu\n", fid, preLoadPos, readSize);
            assert(preLoadPos == readSize);
        }
        tempCache.lastVisit = index;
        {
            MutexLockGuard cacheLockGuard(cacheLock);
            containerCache[fid] = tempCache;
            {
                MutexLockGuard lruLockGuard(lruLock);
                lruList[index] = fid;
                index++;
                counter ++;
                totalSize += tableSize;
                write += tableSize;
            }
            kick();
        }

        free(preloadBuffer);
        freeSize += ContainerSize*2;
        gettimeofday(&t1, NULL);
        prefetchingDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
    }

    void addLocalCache(const SHA1FP &sha1Fp, uint8_t *buffer, uint64_t length){
        uint8_t *cacheBuffer = (uint8_t *) malloc(length);
        mallocSize += length;
        memcpy(cacheBuffer, buffer, length);
        CacheBlock insertItem = {
                cacheBuffer, 0, length
        };
        localCache[sha1Fp].push_back(insertItem);
        localSize += length;
        write += length;
    }

    void localRelease(uint64_t fid){
        ContainerCache tempCache;
        tempCache.lastVisit = index;
        tempCache.chunkTable.swap(localCache);

        {
            MutexLockGuard cacheLockGuard(cacheLock);
            containerCache[fid] = tempCache;
            totalSize += localSize;
            {
                MutexLockGuard lruLockGuard(lruLock);
                lruList[index] = fid;
                index++;
                counter++;
            }
            kick();
        }
        localCache.clear();
        localSize = 0;
    }

    int getLocalCache(const SHA1FP &sha1Fp, CachedChunk *cachedChunk, SHA1FP *baseFP) {
        if (baseFP == nullptr) {
            auto iter = localCache.find(sha1Fp);
            if (iter != localCache.end()) {
                for (auto &item: iter->second) {
                    if (item.type == 0) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        return 1;
                    }
                }
            } else {
                return 0;
            }
        } else {
            auto iter = localCache.find(sha1Fp);
            if (iter != localCache.end()) {
                for (auto &item: iter->second) {
                    if (item.baseFP == *baseFP) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        return 1;
                    }
                }
            } else {
                return 0;
            }
        }

      return 0;
    }

    int tryGetLocalCache(const SHA1FP &sha1Fp) {
        auto iter = localCache.find(sha1Fp);
        if (iter != localCache.end()) {
            for (auto &item: iter->second) {
                if (item.type == 0) {
                    return 1;
                }
            }
        } else {
            return 0;
        }
    }

    void kick() {
        MutexLockGuard lruLockGuard(lruLock);
        while (counter > ContainerThreshold) {
            auto iterLru = lruList.begin();
            assert(iterLru != lruList.end());
            auto iterCache = containerCache.find(iterLru->second);
            assert(iterCache != containerCache.end());
            for (auto &entry: iterCache->second.chunkTable) {
                for (auto &item: entry.second) {
                    free(item.block);
                    freeSize += item.length;
                    totalSize -= item.length;
                }
            }
            containerCache.erase(iterCache);
            lruList.erase(iterLru);
            counter--;
        }
    }

    int getRecord(uint64_t cid, const SHA1FP &sha1Fp, CachedChunk *cachedChunk, SHA1FP *baseFP) {
        {
            MutexLockGuard cacheLockGuard(cacheLock);
            auto iterCon = containerCache.find(cid);
            if (iterCon == containerCache.end()) {
                return 0;
            }
            std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> &table = iterCon->second.chunkTable;
            auto iterCache = table.find(sha1Fp);
            assert(iterCache != table.end());
            if (baseFP == nullptr) {
                for (auto &item: iterCache->second) {
                    if (item.type == 0) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        freshLastVisit(iterCon);
                        return 1;
                    }
                }
            } else {
                for (auto &item: iterCache->second) {
                    if (item.baseFP == *baseFP) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        freshLastVisit(iterCon);
                        return 1;
                    }
                }
            }
            assert(0);
        }
    }

    int tryGetRecord(uint64_t cid, const SHA1FP &sha1Fp) {
        {
            MutexLockGuard cacheLockGuard(cacheLock);
            auto iterCon = containerCache.find(cid);
            if (iterCon == containerCache.end()) {
                return 0;
            }
            std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> &table = iterCon->second.chunkTable;
            auto iterCache = table.find(sha1Fp);
            assert(iterCache != table.end());
            for (auto &item: iterCache->second) {
                if (item.type == 0) {
                    freshLastVisit(iterCon);
                    return 1;
                }
            }
            return 0;

        }
    }


private:
    void freshLastVisit(std::unordered_map<uint64_t, ContainerCache>::iterator iter) {
        MutexLockGuard lruLockGuard(lruLock);
        auto iterl = lruList.find(iter->second.lastVisit);
        lruList[index] = iterl->second;
        lruList.erase(iterl);
        iter->second.lastVisit = index;
        index++;
    }

    uint64_t index;
    uint64_t totalSize;
    uint64_t counter = 0;
    std::unordered_map<uint64_t, ContainerCache> containerCache;
    std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> localCache;
    std::map<uint64_t, uint64_t> lruList;
    MutexLock cacheLock;
    MutexLock lruLock;
    uint64_t write, read;

    uint64_t prefetchingSize = 0;
    uint64_t prefetchingTimes = 0;
    uint64_t prefetchingDuration = 0;

    uint64_t localSize = 0;
};

#endif //ODESSSTORAGE_CHUNKCACHE_H
