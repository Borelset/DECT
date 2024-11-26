/*
 * Author   : Xiangyu Zou
 * Date     : 07/01/2021
 * Time     : 20:03
 * Project  : OdessStorage
 This source code is licensed under the GPLv2
 */

#ifndef ODESSSTORAGE_HARCONTAINERMANAGER_H
#define ODESSSTORAGE_HARCONTAINERMANAGER_H

DEFINE_double(HARThreshold, 0.2, "");


struct HarEntry {
    uint64_t cid;
    uint64_t size;
};

bool HarOrder(const HarEntry &first, const HarEntry &second) {
    return first.size > second.size;
}

float HARSparseLimit = 0.005;

class HarContainerManager {
public:
    HarContainerManager() {
        HARThreshold = FLAGS_HARThreshold;
    }

    int init() {
        InvolvedContainerList.clear();
      return 0;
    }

    int isSparse(uint64_t cid){
        auto iter = SparseContainerList.find(cid);
        if(iter != SparseContainerList.end()){
            return 1;
        }else{
            return 0;
        }
    }

    int addRecord(uint64_t cid, uint64_t size){
        auto iter2 = InvolvedContainerList.find(cid);
        if(iter2 != InvolvedContainerList.end()){
            iter2->second += size;
        }else{
            InvolvedContainerList[cid] = size;
        }
      return 0;
    }


    int update(){
        uint64_t totalSize = 0;
        uint64_t sparseSize = 0;
        std::list<HarEntry> tempMap;
        uint64_t sparse = 0;
        for (const auto &entry: InvolvedContainerList) {
            totalSize += entry.second;
            if (1.0 * entry.second / ContainerSize < HARThreshold) {
                sparseSize += entry.second;
                tempMap.push_back({entry.first, entry.second});
                sparse++;
            }
        }

        tempMap.sort(HarOrder);

        while (1.0 * sparseSize / totalSize > HARSparseLimit) {
            const auto iter = tempMap.begin();
            sparseSize -= iter->size;
            sparseSize--;
            tempMap.erase(iter);
        }
        printf("Sparse Containers:%lu\n", sparse);

//        SparseContainerList.clear();

        for (auto item: tempMap) {
            SparseContainerList.insert(item.cid);
        }
      return 0;
    }

private:
    std::set<uint64_t> SparseContainerList;
    std::unordered_map<uint64_t, uint64_t> InvolvedContainerList;

    int srMonitorThreshold = 2560;
    double HARThreshold;
};

#endif //ODESSSTORAGE_HARCONTAINERMANAGER_H
