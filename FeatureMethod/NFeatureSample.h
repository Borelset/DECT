//
// Created by BorelsetR on 2019/8/7.
//

#ifndef ODESSSTORAGE_NFEATURESAMPLE_H
#define ODESSSTORAGE_NFEATURESAMPLE_H

#include <cstdint>
#include <cstdlib>
#include <random>
#include "../RollHash/RollHash.h"
#include "DetectMethod.h"
#include "../Utility/xxhash.h"

DEFINE_int32(OdessShiftBits, 1, "bit shift for gear in odess, decide the window size.");

DEFINE_uint64(OdessSamplingMask, 0x0000400303410000, "Sampling mask for Odess");

#define SF_NUMBER 3
#define F_NUMBER 4

extern const uint64_t DistributionAMin;
extern const uint64_t DistributionAMax;

extern const uint64_t DistributionBMin;
extern const uint64_t DistributionBMax;

class NFeatureSample : public DetectMethod {
public:
    NFeatureSample(int k, RollHash *rollHash) {
      std::uniform_int_distribution<uint64_t>::param_type paramA(DistributionAMin, DistributionAMax);
      std::uniform_int_distribution<uint64_t>::param_type paramB(DistributionBMin, DistributionBMax);
      distributionA.param(paramA);
      distributionB.param(paramB);
      featureAmount = SF_NUMBER * F_NUMBER;
      recordsList = (uint64_t *) malloc(sizeof(uint64_t) * SF_NUMBER * F_NUMBER);
      transformListA = (int *) malloc(sizeof(int) * SF_NUMBER * F_NUMBER);
      transformListB = (int *) malloc(sizeof(int) * SF_NUMBER * F_NUMBER);
      for (int i = 0; i < SF_NUMBER * F_NUMBER; i++) {
        transformListA[i] = argRandomA();
        transformListB[i] = argRandomB();
        recordsList[i] = 0; // min uint64_t
      }
      usingHash = rollHash;
      matrix = usingHash->getMatrix();
      mask = FLAGS_OdessSamplingMask;
      printf("Odess Sampling Mask : %lx\n", mask);
    }

    ~NFeatureSample() {
      ;

      free(recordsList);
      free(transformListA);
      free(transformListB);
    }

    //0x0000400303410000
    void detect(uint8_t *inputPtr, uint64_t length) override {
        uint64_t hashValue = 0;
        for (uint64_t i = 0; i < length; i++) {
            hashValue = (hashValue << FLAGS_OdessShiftBits) + matrix[*(inputPtr + i)];
            if (!(hashValue & mask)) {
                for (int j = 0; j < featureAmount; j++) {
                    uint64_t transResult = (hashValue * transformListA[j] + transformListB[j]);
                    if (transResult > recordsList[j])
                        recordsList[j] = transResult;
                }
            }
        }
    }

    void detectTest(uint8_t *inputPtr, uint64_t length) override {
        for (uint64_t i = 0; i < length; i++) {
            char test = *(inputPtr + i);
            uint64_t hashValue = usingHash->rolling(inputPtr + i);
            for (int j = 0; j < featureAmount; j++) {
                uint64_t transResult = featureTranformation(hashValue, j);
                if (transResult > recordsList[j])
                    recordsList[j] = transResult;
            }
        }

        if (1) {
            for (int i = 0; i < featureAmount; i++) {
                printf("feature #%d : %lu\n", i, recordsList[i]);
            }
        }
    }

    int getResult(SFSet *result) override {
      for (int i = 0; i < SF_NUMBER; i++) {
        result->sf[i] = XXH64(&recordsList[i * F_NUMBER], sizeof(uint64_t) * F_NUMBER, 0x7fcaf1);
      }
      memcpy(result->feature, recordsList, sizeof(uint64_t) * featureAmount);
      return featureAmount;
    }

    virtual int setTotalLength(uint64_t length) override {
        resetHash();
        for (int i = 0; i < featureAmount; i++) {
          recordsList[i] = 0; // min uint64_t
        }
      return 0;
    }

    virtual int resetHash() override {
      usingHash->reset();
      return 0;
    }

    uint64_t *getMatrix() {
      return matrix;
    }

    int *getTransA() {
      return transformListA;
    }

    int *getTransB() {
      return transformListB;
    }

private:
    int featureAmount;
    uint64_t *recordsList;
    int *transformListA;
    int *transformListB;
    RollHash *usingHash;
    uint64_t *matrix;

    std::default_random_engine randomEngine;
    std::uniform_int_distribution<uint64_t> distributionA;
    std::uniform_int_distribution<uint64_t> distributionB;

    uint64_t mask;

    uint64_t argRandomA() {
        return distributionA(randomEngine);
    }

    uint64_t argRandomB() {
        return distributionB(randomEngine);
    }

    uint64_t featureTranformation(uint64_t hash, int index) {
        return hash * transformListA[index] + transformListB[index];
    }
};

#endif //ODESSSTORAGE_NFEATURESAMPLE_H
