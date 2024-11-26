//
// Created by Borelset on 2022/6/29.
//

#ifndef FDISTRIBUTIONSOLVER_H
#define FDISTRIBUTIONSOLVER_H

#include <malloc.h>
#include <list>
#include <algorithm>
#include <functional>
#include "math.h"

#define QUANTIZATION_GRID_NUMBER 100
#define PI (3.1415926)
#define STOPTHRESHOLD (0.00001)
#define LEARNINGRATE (0.02)
#define Beta1 (0.8)
#define Beta2 (0.95)
#define Eta (0.05)
#define Epsilon (1e-8)
#define DECAYSTEP 100
#define DECAYRATE (0.05)
#define STOPSEGMENT (20)

double FI(double x, double mu, double sigma) {
  double result = 1.0;
  result *= 1 / (sqrt(2 * PI) * sigma);
  result *= exp(-pow(x - mu, 2) / (2 * pow(sigma, 2)));
  return result;
}

double FIdm(double x, double mu, double sigma) {
  double result = 1.0;
  result *= (x - mu) / sqrt(2 * PI) * pow(sigma, 3);
  result *= exp(-pow(x - mu, 2) / (2 * pow(sigma, 2)));
  return result;
}

double FIds(double x, double mu, double sigma) {
  double result = 1.0;
  result *= (-1) / (sqrt(2 * PI) * pow(sigma, 2)) + pow(x - mu, 2) / (sqrt(2 * PI) * pow(sigma, 4));
  result *= exp(-pow(x - mu, 2) / (2 * pow(sigma, 2)));
  return result;
}

uint64_t combinationNumber(int total, int select) {
  if (total < select) return 0;
  if (select == 0) return 1;
  if (total - select > select) select = total - select;
  uint64_t result = 1;
  for (int i = select + 1; i <= total; i++) {
    result *= i;
  }
  for (int i = 1; i <= (total - select); i++) {
    result /= i;
  }
  return result;
}

class FDistributionSolver {
public:
    FDistributionSolver(uint64_t f, int64_t *list) : mu(0.7), sigma(1.0), totalF(f) {
      distribution = (double *) malloc(sizeof(double) * QUANTIZATION_GRID_NUMBER);
      FtoSim = (double *) malloc(sizeof(double) * (totalF + 1));
      generateWeight(f);

      double *percentage = (double *) malloc(sizeof(double) * (totalF + 1));
      int64_t sum = 0;
      for (int i = 0; i <= totalF; i++) {
        sum += list[i];
      }
      for (int i = 0; i <= totalF; i++) {
        percentage[i] = (double) list[i] / sum;
      }

      double *fitP = (double *) malloc(sizeof(double) * (totalF + 1));
      double *loss = (double *) malloc(sizeof(double) * (totalF + 1));

      while (1) {
        for (int i = 0; i <= totalF; i++) {
          fitP[i] = DistributionToFeatureMatchPercentage(i);
        }
        for (int i = 0; i <= totalF; i++) {
          loss[i] = percentage[i] - fitP[i];
        }

        double totalLoss = 0.0;
        for (int i = 0; i <= totalF; i++) {
          totalLoss += loss[i] * loss[i];
        }

//        printf("Round %d...\n", round+1);
//        printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, totalLoss);
        round++;

        lastLoss.push_back(totalLoss);
        if (lastLoss.size() > STOPSEGMENT) lastLoss.pop_front();
        double max = *lastLoss.begin(), min = *lastLoss.begin();
        for (auto item: lastLoss) {
          if (item > max) max = item;
          if (item < min) min = item;
        }

        if (lastLoss.size() >= STOPSEGMENT && max / min - 1 < STOPTHRESHOLD) {
          finalLoss = totalLoss;
          break;
        }

        double gradMu = 0.0, gradSigma = 0.0;

        for (int i = 0; i <= totalF; i++) {
          gradMu += DistributionToFeatureMatchPercentageDM(i) * 2 * (percentage[i] - fitP[i]);
          gradSigma += DistributionToFeatureMatchPercentageDS(i) * 2 * (percentage[i] - fitP[i]);
        }

        mTmu = Beta1 * mTmu + (1 - Beta1) * gradMu;
        mTsigma = Beta1 * mTsigma + (1 - Beta1) * gradSigma;
        vTmu = Beta2 * vTmu + (1 - Beta2) * pow(gradMu, 2);
        vTsigma = Beta2 * vTsigma + (1 - Beta2) * pow(gradSigma, 2);

        double mTmu_temp = mTmu / (1 - pow(Beta1, round));
        double mTsigma_temp = mTsigma / (1 - pow(Beta1, round));
        double vTmu_temp = vTmu / (1 - pow(Beta2, round));
        double vTsigma_temp = vTsigma / (1 - pow(Beta2, round));

        mu += mTmu_temp * (Eta / (sqrt(vTmu_temp) + Epsilon)) * LEARNINGRATE * exp(-DECAYRATE * round / DECAYSTEP);
        sigma += mTsigma_temp * (Eta / (sqrt(vTsigma_temp) + Epsilon)) * LEARNINGRATE *
                 exp(-DECAYRATE * round / DECAYSTEP);
      }

      double disSum = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        disSum += FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        distribution[i] = FI(prob, mu, sigma) / disSum;
      }


//      for(int i=0; i<QUANTIZATION_GRID_NUMBER; i++){
//        double prob = (double)(i+1)/QUANTIZATION_GRID_NUMBER;
//        printf("sim: %f  dis: %f\n", prob, distribution[i]);
//      }

      printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, finalLoss);

      for (int i = 0; i <= totalF; i++) {
        double weightProbSum = 0.0;
        double conv = 0.0;
        for (int j = 0; j < QUANTIZATION_GRID_NUMBER; j++) {
          weightProbSum += weightArray[j * (totalF + 1) + i] * distribution[j];
          double prob = (double) (j + 1) / QUANTIZATION_GRID_NUMBER;
          conv += weightArray[j * (totalF + 1) + i] * distribution[j] * prob;
        }
        FtoSim[i] = conv / weightProbSum;
//        printf("Feature %d = %f\n", i, SFtoSim[i]);
      }

      free(percentage);
      free(fitP);
      free(loss);
    }

    ~FDistributionSolver() {
      free(distribution);
      free(weightArray);
      free(FtoSim);
    }

    void generateWeight(uint64_t f) {
      weightArray = (double *) malloc(sizeof(double) * (f + 1) * QUANTIZATION_GRID_NUMBER);
      for (int i = 0; i <= totalF; i++) {
        for (int j = 0; j < QUANTIZATION_GRID_NUMBER; j++) {
          double p = (double) (j + 1) / QUANTIZATION_GRID_NUMBER;
          weightArray[j * (totalF + 1) + i] = pow(p, i) * pow(1.0 - p, totalF - i) * combinationNumber(totalF, i);
        }
      }
    }

    double DistributionToFeatureMatchPercentage(int f) {
      double head = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        head += weightArray[i * (totalF + 1) + f] * FI(prob, mu, sigma);
      }
      double base = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        base += FI(prob, mu, sigma);
      }
      return head / base;
    }

    double DistributionToFeatureMatchPercentageDM(int f) {
      double head = 0.0, headDM = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        head += weightArray[i * (totalF + 1) + f] * FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        headDM += weightArray[i * (totalF + 1) + f] * FIdm(prob, mu, sigma);
      }
      double base = 0.0, baseDM = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        base += FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        baseDM += FIdm(prob, mu, sigma);
      }
      return headDM * base - head * baseDM;
    }

    double DistributionToFeatureMatchPercentageDS(int f) {
      double head = 0.0, headDS = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        head += weightArray[i * (totalF + 1) + f] * FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        headDS += weightArray[i * (totalF + 1) + f] * FIds(prob, mu, sigma);
      }
      double base = 0.0, baseDS = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        base += FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        baseDS += FIds(prob, mu, sigma);
      }
      return headDS * base - head * baseDS;
    }

    double getDistribution(int sim) {
      return distribution[sim];
    }

    double getFtoSim(int f) {
      return FtoSim[f];
    }

    void print(int64_t *list) {
      double *percentage = (double *) malloc(sizeof(double) * (totalF + 1));
      int64_t sum = 0;
      for (int i = 0; i <= totalF; i++) {
        sum += list[i];
      }
      for (int i = 0; i <= totalF; i++) {
        percentage[i] = (double) list[i] / sum;
      }

      double *fitP = (double *) malloc(sizeof(double) * (totalF + 1));
      double *loss = (double *) malloc(sizeof(double) * (totalF + 1));

      for (int i = 0; i < 1500; i++) {
        printf("mu = %f\n", (double) i / 1500);
        for (int j = 0; j <= 1500; j++) {
          mu = (double) i / 1000;
          sigma = (double) j / 1000;

          for (int i = 0; i <= totalF; i++) {
            fitP[i] = DistributionToFeatureMatchPercentage(i);
          }
          for (int i = 0; i <= totalF; i++) {
            loss[i] = percentage[i] - fitP[i];
          }

          double totalLoss = 0.0;
          for (int i = 0; i <= totalF; i++) {
            totalLoss += loss[i] * loss[i];
          }

          printf("%f ", totalLoss);
        }
        printf("\n");
      }

      free(percentage);
      free(fitP);
      free(loss);
    }

private:
    double *distribution;
    double *FtoSim;
    double *weightArray;
    double mu, sigma;
    double finalLoss;
    int totalF;
    uint64_t round = 0;
    std::list<double> lastLoss;
    double mTmu = 0.0, mTsigma = 0.0, vTmu = 0.0, vTsigma = 0.0;
};

#endif //FDISTRIBUTIONSOLVER_H
