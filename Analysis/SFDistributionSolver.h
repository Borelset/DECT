//
// Created by Borelset on 2022/7/21.
//

#ifndef SFDISTRIBUTIONSOLVER_H
#define SFDISTRIBUTIONSOLVER_H

#include <vector>
#include <cassert>

struct SFToFSolution {
    SFToFSolution(int sfn, int fn) : sf(sfn), f(fn / sfn), pos(fn) {
      for (int i = 0; i < sf * f; i++) {
        pos[i] = 0;
      }
    }

    std::vector<int> pos;
    int sf, f;

    bool match(int o) const {

      assert(o >= 1 && o <= sf);
      for (int i = 0; i < f; i++) {
        if (pos[f * (o - 1) + i] != 1) return false;
      }
      return true;
    }

    bool operator==(SFToFSolution &r) const {
      for (int i = 0; i < sf * f; i++) {
        if (pos[i] != r.pos[i]) return false;
      }
      return true;
    }
};

class SFDistributionSolver {
public:
    SFDistributionSolver(int f, int sf, int64_t *list, int mod) : mu(0.7), sigma(1.0), totalSF(sf), mode(mod) {
      distribution = (double *) malloc(sizeof(double) * QUANTIZATION_GRID_NUMBER);
      SFtoSim = (double *) malloc(sizeof(double) * (totalSF));
      generateWeight(f, sf);

      double *percentage = (double *) malloc(sizeof(double) * (totalSF + 1));
      int64_t sum = 0;
      for (int i = 0; i < totalSF; i++) {
        sum += list[i];
      }
      for (int i = 0; i < totalSF; i++) {
        percentage[i] = (double) list[i] / sum;
      }

      double *fitP_SF = (double *) malloc(sizeof(double) * (totalSF + 1));
      double *loss = (double *) malloc(sizeof(double) * (totalSF + 1));

      while (1) {
        for (int i = 0; i < totalSF; i++) {
          fitP_SF[i] = DistributionToSFMatch(i);
        }
        for (int i = 0; i < totalSF; i++) {
          loss[i] = percentage[i] - fitP_SF[i];
        }

        double totalLoss = 0.0;
        for (int i = 0; i < totalSF; i++) {
          totalLoss += loss[i] * loss[i];
        }

        if (round < 1000) {
          if (round % 100 == 0) {
            printf("Round %d...\n", round + 1);
            printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, totalLoss);
          }
        } else if (round < 10000) {
          if (round % 1000 == 0) {
            printf("Round %d...\n", round + 1);
            printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, totalLoss);
          }
        } else if (round < 100000) {
          if (round % 10000 == 0) {
            printf("Round %d...\n", round + 1);
            printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, totalLoss);
          }
        } else if (round < 1000000) {
          if (round % 100000 == 0) {
            printf("Round %d...\n", round + 1);
            printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, totalLoss);
          }
        } else {
          printf("Force Stop\n");
          printf("Round %d...\n", round + 1);
          printf("mu=%f, sigma=%f, loss=%e\n", mu, sigma, totalLoss);
          break;
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

        // calculate grads
        for (int i = 0; i < totalSF; i++) {
          gradMu += DistributionToSFMatchDM(i) * 2 * (percentage[i] - fitP_SF[i]);
          gradSigma += DistributionToSFMatchDS(i) * 2 * (percentage[i] - fitP_SF[i]);
        }

        // adam optimizer
        mTmu = Beta1 * mTmu + (1 - Beta1) * gradMu;
        mTsigma = Beta1 * mTsigma + (1 - Beta1) * gradSigma;
        vTmu = Beta2 * vTmu + (1 - Beta2) * pow(gradMu, 2);
        vTsigma = Beta2 * vTsigma + (1 - Beta2) * pow(gradSigma, 2);

        double mTmu_temp = mTmu / (1 - pow(Beta1, round));
        double mTsigma_temp = mTsigma / (1 - pow(Beta1, round));
        double vTmu_temp = vTmu / (1 - pow(Beta2, round));
        double vTsigma_temp = vTsigma / (1 - pow(Beta2, round));

        double dmu =
                mTmu_temp * (Eta / (sqrt(vTmu_temp) + Epsilon)) * LEARNINGRATE * exp(-DECAYRATE * round / DECAYSTEP);
        double dsigma = mTsigma_temp * (Eta / (sqrt(vTsigma_temp) + Epsilon)) * LEARNINGRATE *
                        exp(-DECAYRATE * round / DECAYSTEP);
        if (sigma + dsigma < 0) {
          finalLoss = totalLoss;
          break;
        }
        mu += dmu;
        sigma += dsigma;
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

      printf("Round=%lu, mu=%f, sigma=%f, loss=%e\n", round, mu, sigma, finalLoss);

      for (int i = 0; i < totalSF; i++) {
        double weightProbSum = 0.0;
        double conv = 0.0;
        for (int j = 0; j < QUANTIZATION_GRID_NUMBER; j++) {
          weightProbSum += weightArray_stosf[j * totalSF + i] * distribution[j];
          double prob = (double) (j + 1) / QUANTIZATION_GRID_NUMBER;
          conv += weightArray_stosf[j * totalSF + i] * distribution[j] * prob;
        }
        SFtoSim[i] = conv / (weightProbSum + Epsilon);
        printf("SF %d = %f\n", i, SFtoSim[i]);
      }

      free(percentage);
      free(fitP_SF);
      free(loss);
    }

    ~SFDistributionSolver() {
      free(distribution);
      free(weightArray_stosf);
      free(SFtoSim);
    }

    void generateWeight(int f, int sf) {
      weightArray_stosf = (double *) malloc(sizeof(double) * sf * QUANTIZATION_GRID_NUMBER);
      for (int j = 0; j < QUANTIZATION_GRID_NUMBER; j++) {
        double p = (double) (j + 1) / QUANTIZATION_GRID_NUMBER;
        for (int i = 0; i < sf; i++) {
          double pickRate = pow(p, (f / sf));
          if (mode == 0) {
            weightArray_stosf[j * sf + i] = pickRate * pow(1 - pickRate, i);
          } else {
            weightArray_stosf[j * sf + i] =
                    pow(pickRate, i + 1) * pow((1 - pickRate), sf - (i + 1)) * combinationNumber(sf, i + 1);
          }
        }
      }
    }

    double DistributionToSFMatch(int sf) {
      double head = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        head += weightArray_stosf[i * totalSF + sf] * FI(prob, mu, sigma);
      }
      double base = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        base += FI(prob, mu, sigma);
      }
      return head / (base + Epsilon);
    }

    double DistributionToSFMatchDM(int sf) {
      double head = 0.0, headDM = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        head += weightArray_stosf[i * totalSF + sf] * FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        headDM += weightArray_stosf[i * totalSF + sf] * FIdm(prob, mu, sigma);
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

    double DistributionToSFMatchDS(int sf) {
      double head = 0.0, headDS = 0.0;
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        head += weightArray_stosf[i * totalSF + sf] * FI(prob, mu, sigma);
      }
      for (int i = 0; i < QUANTIZATION_GRID_NUMBER; i++) {
        double prob = (double) (i + 1) / QUANTIZATION_GRID_NUMBER;
        headDS += weightArray_stosf[i * totalSF + sf] * FIds(prob, mu, sigma);
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
      return SFtoSim[f];
    }

private:
    double *distribution;
    double *SFtoSim;
    double *weightArray_stosf;
    int mode = 0;
    double mu, sigma;
    double finalLoss;
    int totalSF;
    uint64_t round = 0;
    std::list<double> lastLoss;
    double mTmu = 0.0, mTsigma = 0.0, vTmu = 0.0, vTsigma = 0.0;

    void combination(SFToFSolution pre, int max, int total, int select, std::vector<SFToFSolution> *result) {
      if (select == 0) {
        result->push_back(pre);
        return;
      }
      if (total == 1) { // select = 1
        pre.pos[max - total] = 1;
        result->push_back(pre);
        return;
      }
      if (total > select) {
        combination(pre, max, total - 1, select, result);
      }
      if (total > 1) {
        pre.pos[max - total] = 1;
        combination(pre, max, total - 1, select - 1, result);
      }
      return;
    }
};

#endif //SFDISTRIBUTIONSOLVER_H
