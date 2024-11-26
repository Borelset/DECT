//
// Created by Borelset on 2022/6/29.
//

#include <cstdint>
#include "../Analysis/FDistributionSolver.h"
#include "../Analysis/SFDistributionSolver.h"

int main() {
  int64_t list[3] = {1352408, 204619, 308648};
  //81,673,5213,6361,12879,20838,27985,32093,31962,27109,19876,11618,6012
  //0,0,0,2,6,48,290,1333,4812,14428,35464,65098,78519
  //142, 785, 2764, 6743, 13087, 20473, 27617, 31319, 31718, 26853, 20027, 11906, 6566


  SFDistributionSolver distributionGenerator(12, 3, list, 1);
//  distributionGenerator.print(list);
//
//  for(int i=0; i<=12; i++){
//    printf("%d : %f\n", i, fNtoSim.getExpF(i));
//  }
}