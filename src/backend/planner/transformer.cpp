/*
 * transformer.cpp
 *
 *  Copyright(c) 2015, CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */
#include <iostream>
#include "planner/transformer.h"
#include "postgres/include/nodes/pprint.h"

extern "C" void printPlanStateTree(const PlanState * planstate);


namespace nstore {
namespace planner {

PlanTransformer&
PlanTransformer::GetInstance() {
  static PlanTransformer planTransformer;
  return planTransformer;
}

void PlanTransformer::printPostgresPlanStateTree(const PlanState * planstate) {
  printPlanStateTree(planstate);
}

}
}

