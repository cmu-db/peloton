/*
 * transformer.h
 *
 *  Created on: Jun 18, 2015
 *      Author: parallels
 */

#pragma once

#include "postgres/include/executor/execdesc.h"

namespace nstore {
namespace planner {
class PlanTransformer {
 public:
  PlanTransformer() {
  }
  ~PlanTransformer() {
  }

  static PlanTransformer& GetInstance();

  void printPostgresPlanStateTree(const PlanState *planstate);

};
}
}
