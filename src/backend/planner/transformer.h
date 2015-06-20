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

  void printPostgresPlanStates(const PlanState *planstate);

 private:
  void printPostgresPlanStates(const PlanState *planstate, int indentLeven);
  void printSubPlanStateList(const List* list, int indentLevel);
  void indent(int indentLevel);
};
}
}
