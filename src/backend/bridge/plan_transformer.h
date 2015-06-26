/*
 * transformer.h
 *
 *  Created on: Jun 18, 2015
 *      Author: parallels
 */

#pragma once

#include "postgres/include/executor/execdesc.h"
#include <tr1/memory>
#include "backend/planner/abstract_plan_node.h"

namespace nstore {
namespace bridge {
class PlanTransformer {
 public:
  PlanTransformer() {
  }
  ~PlanTransformer() {
  }

  typedef std::tr1::shared_ptr<planner::AbstractPlanNode> AbstractPlanNodePtr;

  static PlanTransformer& GetInstance();

  void printPostgresPlanStateTree(const PlanState *planstate) const;

  AbstractPlanNodePtr transform(const PlanState *planstate);

 private:
  static AbstractPlanNodePtr transformModifyTable(const ModifyTableState *plan);
  static AbstractPlanNodePtr transformInsert(const ModifyTableState *plan);

};
}
}
