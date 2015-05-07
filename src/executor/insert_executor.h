/**
 * @brief Header file for insert executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

#include <vector>

namespace nstore {
namespace executor {

class InsertExecutor : public AbstractExecutor {
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor& operator=(const InsertExecutor &) = delete;

 public:
  explicit InsertExecutor(planner::AbstractPlanNode *node,
                          Context *context,
                          const std::vector<storage::Tuple *>& tuples);

 protected:
  bool DInit();

  bool DExecute();

 private:

  // tuples to be inserted
  std::vector<storage::Tuple *> tuples;
};

} // namespace executor
} // namespace nstore
