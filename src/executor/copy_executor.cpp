//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/executor/copy_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <concurrency/transaction_manager_factory.h>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "executor/copy_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile_factory.h"
#include "expression/container_tuple.h"
#include "planner/copy_plan.h"
#include "storage/table_factory.h"
#include "logging/logging_util.h"
#include "common/exception.h"
#include <sys/stat.h>
#include <sys/mman.h>

namespace peloton {
namespace executor {

/**
 * @brief Constructor for Copy executor.
 * @param node Copy node corresponding to this executor.
 */
CopyExecutor::CopyExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

CopyExecutor::~CopyExecutor() {}

/**
 * @brief Basic initialization.
 * @return true on success, false otherwise.
 */
bool CopyExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);

  LOG_TRACE("Copy executor :: 1 child ");

  // Grab info from plan node and check it
  const planner::CopyPlan &node = GetPlanNode<planner::CopyPlan>();

  bool success = logging::LoggingUtil::InitFileHandle(node.file_path.c_str(),
                                                      file_handle_, "w");
  if (success == false) {
    throw ExecutorException("Failed to create file " + node.file_path);
    return false;
  }
  LOG_DEBUG("Created target copy output file: %s", node.file_path.c_str());
  return true;
}

/**
 * @return true on success, false otherwise.
 */
bool CopyExecutor::DExecute() {
  // skip if we're done
  if (done) {
    return false;
  }

  while (children_[0]->Execute() == true) {
    // Get input tiles and Copy them
    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());

    // Physical schema of the tile
    std::unique_ptr<catalog::Schema> output_schema(
        logical_tile->GetPhysicalSchema());
    std::vector<std::vector<std::string>> answer_tuples;
    auto col_count = output_schema->GetColumnCount();

    // Construct result format for varchar
    std::vector<int> result_format(col_count, 0);
    answer_tuples =
        std::move(logical_tile->GetAllValuesAsStrings(result_format));

    LOG_DEBUG("Looping over tile..");
    char buff[4096];
    size_t ptr = 0;

    // Construct the returned results
    for (auto &tuple : answer_tuples) {
      for (unsigned int col_index = 0; col_index < col_count; col_index++) {
        auto &val = tuple[col_index];
        int len = val.length();
        if (ptr + len * 2 + 2 >= 4096) {
          fwrite(buff, sizeof(char), ptr, file_handle_.file);
          ptr = 0;
        }
        for (int i = 0; i < len; i++) {
          if (val[i] == delimiter || val[i] == '\n') {
            buff[ptr++] = '\\';
          }
          buff[ptr++] = val[i];
        }
        if (col_index != col_count - 1) {
          buff[ptr++] = delimiter;
        }
      }
      buff[ptr++] = '\n';
    }
    fwrite(buff, sizeof(char), ptr, file_handle_.file);
    // Sync and close
    fclose(file_handle_.file);
    LOG_DEBUG("Finished writing to csv file");
  }
  done = true;
  return true;
}

}  // namespace executor
}  // namespace peloton
