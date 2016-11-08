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
// TODO remove me in the future
#include "wire/wire.h"

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

  deserialize_parameters = node.deserialize_parameters;

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

  // TODO replay logs to serialize data

  while (children_[0]->Execute() == true) {
    // Get input tiles and Copy them
    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());

    LOG_DEBUG("Looping over tile..");
    char buff[4096];
    size_t ptr = 0;
    // Physical schema of the tile
    std::unique_ptr<catalog::Schema> output_schema(
        logical_tile->GetPhysicalSchema());

    auto col_count = output_schema->GetColumnCount();
    std::vector<std::vector<std::string>> answer_tuples;

    // Construct result format for varchar
    std::vector<int> result_format(col_count, 0);
    answer_tuples =
        std::move(logical_tile->GetAllValuesAsStrings(result_format));

    // TODO also implement normal cases
    PL_ASSERT(deserialize_parameters == true);

    std::vector<int16_t> formats;
    int num_params = 0;

    // TODO resize it later
    std::vector<std::pair<int, std::string>> bind_parameters;
    std::vector<common::Value> param_values;

    // Construct the returned results
    for (auto &tuple : answer_tuples) {
      for (unsigned int col_index = 0; col_index < col_count; col_index++) {

        // TODO now it copies a string, need to avoid this in the future
        auto &val = tuple[col_index];
        int len = val.length();

        switch (col_index) {
          case 2: {
            // num_param column.
            num_params = std::stoi(val);
            break;
          }
          case 3: {

            PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                      common::Type::VARBINARY);

            // param_formats column
            wire::Packet packet;

            // The actual length of data should include NULL?
            packet.len = len;
            packet.buf.resize(packet.len);

            // Copy the data from string to packet buf...
            for (size_t i = 0; i < len; i++) {
              packet.buf[i] = val[i];
            }

            // Read param formats
            wire::PacketManager::ReadParamFormat(&packet, num_params, formats);
            break;
          }
          case 4: {

            PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                      common::Type::VARBINARY);

            // param_values column
            wire::Packet packet;

            // The actual length of data should include NULL?
            packet.len = len;
            packet.buf.resize(packet.len);

            // Copy the data from string to packet buf...
            for (size_t i = 0; i < len; i++) {
              packet.buf[i] = val[i];
            }

            // TODO store param types as well..?
            wire::PacketManager::ReadParamValue(&packet, num_params,
                                                param_types, bind_parameters,
                                                param_values, formats);

            break;
          }
          default: {
            // other columns
            // do it the normal way
            break;
          }
        }
        // TODO move the bottom up..

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
