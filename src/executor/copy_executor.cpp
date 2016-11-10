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

  deserialize_parameters = node.deserialize_parameters;

  if (success == false) {
    throw ExecutorException("Failed to create file " + node.file_path +
                            ". Try absolute path and make sure you have the "
                            "permission to access this file.");
    return false;
  }
  LOG_DEBUG("Created target copy output file: %s", node.file_path.c_str());
  return true;
}

void CopyExecutor::Copy(const char *data, int len, bool end_of_line) {

  // TODO use memcpy instead of looping?
  // Worst case we need to escape all character and two delimiters
  while (COPY_BUFFER_SIZE - buff_size - buff_ptr < (size_t)len * 2) {
    PL_ASSERT(buff_ptr < COPY_BUFFER_SIZE);
    PL_ASSERT(buff_size + buff_ptr <= COPY_BUFFER_SIZE);
    while (buff_size > 0) {
      size_t bytes_written =
          fwrite(buff + buff_ptr, sizeof(char), buff_size, file_handle_.file);
      buff_ptr += bytes_written;
      buff_size -= bytes_written;
      total_bytes_written += bytes_written;
    }
    buff_ptr = 0;
  }

  // Now copy the string to local buffer and escape delimiters
  for (int i = 0; i < len; i++) {
    // Check delimiter
    if (data[i] == delimiter || data[i] == '\n') {
      buff[buff_size++] = '\\';
    }
    buff[buff_size++] = data[i];
  }
  // Append col delimiter / new line delimiter
  if (end_of_line == false) {
    buff[buff_size++] = delimiter;
  } else {
    buff[buff_size++] = '\n';
  }
  PL_ASSERT(buff_size <= COPY_BUFFER_SIZE);
}

void CopyExecutor::CreateParamPacket(wire::Packet &packet, int len,
                                     std::string &val) {
  // The actual length of data should include NULL?
  packet.len = len;
  packet.buf.resize(packet.len);

  // Copy the data from string to packet buf
  // TODO use memcopy
  for (int i = 0; i < len; i++) {
    packet.buf[i] = val[i];
  }
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
    LOG_DEBUG("Looping over the output tile..");
    // Get input a tile
    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());

    // Get physical schema of the tile
    std::unique_ptr<catalog::Schema> output_schema(
        logical_tile->GetPhysicalSchema());

    auto col_count = output_schema->GetColumnCount();
    std::vector<std::vector<std::string>> answer_tuples;

    //#define QUERY_PARAM_TYPE_COL_NAME "param_types"
    //#define QUERY_PARAM_FORMAT_COL_NAME "param_formats"
    //#define QUERY_PARAM_VAL_COL_NAME "param_values"

    // Construct result format for varchar
    std::vector<int> result_format(col_count, 0);
    answer_tuples =
        std::move(logical_tile->GetAllValuesAsStrings(result_format));

    int num_params = 0;

    // vectors for prepared statement parameters
    std::vector<std::pair<int, std::string>> bind_parameters;
    std::vector<common::Value> param_values;
    std::vector<int16_t> formats;
    std::vector<int32_t> types;

    // Loop over the returned results
    for (auto &tuple : answer_tuples) {
      // Loop over the columns
      for (unsigned int col_index = 0; col_index < col_count; col_index++) {

        auto val = tuple[col_index];
        std::cout << val << std::endl;
        auto origin_col_id =
            logical_tile->GetColumnInfo(col_index).origin_column_id;

        LOG_INFO("Got %s",
                 output_schema->GetColumn(col_index).column_name.c_str());

        int len = val.length();
        if (deserialize_parameters && origin_col_id == 2) {
          // num_param column.
          num_params = std::stoi(val);
          Copy(val.c_str(), val.length(), false);

        } else if (deserialize_parameters && origin_col_id == 3) {

          LOG_ERROR("types before deser %s, %d", val.c_str(),
                    (int)val.length());

          PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                    common::Type::VARBINARY);

          // param_types column
          wire::Packet packet;
          CreateParamPacket(packet, len, val);

          types.resize(num_params);
          // Read param types
          wire::PacketManager::ReadParamType(&packet, num_params, types);

          // Write all the types to output file
          for (int i = 0; i < num_params; i++) {
            auto type = types[i];
            std::string type_str = std::to_string(type);
            // LOG_ERROR("type: %d", type);
            Copy(type_str.c_str(), type_str.length(), false);
          }
        } else if (deserialize_parameters && origin_col_id == 4) {

          PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                    common::Type::VARBINARY);

          LOG_ERROR("format before deser %s, %d", val.c_str(),
                    (int)val.length());
          // param_formats column
          wire::Packet packet;
          CreateParamPacket(packet, len, val);

          formats.resize(num_params);
          // Read param formats
          wire::PacketManager::ReadParamFormat(&packet, num_params, formats);

          //          for (auto format : formats) {
          // LOG_ERROR("format_str: %d", format);
          //        }

        } else if (deserialize_parameters && origin_col_id == 5) {

          LOG_ERROR("val before deser %s, %d", val.c_str(), (int)val.length());

          PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                    common::Type::VARBINARY);

          // param_values column
          wire::Packet packet;
          CreateParamPacket(packet, len, val);

          bind_parameters.resize(num_params);
          param_values.resize(num_params);
          wire::PacketManager::ReadParamValue(&packet, num_params, types,
                                              bind_parameters, param_values,
                                              formats);

          // Write all the values to output file
          for (int i = 0; i < num_params; i++) {
            auto param_value = param_values[i];
            // LOG_ERROR("param_value.GetTypeId(): %d",
            // param_value.GetTypeId());
            if (param_value.GetTypeId() == common::Type::VARBINARY ||
                param_value.GetTypeId() == common::Type::VARCHAR) {
              const char *data = param_value.GetData();
              Copy(data, param_value.GetLength(), false);
            } else {
              auto param_str = param_value.ToString();
              Copy(param_str.c_str(), param_str.length(), false);
            }
          }

        } else {
          bool end_of_line = col_index == col_count - 1;
          Copy(val.c_str(), val.length(), end_of_line);
        }
      }
    }
    //    if (buff_size > 0) {
    //      PL_ASSERT(buff_size + buff_ptr <= COPY_BUFFER_SIZE);
    //      auto ret_val =
    //          fwrite(buff + buff_ptr, sizeof(char), buff_size,
    // file_handle_.file);
    //      buff_size -= ret_val;
    //
    //      if (ret_val <= 0) {
    //        LOG_ERROR("Could not write Max Delimiter to file header: %s",
    //                  strerror(errno));
    //      }
    //    }
    // auto ret_val = fsync(file_handle_.file);
    //    if (ret_val <= 0) {
    //      LOG_ERROR("Could not write Max Delimiter to file header: %s",
    //                strerror(errno));
    //    }

    LOG_DEBUG("Finished writing to csv file");
  }
  logging::LoggingUtil::FFlushFsync(file_handle_);
  // Sync and close
  fclose(file_handle_.file);

  done = true;
  return true;
}

}  // namespace executor
}  // namespace peloton
