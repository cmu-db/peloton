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
#include "catalog/catalog.h"
#include "executor/copy_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile_factory.h"
#include "planner/copy_plan.h"
#include "storage/table_factory.h"
#include "common/exception.h"
#include "common/macros.h"
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

  // Grab info from plan node and check it
  const planner::CopyPlan &node = GetPlanNode<planner::CopyPlan>();

  bool success = InitFileHandle(node.file_path.c_str(), "w");

  if (success == false) {
    throw ExecutorException("Failed to create file " + node.file_path +
                            ". Try absolute path and make sure you have the "
                            "permission to access this file.");
    return false;
  }
  LOG_DEBUG("Created target copy output file: %s", node.file_path.c_str());

  // Whether we're copying the parameters which require deserialization
  if (node.deserialize_parameters) {
    InitParamColIds();
  }
  return true;
}


bool CopyExecutor::InitFileHandle(const char *name, const char *mode) {
  auto file = fopen(name, mode);
  if (file == NULL) {
    LOG_ERROR("File is NULL");
    return false;
  } else {
    file_handle_.file = file;
  }

  // get the descriptor
  auto fd = fileno(file);
  if (fd == INVALID_FILE_DESCRIPTOR) {
    LOG_ERROR("file fd is -1");
    return false;
  } else {
    file_handle_.fd = fd;
  }
  file_handle_.size = 0;
  return true;
}

/**
 * Write buffer data to the file. Although fwrite() is not a system call,
 * calling fwrite frequently with small byte is not efficient because fwrite
 * does many sanity checks. We use local buffer to amortize that.
 */
void CopyExecutor::FlushBuffer() {
  PL_ASSERT(buff_ptr < COPY_BUFFER_SIZE);
  PL_ASSERT(buff_size + buff_ptr <= COPY_BUFFER_SIZE);
  while (buff_size > 0) {
    size_t bytes_written =
        fwrite(buff + buff_ptr, sizeof(char), buff_size, file_handle_.file);
    // Book keeping
    buff_ptr += bytes_written;
    buff_size -= bytes_written;
    total_bytes_written += bytes_written;
    LOG_TRACE("fwrite %d bytes", (int)bytes_written);
  }
  buff_ptr = 0;
}


void CopyExecutor::FFlushFsync() {
  // First, flush
  PL_ASSERT(file_handle_.fd != -1);
  if (file_handle_.fd == -1) return;
  int ret = fflush(file_handle_.file);
  if (ret != 0) {
    LOG_ERROR("Error occurred in fflush(%s)", strerror(errno));
  }
  // Finally, sync
  ret = fsync(file_handle_.fd);
  if (ret != 0) {
    LOG_ERROR("Error occurred in fsync(%s)", strerror(errno));
  }
}

void CopyExecutor::InitParamColIds() {
  // If we're going to deserialize prepared statement, get the column ids for
  // the varbinary columns first
  // auto catalog = catalog::Catalog::GetInstance();
  // try {
  //   auto query_metric_table =
  //       catalog->GetTableWithName(CATALOG_DATABASE_NAME, QUERY_METRIC_NAME);
  //   auto schema = query_metric_table->GetSchema();
  //   auto &cols = schema->GetColumns();
  //   for (unsigned int i = 0; i < cols.size(); i++) {
  //     auto col_name = cols[i].column_name.c_str();
  //     if (std::strcmp(col_name, QUERY_PARAM_TYPE_COL_NAME) == 0) {
  //       param_type_col_id = i;
  //     } else if (std::strcmp(col_name, QUERY_PARAM_FORMAT_COL_NAME) == 0) {
  //       param_format_col_id = i;
  //     } else if (std::strcmp(col_name, QUERY_PARAM_VAL_COL_NAME) == 0) {
  //       param_val_col_id = i;
  //     } else if (std::strcmp(col_name, QUERY_NUM_PARAM_COL_NAME) == 0) {
  //       num_param_col_id = i;
  //     }
  //   }
  // }
  // catch (Exception &e) {
  //   e.PrintStackTrace();
  // }
}

void CopyExecutor::Copy(const char *data, int len, bool end_of_line) {
  // Worst case we need to escape all character and two delimiters
  while (COPY_BUFFER_SIZE - buff_size - buff_ptr < (size_t)len * 3) {
    FlushBuffer();
  }

  // Now copy the string to local buffer and escape delimiters
  // TODO A better way is to search for delimiter once and perform copy
  for (int i = 0; i < len; i++) {
    char ch = data[i];
    // Check delimiter
    if (ch == delimiter) {
      buff[buff_size++] = '\\';
      buff[buff_size++] = '\\';
    } else if (ch == new_line) {
      buff[buff_size++] = '\\';
    }
    buff[buff_size++] = ch;
  }

  // Append col delimiter and new line delimiter
  if (end_of_line == false) {
    buff[buff_size++] = delimiter;
  } else {
    buff[buff_size++] = new_line;
  }
  PL_ASSERT(buff_size <= COPY_BUFFER_SIZE);
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
    // Get input a tile
    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    LOG_DEBUG("Looping over the output tile..");

    // Get physical schema of the tile
    std::unique_ptr<catalog::Schema> output_schema(
        logical_tile->GetPhysicalSchema());

    // vectors for prepared statement parameters
    int num_params = 0;
    std::vector<std::pair<int, std::string>> bind_parameters;
    std::vector<type::Value> param_values;
    std::vector<int16_t> formats;
    std::vector<int32_t> types;

    // Construct result format as varchar
    auto col_count = output_schema->GetColumnCount();
    std::vector<std::vector<std::string>> answer_tuples;
    std::vector<int> result_format(col_count, 0);
    answer_tuples =
        logical_tile->GetAllValuesAsStrings(result_format, true);

    // Loop over the returned results
    for (auto &tuple : answer_tuples) {
      // Loop over the columns
      for (unsigned int col_index = 0; col_index < col_count; col_index++) {
        auto val = tuple[col_index];
        auto origin_col_id =
            logical_tile->GetColumnInfo(col_index).origin_column_id;
        int len = val.length();

        if (origin_col_id == num_param_col_id) {
          // num_param column
          num_params = std::stoi(val);
          Copy(val.c_str(), val.length(), false);

        } else if (origin_col_id == param_type_col_id) {
          // param_types column
          PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                    type::TypeId::VARBINARY);

          wire::InputPacket packet(len, val);

          // Read param types
          types.resize(num_params);
          wire::PacketManager::ReadParamType(&packet, num_params, types);

          // Write all the types to output file
          for (int i = 0; i < num_params; i++) {
            std::string type_str = std::to_string(types[i]);
            Copy(type_str.c_str(), type_str.length(), false);
          }
        } else if (origin_col_id == param_format_col_id) {
          // param_formats column
          PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                    type::TypeId::VARBINARY);

          wire::InputPacket packet(len, val);

          // Read param formats
          formats.resize(num_params);
          wire::PacketManager::ReadParamFormat(&packet, num_params, formats);

        } else if (origin_col_id == param_val_col_id) {
          // param_values column
          PL_ASSERT(output_schema->GetColumn(col_index).GetType() ==
                    type::TypeId::VARBINARY);

          wire::InputPacket packet(len, val);
          bind_parameters.resize(num_params);
          param_values.resize(num_params);
          wire::PacketManager::ReadParamValue(&packet, num_params, types,
                                              bind_parameters, param_values,
                                              formats);

          // Write all the values to output file
          for (int i = 0; i < num_params; i++) {
            auto param_value = param_values[i];
            LOG_TRACE("param_value.GetTypeId(): %d", param_value.GetTypeId());
            // Avoid extra copy for varlen types
            if (param_value.GetTypeId() == type::TypeId::VARBINARY) {
              const char *data = param_value.GetData();
              Copy(data, param_value.GetLength(), false);
            } else if (param_value.GetTypeId() == type::TypeId::VARCHAR) {
              const char *data = param_value.GetData();
              // Don't write the NULL character for varchar
              Copy(data, param_value.GetLength() - 1, false);
            } else {
              // Convert integer / double types to string before copying
              auto param_str = param_value.ToString();
              Copy(param_str.c_str(), param_str.length(), false);
            }
          }
        } else {
          // For other columns, just copy the content to local buffer
          bool end_of_line = col_index == col_count - 1;
          Copy(val.c_str(), val.length(), end_of_line);
        }
      }
    }
    LOG_DEBUG("Done writing to csv file for this tile");
  }
  LOG_INFO("Done copying all logical tiles");
  FlushBuffer();
  FFlushFsync();
  // Sync and close
  fclose(file_handle_.file);

  done = true;
  return true;
}

}  // namespace executor
}  // namespace peloton
