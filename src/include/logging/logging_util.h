//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_util.h
//
// Identification: src/include/logging/logging_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/logging/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"
#include "common/logger.h"
#include "logging/records/transaction_record.h"
#include "logging/records/tuple_record.h"
#include "storage/data_table.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

class LoggingUtil {
 public:
  static void FFlushFsync(FileHandle &file_handle);

  static bool InitFileHandle(const char *name, FileHandle &file_handle,
                             const char *mode);

  static size_t GetLogFileSize(FileHandle &file_handle);

  static bool IsFileTruncated(FileHandle &file_handle, size_t size_to_read);

  static size_t GetNextFrameSize(FileHandle &file_handle);

  static LogRecordType GetNextLogRecordType(FileHandle &file_handle);

  static int ExtractNumberFromFileName(const char *name);

  static bool ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                          FileHandle &file_handle);

  static bool ReadTupleRecordHeader(TupleRecord &tuple_record,
                                    FileHandle &file_handle);

  static storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema,
                                             VarlenPool *pool,
                                             FileHandle &file_handle);

  static void SkipTupleRecordBody(FileHandle &file_handle);

  static int GetFileSizeFromFileName(const char *);

  static bool CreateDirectory(const char *dir_name, int mode);

  static bool RemoveDirectory(const char *dir_name, bool only_remove_file);

  // Wrappers
  /**
   * @brief Read get table based on tuple record
   * @param tuple record
   * @return data table
   */
  static storage::DataTable *GetTable(TupleRecord &tupleRecord);
};

}  // namespace logging
}  // namespace peloton
