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

#pragma once

#include "type/type.h"
#include "common/logger.h"
#include "storage/data_table.h"
#include "type/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

class LoggingUtil {
 public:
  // FILE SYSTEM RELATED OPERATIONS
  static bool CheckDirectoryExistence(const char *dir_name);

  static bool CreateDirectory(const char *dir_name, int mode);

  static bool RemoveDirectory(const char *dir_name, bool only_remove_file);

  static bool GetDirectoryList(const char *dir_name,
                               std::vector<std::string> &dir_name_list);

  static bool GetFileList(const char *dir_name,
                          std::vector<std::string> &file_name_list);

  static void FFlushFsync(FileHandle &file_handle);

  static bool OpenFile(const char *name, const char *mode,
                       FileHandle &file_handle);

  static bool MoveFile(const char *oldname, const char *newname);

  static bool CloseFile(FileHandle &file_handle);

  static bool IsFileTruncated(FileHandle &file_handle, size_t size_to_read);

  static size_t GetFileSize(FileHandle &file_handle);

  static bool ReadNBytesFromFile(FileHandle &file_handle, void *bytes_read,
                                 size_t n);
};

}  // namespace logging
}  // namespace peloton
