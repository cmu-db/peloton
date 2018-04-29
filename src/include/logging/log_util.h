/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /src/backend/logging/logging_util.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/internal_types.h"
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


    static bool OpenFile(const char *name, std::ios_base::openmode mode,
                               std::fstream &fs);

    static void CloseFile(std::fstream &fs);

    static int32_t ReadNBytesFromFile(std::fstream &fs, char *bytes_read,
                                      size_t n);
};

}  // namespace logging
}  // namespace peloton
