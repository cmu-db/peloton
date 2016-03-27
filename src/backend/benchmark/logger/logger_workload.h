//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/logger/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/logger/logger_configuration.h"

namespace peloton {

namespace catalog {
class Column;
class Schema;
}

namespace storage {
class Tuple;
class DataTable;
}

namespace benchmark {
namespace logger {

extern configuration state;

//===--------------------------------------------------------------------===//
// PREPARE LOG FILE
//===--------------------------------------------------------------------===//

bool PrepareLogFile(std::string file_name);

//===--------------------------------------------------------------------===//
// CHECK RECOVERY
//===--------------------------------------------------------------------===//

void ResetSystem(void);

void DoRecovery(std::string file_name);

//===--------------------------------------------------------------------===//
// WRITING LOG RECORD
//===--------------------------------------------------------------------===//

void BuildLog();

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
