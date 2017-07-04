//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_tests_constrain_util.h
//
// Identification: test/include/executor/executor_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "type/types.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace catalog {
class Column;
class Manager;
}

namespace concurrency {
class Transaction;
}

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace storage {
class Backend;
class TileGroup;
class DataTable;
class Tuple;
class Database;
}

#define TESTS_TUPLES_PER_TILEGROUP 5
#define DEFAULT_TILEGROUP_COUNT 3

namespace test {

}

