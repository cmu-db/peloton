//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload.cpp
//
// Identification: src/main/tpch/tpch_workload.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_workload.h"

#include "benchmark/tpch/tpch_database.h"
#include "../../include/benchmark/tpch/tpch_database.h"
#include "../../include/benchmark/tpch/tpch_workload.h"

#include "../../include/common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpch {

TPCHBenchmark::TPCHBenchmark(const Configuration &config,
                             TPCHDatabase &db)
    : config_(config), db_(db) {}

void TPCHBenchmark::RunBenchmark() {
  RunQ1();
}

void TPCHBenchmark::RunQ1() {
  LOG_INFO("Running TPCH Q1");

  // Load the lineitem table
  db_.LoadLineitemTable();
}

void TPCHBenchmark::RunQ2() {}
void TPCHBenchmark::RunQ3() {}
void TPCHBenchmark::RunQ4() {}
void TPCHBenchmark::RunQ5() {}
void TPCHBenchmark::RunQ6() {}
void TPCHBenchmark::RunQ7() {}
void TPCHBenchmark::RunQ8() {}
void TPCHBenchmark::RunQ9() {}
void TPCHBenchmark::RunQ10() {}
void TPCHBenchmark::RunQ11() {}
void TPCHBenchmark::RunQ12() {}
void TPCHBenchmark::RunQ13() {}
void TPCHBenchmark::RunQ14() {}
void TPCHBenchmark::RunQ15() {}
void TPCHBenchmark::RunQ16() {}
void TPCHBenchmark::RunQ17() {}
void TPCHBenchmark::RunQ18() {}
void TPCHBenchmark::RunQ19() {}
void TPCHBenchmark::RunQ20() {}
void TPCHBenchmark::RunQ21() {}
void TPCHBenchmark::RunQ22() {}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton