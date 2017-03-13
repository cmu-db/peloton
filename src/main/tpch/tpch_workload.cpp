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
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpch {

TPCHBenchmark::TPCHBenchmark(const Configuration &config, TPCHDatabase &db)
    : config_(config), db_(db) {}

void TPCHBenchmark::RunBenchmark() {
  if (config_.ShouldRunQuery(Configuration::QueryId::Q1)) {
    RunQ1();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q2)) {
    RunQ2();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q3)) {
    RunQ3();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q4)) {
    RunQ4();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q5)) {
    RunQ5();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q6)) {
    RunQ6();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q7)) {
    RunQ7();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q8)) {
    RunQ8();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q9)) {
    RunQ9();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q10)) {
    RunQ10();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q11)) {
    RunQ11();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q12)) {
    RunQ12();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q13)) {
    RunQ13();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q14)) {
    RunQ14();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q15)) {
    RunQ15();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q16)) {
    RunQ16();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q17)) {
    RunQ17();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q18)) {
    RunQ18();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q19)) {
    RunQ19();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q20)) {
    RunQ20();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q21)) {
    RunQ21();
  }
  if (config_.ShouldRunQuery(Configuration::QueryId::Q22)) {
    RunQ22();
  }
}

void TPCHBenchmark::RunQ1() {
  LOG_INFO("Running TPCH Q1");

  // Load the lineitem table
  db_.LoadLineitemTable();
}

void TPCHBenchmark::RunQ2() {
  LOG_INFO("Running TPCH Q2");
}

void TPCHBenchmark::RunQ3() {
  LOG_INFO("Running TPCH Q3");
}

void TPCHBenchmark::RunQ4() {
  LOG_INFO("Running TPCH Q4");
}

void TPCHBenchmark::RunQ5() {
  LOG_INFO("Running TPCH Q5");
}

void TPCHBenchmark::RunQ6() {
  LOG_INFO("Running TPCH Q6");
}

void TPCHBenchmark::RunQ7() {
  LOG_INFO("Running TPCH Q7");
}

void TPCHBenchmark::RunQ8() {
  LOG_INFO("Running TPCH Q8");
}

void TPCHBenchmark::RunQ9() {
  LOG_INFO("Running TPCH Q9");
}

void TPCHBenchmark::RunQ10() {
  LOG_INFO("Running TPCH Q10");
}

void TPCHBenchmark::RunQ11() {
  LOG_INFO("Running TPCH Q11");
}

void TPCHBenchmark::RunQ12() {
  LOG_INFO("Running TPCH Q12");
}

void TPCHBenchmark::RunQ13() {
  LOG_INFO("Running TPCH Q13");
}

void TPCHBenchmark::RunQ14() {
  LOG_INFO("Running TPCH Q14");
}

void TPCHBenchmark::RunQ15() {
  LOG_INFO("Running TPCH Q15");
}

void TPCHBenchmark::RunQ16() {
  LOG_INFO("Running TPCH Q16");
}

void TPCHBenchmark::RunQ17() {
  LOG_INFO("Running TPCH Q17");
}

void TPCHBenchmark::RunQ18() {
  LOG_INFO("Running TPCH Q18");
}

void TPCHBenchmark::RunQ19() {
  LOG_INFO("Running TPCH Q19");
}

void TPCHBenchmark::RunQ20() {
  LOG_INFO("Running TPCH Q20");
}

void TPCHBenchmark::RunQ21() {
  LOG_INFO("Running TPCH Q21");
}

void TPCHBenchmark::RunQ22() {
  LOG_INFO("Running TPCH Q22");
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton