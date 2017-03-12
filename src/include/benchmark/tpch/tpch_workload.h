//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload.h
//
// Identification: src/include/benchmark/tpch/tpch_workload.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace peloton {
namespace benchmark {
namespace tpch {

class TPCHDatabase;
class TPCHLoader;

struct Configuration {
  // Default 64K tuples per tile group
  uint32_t tuples_per_tile_group = 1 << 16;

  // The scale factor of the benchmark
  double scale_factor = 1;

  // The directory where all the data files are
  std::string data_dir;

  // The suffix of all the files
  std::string suffix;

  // Do we dictionary encode strings?
  bool dictionary_encode = true;

  // Which queries will the benchmark run?
  bool queries_to_run[22] = {false};

  std::string GetInputPath(std::string file_name) const {
    auto name = file_name + "." + suffix;
    return data_dir +
           (data_dir[data_dir.length() - 1] == '/' ? name : "/" + name);
  }

  std::string GetCustomerPath() const { return GetInputPath("customer"); }
  std::string GetLineitemPath() const { return GetInputPath("lineitem"); }
  std::string GetNationPath() const { return GetInputPath("nation"); }
  std::string GetOrdersPath() const { return GetInputPath("orders"); }
  std::string GetPartSuppPath() const { return GetInputPath("partsupp"); }
  std::string GetPartPath() const { return GetInputPath("part"); }
  std::string GetSupplierPath() const { return GetInputPath("supplier"); }
  std::string GetRegionPath() const { return GetInputPath("region"); }

  enum class QueryId {
    Q1 = 0,
    Q2,
    Q3,
    Q4,
    Q5,
    Q6,
    Q7,
    Q8,
    Q9,
    Q10,
    Q11,
    Q12,
    Q13,
    Q14,
    Q15,
    Q16,
    Q17,
    Q18,
    Q19,
    Q20,
    Q21,
    Q22,
  };

  void SetRunnableQueries(char *query_list) {
    // Disable all queries
    for (uint32_t i = 0; i < 22; i++) queries_to_run[i] = false;

    // Now pull out the queries the user actually wants to run
    char *ptr = strtok(query_list, ",");
    while (ptr != nullptr) {
      int query = atoi(ptr);
      if (query >= 1 && query <= 22) {
        queries_to_run[query - 1] = true;
      }
      ptr = strtok(nullptr, ",");
    }
  }

  bool ShouldRunQuery(QueryId query) const {
    return queries_to_run[static_cast<uint32_t>(query)];
  }
};

// The benchmark
class TPCHBenchmark {
 public:
  TPCHBenchmark(const Configuration &config, TPCHDatabase &db);

  void RunBenchmark();

  void RunQ1();
  void RunQ2();
  void RunQ3();
  void RunQ4();
  void RunQ5();
  void RunQ6();
  void RunQ7();
  void RunQ8();
  void RunQ9();
  void RunQ10();
  void RunQ11();
  void RunQ12();
  void RunQ13();
  void RunQ14();
  void RunQ15();
  void RunQ16();
  void RunQ17();
  void RunQ18();
  void RunQ19();
  void RunQ20();
  void RunQ21();
  void RunQ22();

 private:
  // The benchmark configuration
  const Configuration &config_;
  // The TPCH database
  TPCHDatabase &db_;
};

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton