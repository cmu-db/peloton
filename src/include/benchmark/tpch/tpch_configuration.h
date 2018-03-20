//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_configuration.h
//
// Identification: src/include/benchmark/tpch/tpch_configuration.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

#include "common/internal_types.h"

namespace peloton {
namespace benchmark {
namespace tpch {

//===----------------------------------------------------------------------===//
// Type size constants
//===----------------------------------------------------------------------===//

extern oid_t kIntSize;
extern oid_t kDateSize;
extern oid_t kBigIntSize;
extern oid_t kDecimalSize;

//===----------------------------------------------------------------------===//
// Query and Table types
//===----------------------------------------------------------------------===//

enum class QueryId : uint32_t {
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

enum class TableId : uint32_t {
  Part = 44,
  Supplier = 45,
  PartSupp = 46,
  Customer = 47,
  Nation = 48,
  Lineitem = 49,
  Region = 50,
  Orders = 51,
};

//===----------------------------------------------------------------------===//
// The benchmark configuration
//===----------------------------------------------------------------------===//

struct Configuration {
  // Default 64K tuples per tile group
  uint32_t tuples_per_tile_group = 1 << 16;

  // The scale factor of the benchmark
  double scale_factor = 1;

  // The number of runs to average over
  uint32_t num_runs = 10;

  // The directory where all the data files are
  std::string data_dir;

  // The suffix of all the files
  std::string suffix;

  // Do we dictionary encode strings?
  bool dictionary_encode = true;

  // Which queries will the benchmark run?
  bool queries_to_run[22] = {false};

  bool IsValid() const;

  std::string GetInputPath(std::string file_name) const;

  std::string GetCustomerPath() const;
  std::string GetLineitemPath() const;
  std::string GetNationPath() const;
  std::string GetOrdersPath() const;
  std::string GetPartSuppPath() const;
  std::string GetPartPath() const;
  std::string GetSupplierPath() const;
  std::string GetRegionPath() const;

  void SetRunnableQueries(char *query_list);

  bool ShouldRunQuery(QueryId qid) const;
};

}  // namespace tpch
}  // namespace benchmark
}  // namespace pelton