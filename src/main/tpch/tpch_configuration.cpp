//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_configuration.cpp
//
// Identification: src/main/tpch/tpch_configuration.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_configuration.h"

#include <sys/stat.h>

#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpch {

oid_t kIntSize =
    static_cast<oid_t>(type::Type::GetTypeSize(type::Type::TypeId::INTEGER));

oid_t kDateSize =
    static_cast<oid_t>(type::Type::GetTypeSize(type::Type::TypeId::DATE));

oid_t kBigIntSize =
    static_cast<oid_t>(type::Type::GetTypeSize(type::Type::TypeId::BIGINT));

oid_t kDecimalSize =
    static_cast<oid_t>(type::Type::GetTypeSize(type::Type::TypeId::DECIMAL));

bool Configuration::IsValid() const {
  struct stat info;
  if (stat(data_dir.c_str(), &info) != 0) {
    LOG_ERROR("Data directory [%s] isn't accessible", data_dir.c_str());
    return false;
  } else if ((info.st_mode & S_IFDIR) == 0) {
    LOG_ERROR("Data directory [%s] isn't a directory", data_dir.c_str());
    return false;
  }
  auto inputs = {GetCustomerPath(), GetLineitemPath(),
                 GetNationPath(),   GetOrdersPath(),
                 GetPartSuppPath(), GetPartPath(),
                 GetSupplierPath(), GetRegionPath()};
  for (const auto &input : inputs) {
    struct stat info;
    if (stat(input.c_str(), &info) != 0) {
      LOG_ERROR("Input file [%s] isn't accessible", input.c_str());
      return false;
    }
  }

  // All good
  return true;
}

std::string Configuration::GetInputPath(std::string file_name) const {
  auto name = file_name + "." + suffix;
  return data_dir +
         (data_dir[data_dir.length() - 1] == '/' ? name : "/" + name);
}

std::string Configuration::GetCustomerPath() const {
  return GetInputPath("customer");
}

std::string Configuration::GetLineitemPath() const {
  return GetInputPath("lineitem");
}

std::string Configuration::GetNationPath() const {
  return GetInputPath("nation");
}

std::string Configuration::GetOrdersPath() const {
  return GetInputPath("orders");
}

std::string Configuration::GetPartSuppPath() const {
  return GetInputPath("partsupp");
}

std::string Configuration::GetPartPath() const { return GetInputPath("part"); }

std::string Configuration::GetSupplierPath() const {
  return GetInputPath("supplier");
}

std::string Configuration::GetRegionPath() const {
  return GetInputPath("region");
}

void Configuration::SetRunnableQueries(char *query_list) {
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

bool Configuration::ShouldRunQuery(QueryId qid) const {
  return queries_to_run[static_cast<uint32_t>(qid)];
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton