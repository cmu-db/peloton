#pragma once

#define private public

#include "catalog/catalog.h"
#include "tcop/tcop.h"

namespace peloton {

namespace test {

class OptimizerTestsUtil {
 public:
  // Create database instance for optimizer tests
  static void CreateDatabase(const std::string name);

  // Create a table for optimizer tests
  static void CreateTable(const std::string stmt);

  // Generate random integer value
  static int GenerateRandomInt(const int min, const int max);

  // Generate random double value
  static double GenerateRandomDouble(const double min, const double max);

  // Generate random string
  static std::string GenerateRandomString(const int len=8);

  // Free resources
  static void DestroyDatabase(const std::string name);

  static tcop::TrafficCop traffic_cop_;

};

} // test
} // peloton
