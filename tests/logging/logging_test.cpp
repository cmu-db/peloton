#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"

#include <fstream>

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

// Logging mode
extern LoggingType     peloton_logging_mode;

extern size_t peloton_pmem_file_size;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test 
//===--------------------------------------------------------------------===//

std::string aries_log_file_name = "aries.log";

std::string peloton_log_file_name = "peloton.log";

/**
 * @brief writing a simple log with multiple threads and then do recovery
 */
TEST(LoggingTests, RecoveryTest) {

  // First, set the global peloton logging mode and pmem file size
  peloton_logging_mode = state.logging_type;
  peloton_pmem_file_size = state.pmem_file_size;
  // Set default experiment type
  if(state.experiment_type == LOGGING_EXPERIMENT_TYPE_INVALID)
    state.experiment_type = LOGGING_EXPERIMENT_TYPE_ACTIVE;

  if(state.logging_type == LOGGING_TYPE_ARIES) {

    // Prepare a simple log file
    EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(aries_log_file_name));

    // Reset data
    LoggingTestsUtil::ResetSystem();

    // Do recovery
    LoggingTestsUtil::DoRecovery(aries_log_file_name);

  }
  else if(state.logging_type == LOGGING_TYPE_PELOTON) {

    // Test a simple log process
    EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(peloton_log_file_name));

    // Do recovery
    LoggingTestsUtil::DoRecovery(peloton_log_file_name);

  }
  else if(state.logging_type == LOGGING_TYPE_INVALID) {

    // Test a simple log process
    EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(peloton_log_file_name));

  }

}


}  // End test namespace
}  // End peloton namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Setup testing configuration
  peloton::test::LoggingTestsUtil::ParseArguments(argc, argv);

  return RUN_ALL_TESTS();
}
