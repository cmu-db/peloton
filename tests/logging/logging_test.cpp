#include "gtest/gtest.h"

#include <fstream>

#include "logging/logging_tests_util.h"
#include "backend/common/logger.h"


//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

// Logging mode
extern LoggingType peloton_logging_mode;

extern size_t peloton_data_file_size;

extern int64_t peloton_wait_timeout;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Test
//===--------------------------------------------------------------------===//

std::string wal_log_file_name = "wal.log";

std::string wbl_log_file_name = "wbl.log";

/**
 * @brief writing a simple log with multiple threads and then do recovery
 */
TEST(LoggingTests, RecoveryTest) {
  // First, set the global peloton logging mode and pmem file size
  peloton_logging_mode = state.logging_type;
  peloton_data_file_size = state.data_file_size;
  peloton_wait_timeout = state.wait_timeout;

  // Set default experiment type
  if (state.experiment_type == LOGGING_EXPERIMENT_TYPE_INVALID)
    state.experiment_type = LOGGING_EXPERIMENT_TYPE_ACTIVE;

  //===--------------------------------------------------------------------===//
  // WAL
  //===--------------------------------------------------------------------===//
  if (IsBasedOnWriteAheadLogging(peloton_logging_mode)) {
    // Prepare a simple log file
    EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(wal_log_file_name));

    // Reset data
    LoggingTestsUtil::ResetSystem();

    // Do recovery
    LoggingTestsUtil::DoRecovery(wal_log_file_name);

  }
  //===--------------------------------------------------------------------===//
  // WBL
  //===--------------------------------------------------------------------===//
  else if (IsBasedOnWriteBehindLogging(peloton_logging_mode)) {
    // Test a simple log process
    EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(wbl_log_file_name));

    // Do recovery
    LoggingTestsUtil::DoRecovery(wbl_log_file_name);

  }
  //===--------------------------------------------------------------------===//
  // None
  //===--------------------------------------------------------------------===//
  else if (state.logging_type == LOGGING_TYPE_INVALID) {
    // Test a simple log process
    EXPECT_TRUE(LoggingTestsUtil::PrepareLogFile(wbl_log_file_name));

    // No recovery
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
