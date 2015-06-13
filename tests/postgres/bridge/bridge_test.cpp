/*-------------------------------------------------------------------------
 *
 * parser_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/parser/parser_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"
#include "harness.h"

#include <iostream>

extern "C" {

#include "postgres/include/bridge/bridge.h"
#include "postgres/include/postmaster/postmaster.h"
#include "postgres/include/utils/memutils.h"

}

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Bridge Tests
//===--------------------------------------------------------------------===//

int InitPeloton(){

  int pid = fork();
  // Child process
  if ( pid == 0 ) {
    TopMemoryContext = AllocSetContextCreate((MemoryContext) NULL, "TopMemoryContext",
                                             0, 8 * 1024, 8 * 1024);

    MemoryContextSwitchTo(TopMemoryContext);

    ErrorContext = AllocSetContextCreate(TopMemoryContext, "ErrorContext",
                                         8 * 1024, 8 * 1024, 8 * 1024);
    MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext",
                                           8 * 1024, 8 * 1024, 8 * 1024);

    // Start Postmaster
    int argc = 3;
    char const *argv[] =  { "/usr/local/peloton/bin/peloton", "-D", "data"};
    PostmasterMain(argc, (char **)argv);

    return 0;
  }
  // Parent process
  else {
    std::cout << "Started Peloton... \n";
    return pid;
  }
}

int StopPeloton(int pid) {
  int retval = kill(pid, SIGKILL);
  return retval;
}

TEST(BridgeTests, BasicTest) {

  // Start Peloton
  int pid = InitPeloton();

  try {

    char *relation_name = ::GetRelationName(1260);
    std::cout << "Relation name :: " << relation_name << "\n";
  }
  catch (std::exception& e) {
    StopPeloton(pid);
  }

  StopPeloton(pid);
}

} // End test namespace
} // End nstore namespace

