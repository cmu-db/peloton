/*
 * test_util.h
 *
 *  copyright(c) CMU, 2015
 *
 *  Created on: Jun 25, 2015
 *      Author: Ming Fang
 */

#pragma once

#include <string>
extern "C" {
#include "nodes/pprint.h"
}

namespace nstore {
namespace bridge {

class TestUtil {
 public:
  TestUtil() {}
  ~TestUtil() {}

  // Not working
  static PlanState *PlanQuery(std::string query);


};
}
}





