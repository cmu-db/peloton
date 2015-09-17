//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// main.h
//
// Identification: src/backend/main/main.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <getopt.h>
#include <iostream>
#include <string>
#include <cstdlib>

/*!
 \brief Peloton DBMS namespace.
 */
namespace peloton {

class configuration {
 public:
  std::string filesystem_path;
};

// sample test helper
int SampleFunc(int a, int b) { return a + b; }
}
