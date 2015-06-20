/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

namespace nstore {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Class 
//===--------------------------------------------------------------------===//

class DDL {
public:
  static int CreateTable(int arg);

};

extern "C" {
  int DDL_CreateTable(int arg);
}

} // namespace bridge
} // namespace nstore
