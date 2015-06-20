/*-------------------------------------------------------------------------
 *
 * dll.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/bridge/ddl.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "bridge/ddl.h"
#include "executor/create_executor.h"

namespace nstore {
namespace ddl {

int DDL::CreateTable(int arg){
  return arg * 2;
}

extern "C" int DDL_CreateTable(int arg) {
  return DDL::CreateTable(arg);
}

} // namespace ddl
} // namespace nstore
