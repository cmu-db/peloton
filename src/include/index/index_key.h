//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_key.h
//
// Identification: src/include/index/index_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <sstream>

#include "type/value_peeker.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/tuple.h"
#include "index/index.h"

#include <boost/functional/hash.hpp>

namespace peloton {
namespace index {

using namespace peloton::common;

}  // End index namespace
}  // End peloton namespace

#include "ints_key.h"
#include "generic_key.h" 
#include "tuple_key.h" 



