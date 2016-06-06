//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// globals.h
//
// Identification: src/wire/globals.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

namespace peloton {
namespace wire {

// globals used by all client connections
struct ThreadGlobals {
  std::mutex sqlite_mutex; // used for CC over sqlite
};

}  // End wire namespace
}  // End peloton namespace
