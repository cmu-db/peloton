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
#include <mutex>

#ifndef FRONTEND_GLOBALS_H
#define FRONTEND_GLOBALS_H

namespace peloton {
namespace wire {

// globals used by all client connections
struct ThreadGlobals {
	std::mutex sqlite_mutex; // used for CC over sqlite
};

}
}
#endif //FRONTEND_GLOBALS_H
