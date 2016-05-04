//
// Created by siddharth on 27/4/16.
//

#include <mutex>

#ifndef FRONTEND_GLOBALS_H
#define FRONTEND_GLOBALS_H

namespace peloton {
namespace wire {

// globals used by all client connections
struct ThreadGlobals {
	std::mutex sqlite_mutex;
};

}
}
#endif //FRONTEND_GLOBALS_H
