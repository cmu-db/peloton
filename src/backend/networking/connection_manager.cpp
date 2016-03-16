//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// connection_manager.cpp
//
// Identification: /peloton/src/backend/networking/connection_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "connection_manager.h"

namespace peloton {
namespace networking {

// the format of every item in addlist is ip:port
ConnectionManager::ConnectionManager(std::list<std::string&>& addrlist) {

    for (std::list<std::string>::iterator iter = addrlist.begin(); iter != addrlist.end(); iter++) {

        // for a client connection, the socket is -1 and the rpc_server is NULL
        Connection conn = new Connection(-1, NULL);

        // Connect to server with given address
        if ( conn->Connect(addr_) == false ) {
            LOG_TRACE("Connect Error ---> ");

        }

    }
}

ConnectionManager::~ConnectionManager() {}

}  // End peloton networking
}  // End peloton namespace

