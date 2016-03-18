//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// connection_manager.h
//
// Identification: /peloton/src/backend/networking/connection_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/mutex.h"
#include "backend/networking/tcp_connection.h"

#include <event2/event.h>
#include <pthread.h>

namespace peloton {
namespace networking {
//===--------------------------------------------------------------------===//
// Connection Manager
//===--------------------------------------------------------------------===//

class ConnectionManager {
    ConnectionManager & operator=(const ThreadManager &) = delete;
    ConnectionManager & operator=(ThreadManager &&) = delete;

 public:
    // global singleton
    static ConnectionManager &GetInstance(void);

    void ResterRpcServer(RpcServer* server);
    event_base* GetEventBase();

    Connection* GetConn(std::string& addr);

    Connection* GetConn(NetworkAddress& addr);
    Connection* FindConn(NetworkAddress& addr);
    bool AddConn(NetworkAddress addr, Connection* conn);
    bool AddConn(struct sockaddr& addr, Connection* conn);
    bool DeleteConn(NetworkAddress& addr);

    ConnectionManager();
    ~ConnectionManager();

 private:

    // rpc server
    RpcServer* rpc_server_;

    // all the connections established: addr --> connection instance
    std::map<NetworkAddress, Connection*> conn_pool_;

    // a connection can be shared among pthreads
    Mutex mutex_;
    Condition cond_;

};

}  // End peloton networking
}  // End peloton namespace
