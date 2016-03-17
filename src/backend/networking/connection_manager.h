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

    Connection* GetConn(std::string& addr);
    Connection* GetConn(struct sockaddr);

    Connection* FindConn(std::string& addr);
    Connection* FindConn(struct sockaddr);

    bool AddConn(std::string& addr);
    bool AddConn(std::string& addr, Connection* conn);

    bool DeleteConn(std::string& addr);

 private:
    // the format of every item in addlist is ip:port
    ConnectionManager(std::list<std::string&>& addrlist);
    ~ConnectionManager();

 private:

    // all the connections established: addr --> connection instance
    std::map<NetworkAddress, Connection*> conn_pool_;

    pthread_mutex_t mutex_;
    pthread_cond_t cond_;

};

}  // End peloton networking
}  // End peloton namespace



