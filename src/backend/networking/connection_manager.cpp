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

ConnectionManager &ConnectionManager::GetInstance(void) {
    static ConnectionManager connection_nanager;
    return connection_nanager;
}

// the format of every item in addlist is ip:port
ConnectionManager::ConnectionManager() :
        rpc_server_(NULL),
        cond_(&mutex_) {}

ConnectionManager::~ConnectionManager() {}

void ConnectionManager::ResterRpcServer(RpcServer* server) {
    // this function is only called once, but in case, we still have lock here
    mutex_.Lock();
    rpc_server_ = server;
    mutex_.UnLock();
}

/*
 * if the there is no connection, create it
 */
Connection* ConnectionManager::GetConn(std::string& addr) {

    NetworkAddress netaddr(addr);

    return GetConn(netaddr);
}

event_base* ConnectionManager::GetEventBase() {

    event_base* base;
    mutex_.Lock();
    if (rpc_server_ == NULL) {
        base = NULL;
    } else {
        base = rpc_server_->GetListener()->GetEventBase();
    }
    mutex_.UnLock();

    return base;
}
/*
 * if the there is no corresponding connection, create it
 */
Connection* ConnectionManager::GetConn(NetworkAddress& addr) {

    Connection* conn = NULL;

    mutex_.Lock();

    std::map<NetworkAddress, Connection*>::iterator iter =
            conn_pool_.find(addr);

    if (iter == conn_pool_.end()) {

        event_base* base = GetEventBase();

        if (base == NULL) {
            return NULL;
        }
        // for a client connection, the socket is -1 and the rpc_server is NULL
        conn = new Connection(-1, NULL, base);

        // Connect to server with the given address
        if ( conn->Connect(addr) == false ) {
            LOG_TRACE("Connect Error ---> ");
            return NULL;
        }

        conn_pool_.insert(std::pair<NetworkAddress, Connection*>(addr, conn));

    } else {
        conn = iter->second;
    }

    mutex_.UnLock();

    return conn;
}

/*
 * if the there is no connection, return NULL
 */
Connection* ConnectionManager::FindConn(NetworkAddress& addr) {

    mutex_.Lock();
    std::map<NetworkAddress, Connection*>::iterator iter =
            conn_pool_.find(addr);
    mutex_.UnLock();

    if (iter == conn_pool_.end())
      return NULL;

    return iter->second;
}

void ConnectionManager::AddConn(NetworkAddress addr, Connection* conn) {
    // the map is a critical section
    mutex_.Lock();
    conn_pool_.insert(std::pair<NetworkAddress, Connection*>(addr, conn));
    mutex_.UnLock();
}

void ConnectionManager::AddConn(struct sockaddr& addr, Connection* conn) {
    NetworkAddress netaddr(addr);
    // the map is a critical section
    AddConn(netaddr, conn);
}
/*
 * if there is no corresponding connection, return false
 */
bool ConnectionManager::DeleteConn(NetworkAddress& addr) {
    // the map is a critical section
    mutex_.Lock();
    std::map<NetworkAddress, Connection*>::iterator iter;
    iter = conn_pool_.find(addr);
    if (iter == conn_pool_.end()) {
        mutex_.UnLock();
        return false;
    }
    conn_pool_.erase(iter);
    mutex_.UnLock();

    return true;
}
}  // End peloton networking
}  // End peloton namespace

