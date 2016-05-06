//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_manager.cpp
//
// Identification: src/backend/networking/connection_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "connection_manager.h"
#include "backend/common/assert.h"

namespace peloton {
namespace networking {

ConnectionManager& ConnectionManager::GetInstance(void) {
  static ConnectionManager connection_nanager;
  return connection_nanager;
}

// the format of every item in addlist is ip:port
ConnectionManager::ConnectionManager() : rpc_server_(NULL), cond_(&mutex_) {
  struct timeval start;
  gettimeofday(&start, NULL);
  start_time_ = start.tv_usec;
}

ConnectionManager::~ConnectionManager() {
  std::map<NetworkAddress, Connection*>::iterator iter;

  for (iter = conn_pool_.begin(); iter != conn_pool_.end(); iter++) {
    ASSERT(iter->second != NULL);
    delete iter->second;
  }
}

void ConnectionManager::ResterRpcServer(RpcServer* server) {
  // this function is only called once, but in case, we still have lock here
  mutex_.Lock();
  rpc_server_ = server;
  mutex_.UnLock();
}

RpcServer* ConnectionManager::GetRpcServer() {
  assert(rpc_server_ != NULL);
  return rpc_server_;
}

/*
 * if the there is no connection, create it
 */
Connection* ConnectionManager::GetConn(std::string& addr) {
  NetworkAddress netaddr(addr);

  return GetConn(netaddr);
}

struct event_base* ConnectionManager::GetEventBase() {
  struct event_base* base;
  // TODO: should we lock the server?
  // mutex_.Lock();
  if (rpc_server_ == NULL) {
    base = NULL;
  } else {
    base = rpc_server_->GetListener()->GetEventBase();
  }
  // mutex_.UnLock();

  return base;
}
/*
 * if the there is no corresponding connection, create it
 */
Connection* ConnectionManager::GetConn(NetworkAddress& addr) {
  Connection* conn = NULL;

  /* conn_pool_ is a critical section */
  mutex_.Lock();

  /* Check whether the connection already exists */
  std::map<NetworkAddress, Connection*>::iterator iter = conn_pool_.find(addr);

  /* If there is such a connection, create a connection*/
  if (iter == conn_pool_.end()) {
    /* Before creating a connection we should know the event base*/
    struct event_base* base = GetEventBase();

    if (base == NULL) {
      LOG_ERROR("No event base when creating a connection");
      return NULL;
    }

    /* A connection should know rpc server, which is used to find RPC method */
    RpcServer* server = GetRpcServer();
    assert(server != NULL);

    /* For a client connection, the socket fd is -1 (required by libevent)
     * After new a connection, a bufferevent is created and callback is set
     */
    conn = new Connection(-1, base, server, addr);

    /* Connect to server with the given address
     * Note: when connect return true, it doesn't mean connect successfully
     *       we might still get error in event callback
     */
    if (conn->Connect(addr) == false) {
      LOG_TRACE("Connect Error ---> ");
      delete conn;
      return NULL;
    }

    /* Put the new connection in conn_pool
     * Note: if we get close error in event callback, we should remove the
     * connection
     */
    conn_pool_.insert(std::pair<NetworkAddress, Connection*>(addr, conn));
    LOG_TRACE("Connect to ---> %s:%d", addr.IpToString().c_str(),
              addr.GetPort());

  } else {
    /*
     * We already have the connection in the conn_pool
     */
    conn = iter->second;
  }

  mutex_.UnLock();

  return conn;
}

/*
 * This method is only for test, we don't use it for now!!!
 */
Connection* ConnectionManager::CreateConn(NetworkAddress& addr) {
  Connection* conn = NULL;

  /* conn_pool_ is a critical section */
  //    mutex_.Lock();

  /* Check whether the connection already exists */
  std::map<NetworkAddress, Connection*>::iterator iter =
      client_conn_pool_.find(addr);

  /* If there is such a connection, create a connection*/
  if (iter == client_conn_pool_.end()) {
    /* Before creating a connection we should know the event base*/
    struct event_base* base = GetEventBase();

    if (base == NULL) {
      LOG_ERROR("No event base when creating a connection");
      return NULL;
    }

    /* A connection should know rpc server, which is used to find RPC method */
    RpcServer* server = GetRpcServer();
    assert(server != NULL);

    /* For a client connection, the socket fd is -1 (required by libevent)
     * After new a connection, a bufferevent is created and callback is set
     */
    conn = new Connection(-1, base, server, addr);

    /* Connect to server with the given address
     * Note: when connect return true, it doesn't mean connect successfully
     *       we might still get error in event callback
     */
    if (conn->Connect(addr) == false) {
      LOG_TRACE("Connect Error ---> ");
      delete conn;
      return NULL;
    }

    /* Put the new connection in conn_pool
     * Note: if we get close error in event callback, we should remove the
     * connection
     */
    client_conn_pool_.insert(
        std::pair<NetworkAddress, Connection*>(addr, conn));
    LOG_TRACE("Connect to ---> %s:%d", addr.IpToString().c_str(),
              addr.GetPort());

  } else {
    /*
     * We already have the connection in the conn_pool
     */
    conn = iter->second;
  }

  //    mutex_.UnLock();

  return conn;
}

/*
 * if the there is no connection, return NULL
 */
Connection* ConnectionManager::FindConn(NetworkAddress& addr) {
  mutex_.Lock();
  std::map<NetworkAddress, Connection*>::iterator iter = conn_pool_.find(addr);
  mutex_.UnLock();

  if (iter == conn_pool_.end()) return NULL;

  return iter->second;
}

bool ConnectionManager::AddConn(NetworkAddress addr, Connection* conn) {
  // the map is a critical section
  mutex_.Lock();

  std::map<NetworkAddress, Connection*>::iterator iter = conn_pool_.find(addr);

  if (iter != conn_pool_.end()) {
    /* If we already have the connection in conn_pool, we do nothing
     * return false. The caller should do the rest.
     */
    mutex_.UnLock();
    return false;
  } else {
    conn_pool_.insert(std::pair<NetworkAddress, Connection*>(addr, conn));
  }

  mutex_.UnLock();

  return true;
}

bool ConnectionManager::AddConn(struct sockaddr& addr, Connection* conn) {
  NetworkAddress netaddr(addr);
  // the map is a critical section
  return AddConn(netaddr, conn);
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
  ASSERT(iter->second != NULL);
  delete iter->second;
  conn_pool_.erase(iter);
  mutex_.UnLock();

  return true;
}

/*
 * if there is no corresponding connection, return false
 */
bool ConnectionManager::DeleteConn(Connection* conn) {
  NetworkAddress& addr = conn->GetAddr();
  return DeleteConn(addr);
}

}  // End peloton networking
}  // End peloton namespace
