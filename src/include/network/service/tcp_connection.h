//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcp_connection.h
//
// Identification: src/include/network/tcp_connection.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "rpc_server.h"
#include "rpc_channel.h"
#include "rpc_controller.h"
#include "network_address.h"
#include "common/logger.h"

#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event.h>

#include <memory>

namespace peloton {
namespace network {
namespace service {

////////////////////////////////////////////////////////////////////////////////
//                  message structure:
// --Header:  message length (Type+Opcode+request),      uint32_t (4bytes)
// --Type:    message type: REQUEST or RESPONSE          uint16_t (2bytes)
// --Opcode:  std::hash(methodname)-->Opcode,            uint64_t (8bytes)
// --Content: the serialization result of protobuf       Header-8-2
//
// TODO: We did not add checksum code in this version     ///////////////

#define HEADERLEN 4  // the length should be equal with sizeof uint32_t
#define OPCODELEN 8  // the length should be equal with sizeof uint64_t
#define TYPELEN 2    // the length should be equal with sizeof uint16_t

/*
 * Connection is thread-safe
 */
class Connection {
  typedef enum {
    INIT,
    CONNECTED,  // we probably do not need CONNECTED
    SENDING,
    RECVING
  } ConnStatus;

 public:
  /*
   * @brief A connection has its own evenbase.
   * @param fd is the socket
   *            If a connection is created by server, fd(socket) is passed by
   * listener
   *            If a connection is created by client, fd(socket) is -1.
   *        arg is used to pass the rpc_server pointer
   */
  Connection(int fd, struct event_base* base, void* arg, NetworkAddress& addr);
  ~Connection();

  static void ReadCb(struct bufferevent* bev, void* ctx);
  static void EventCb(struct bufferevent* bev, short events, void* ctx);
  static void* ProcessMessage(void* connection);

  static void BufferCb(struct evbuffer* buffer,
                       const struct evbuffer_cb_info* info, void* arg);

  RpcServer* GetRpcServer();
  NetworkAddress& GetAddr();

  /*
   * set the connection status
   */
  void SetStatus(ConnStatus status);

  /*
   * get the connection status
   */
  ConnStatus GetStatus();

  /*
   * @brief After a connection is created, you can use this function to connect
   * to
   *        any server with the given address
   */
  bool Connect(const NetworkAddress& addr);

  /*
   * @brief a rpc will be closed by client after it recvs the response by server
   *        close frees the socket event
   */
  void Close();

  /*
   * This is used by client to execute callback function
   */
  void SetMethodName(std::string name);

  /*
   * This is used by client to execute callback function
   */
  const char* GetMethodName();

  /*
   * Get the readable length of the read buf
   */
  int GetReadBufferLen();

  /*
   * Get the len data from read buf and then save them in the give buffer
   * If the data in read buf are less than len, get all of the data
   * Return the length of moved data
   * Note: the len data are deleted from read buf after this operation
   */
  int GetReadData(char* buffer, int len);

  /*
   * copy data (len) from read buf into the given buffer,
   * if the total data is less than len, then copy all of the data
   * return the length of the copied data
   * the data still exist in the read buf after this operation
   */
  int CopyReadBuffer(char* buffer, int len);

  /*
   * Get the lengh a write buf
   */
  int GetWriteBufferLen();

  /*
   * Add data to write buff,
   * return true on success, false on failure.
   */
  bool AddToWriteBuffer(char* buffer, int len);

  /*
   * Forward data in read buf into write buf
   */
  void MoveBufferData();

 private:
  // addr is the other side address
  NetworkAddress addr_;
  bool close_;

  ConnStatus status_;

  RpcServer* rpc_server_;

  struct bufferevent* bev_;
  struct event_base* base_;

  std::string method_name_;

  // this can be used in buffer cb
  // int total_send_;
};

}  // namespace service
}  // namespace network
}  // namespace peloton
