//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_connection.h
//
// Identification: /peloton/src/backend/networking/tcp_connection.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/logger.h"
#include "rpc_server.h"
#include "rpc_channel.h"
#include "rpc_controller.h"
#include "tcp_address.h"

#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event.h>

#include <memory>

namespace peloton {
namespace networking {

#define HEADERLEN  4    // the length should be equal with sizeof uint32_t
#define OPCODELEN  8    // the length should be equal with sizeof uint64_t

class Connection {

public:

    /*
     * @brief A connection has its own evenbase.
     * @param fd is the socket
     *            If a connection is created by server, fd(socket) is passed by listener
     *            If a connection is created by client, fd(socket) is -1.
     *        arg is used to pass the rpc_server pointer
     */
    Connection(int fd, void* arg);
    ~Connection();

    static void Dispatch(std::shared_ptr<Connection> conn);

    static void ServerReadCb(struct bufferevent *bev, void *ctx);
    static void ClientReadCb(struct bufferevent *bev, void *ctx);
    static void ServerEventCb(struct bufferevent *bev, short events, void *ctx);
    static void ClientEventCb(struct bufferevent *bev, short events, void *ctx);

    RpcServer* GetRpcServer();
//    RpcChannel* GetRpcClient();

    /*
     * @brief After a connection is created, you can use this function to connect to
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

    // Get the readable length of the read buf
    int GetReadBufferLen();

    /*
     * Get the len data from read buf and then save them in the give buffer
     * If the data in read buf are less than len, get all of the data
     * Return the length of moved data
     * Note: the len data are deleted from read buf after this operation
     */
    int GetReadData(char *buffer, int len);

    /*
     * copy data (len) from read buf into the given buffer,
     * if the total data is less than len, then copy all of the data
     * return the length of the copied data
     * the data still exist in the read buf after this operation
     */
    int CopyReadBuffer(char *buffer, int len);

    // Get the lengh a write buf
    int GetWriteBufferLen();

    /*
     * Add data to write buff,
     * return true on success, false on failure.
     */
    bool AddToWriteBuffer(char *buffer, int len);

    // Forward data in read buf into write buf
    void MoveBufferData();

private:

    int socket_;
    bool close_;

    RpcServer* rpc_server_;

    bufferevent* bev_;
    event_base* base_;

    std::string method_name_;
};

}  // namespace networking
}  // namespace peloton
