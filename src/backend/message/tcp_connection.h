//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_connection.h
//
// Identification: /peloton/src/backend/message/tcp_connection.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/logger.h"

#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event.h>

namespace peloton {
namespace message {

class Connection {

public:

    Connection(int fd) :
        socket_(fd) {
        base_ = event_base_new();
        bev_ = bufferevent_socket_new(
                    base_, socket_, BEV_OPT_CLOSE_ON_FREE);

        bufferevent_setcb(bev_, ReadCb, NULL, EventCb, this);

        bufferevent_enable(bev_, EV_READ|EV_WRITE);
    }

    static void Dispatch(Connection* conn) {
        event_base_dispatch(conn->base_);
    }

    static void ReadCb(struct bufferevent *bev, void *ctx) {

            /* This callback is invoked when there is data to read on bev. */
            struct evbuffer *input = bufferevent_get_input(bev);
            struct evbuffer *output = bufferevent_get_output(bev);

            /* Copy all the data from the input buffer to the output buffer. */
            evbuffer_add_buffer(output, input);
    }

    static void EventCb(struct bufferevent *bev, short events, void *ctx) {

            if (events & BEV_EVENT_ERROR)
                    LOG_TRACE("Error from bufferevent");
            if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
                    bufferevent_free(bev);
            }
    }

    ~Connection() {

        if (base_ != NULL) {
            event_base_free(connection_base_);
        }
    }

    // Get the readable length of the read buf
    int GetReadBufferLen() {
        struct evbuffer *input = bufferevent_get_input(bev_);
        return evbuffer_get_length(input);
    }

    /*
     * Get the len data from read buf and then save them in the give buffer
     * If the data in read buf are less than len, get all of the data
     * Return the length of moved data
     * Note: the len data are deleted from read buf after this operation
     */
    int GetReadData(char *buffer, int len) {
        struct evbuffer *input = bufferevent_get_input(bev_);
        return evbuffer_remove(input, buffer, len);
    }

    /*
     * copy data (len) from read buf into the given buffer,
     * if the total data is less than len, then copy all of the data
     * return the length of the copied data
     * the data still exist in the read buf after this operation
     */
    int CopyReadBuffer(char *buffer, int len) {
        struct evbuffer *input = bufferevent_get_input(bev_);
        return evbuffer_copyout(input, buffer, len);
    }

    // Get the lengh a write buf
    int GetWriteBufferLen() {
        struct evbuffer *output = bufferevent_get_output(bev_);
        return evbuffer_get_length(output);
    }

    // Add data to write buff
    int AddToWriteBuffer(char *buffer, int len) {
        struct evbuffer *output = bufferevent_get_output(bev_);
        return evbuffer_add(output, buffer, len);
    }

    // put data in read buf into write buf
    void MoveBufferData() {
        struct evbuffer *input = bufferevent_get_input(bev_);
        struct evbuffer *output = bufferevent_get_output(bev_);

        /* Copy all the data from the input buffer to the output buffer. */
        evbuffer_add_buffer(output, input);
    }

private:

    int socket_;
    bufferevent bev_;
    event_base* base_;
};

}  // namespace message
}  // namespace peloton
