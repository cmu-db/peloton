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
#include "rpc_server.h"
#include "rpc_controller.h"

#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event.h>

namespace peloton {
namespace message {

#include "rpc_controller.h"

#define MAXBYTES 1024

class Connection {

public:

    Connection(int fd, void* arg) :
        socket_(fd),
        client_server_ (arg) {

        base_ = event_base_new();
        bev_ = bufferevent_socket_new(
                    base_, socket_, BEV_OPT_CLOSE_ON_FREE);

        bufferevent_setcb(bev_, ReadCb, NULL, EventCb, this);
        bufferevent_enable(bev_, EV_READ|EV_WRITE);
    }

    ~Connection() {

        if (base_ != NULL) {
            event_base_free(base_);
        }

        if (bev_ != NULL) {
            bufferevent_free(bev_);
        }
    }

    static void Dispatch(std::shared_ptr<Connection> conn) {
        event_base_dispatch(conn->base_);
    }

    static void ReadCb(struct bufferevent *bev, void *ctx) {

        /* This callback is invoked when there is data to read on bev. */
        //struct evbuffer *input = bufferevent_get_input(bev);
        //struct evbuffer *output = bufferevent_get_output(bev);
        // TODO: We might use bev in futurn
        assert(bev != NULL);

        Connection* conn = (Connection*) ctx;

        uint64_t opcode = 0;

        // Receive message
        char buf[MAXBYTES];

        while (conn->GetReadBufferLen()) {

            // Get the data
            conn->GetReadData(buf, sizeof(buf));

            // Get the hashcode of the rpc method
            memcpy((char*) (&opcode), buf, sizeof(opcode));

            // Get the rpc method meta info: method descriptor
            RpcMethod *rpc_method = conn->GetRpcServer()->FindMethod(opcode);

            if (rpc_method == NULL) {
                LOG_TRACE("No method found");
                return;
            }

            const google::protobuf::MethodDescriptor *method = rpc_method->method_;

            // Get request and response type and create them
            google::protobuf::Message *request = rpc_method->request_->New();
            google::protobuf::Message *response = rpc_method->response_->New();

            // Deserialize the receiving message
            request->ParseFromString(buf + sizeof(opcode));

            // Create the corresponding rpc method
            // google::protobuf::Closure* callback =
            //         google::protobuf::internal::NewCallback(&Callback);

             RpcController controller;
             rpc_method->service_->CallMethod(method, &controller, request, response,
                     NULL);

             // TODO: controller should be set within rpc method
             if (controller.Failed()) {
                 std::string error = controller.ErrorText();
                 LOG_TRACE( "RpcServer with controller failed:%s ", error.c_str());
             }

             // Send back the response message. The message has been set up when executing rpc method
             size_t msg_len = response->ByteSize();

             char buffer[msg_len];
             response->SerializeToArray(buf, msg_len);

             // send data
             conn->AddToWriteBuffer(buffer, msg_len);

             delete request;
             delete response;
        }
    }

    static void EventCb(struct bufferevent *bev, short events, void *ctx) {

        if (events & BEV_EVENT_ERROR)
            LOG_TRACE("Error from bufferevent");
        if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
            bufferevent_free(bev);
        }

        // TODO: by michael
        std::cout << ctx;
    }

    RpcServer* GetRpcServer() {
        return (RpcServer*)client_server_;
    }

    RpcClient* GetRpcServer() {
        return (RpcClient*)client_server_;
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
    void* client_server_;
    bufferevent* bev_;
    event_base* base_;
};

}  // namespace message
}  // namespace peloton
