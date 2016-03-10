//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tcp_connection.cpp
//
// Identification: /peloton/src/backend/message/tcp_connection.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tcp_connection.h"

namespace peloton {
namespace message {

Connection::Connection(int fd, void* arg) :
        socket_(fd), client_server_(arg) {

    base_ = event_base_new();
    bev_ = bufferevent_socket_new(base_, socket_, BEV_OPT_CLOSE_ON_FREE);

    // client passes fd with -1 when new a connection
    if ( fd != -1) { // server
        bufferevent_setcb(bev_, ServerReadCb, NULL, EventCb, this);
    } else { // client
        bufferevent_setcb(bev_, ClientReadCb, NULL, EventCb, this);
    }

    bufferevent_enable(bev_, EV_READ | EV_WRITE);
}

Connection::~Connection() {

    if (base_ != NULL) {
        event_base_free(base_);
    }

    if (bev_ != NULL) {
        bufferevent_free(bev_);
    }
}

bool Connection::Connect(const NetworkAddress& addr) {

    struct sockaddr_in sin;
    addr.FillAddr(&sin);

    if (bufferevent_socket_connect(bev_, (struct sockaddr *) &sin, sizeof(sin))
            < 0) {
        /* Error starting connection */
        //bufferevent_free (bev_);
        return false;
    }

    return true;
}


void Connection::Dispatch(std::shared_ptr<Connection> conn) {
    event_base_dispatch(conn->base_);
}

void Connection::ClientReadCb(struct bufferevent *bev, void *ctx) {
    std::cout << bev << ctx;
}

void Connection::ServerReadCb(struct bufferevent *bev, void *ctx) {

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

void Connection::EventCb(struct bufferevent *bev, short events, void *ctx) {

    if (events & BEV_EVENT_ERROR)
        LOG_TRACE("Error from bufferevent");
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
        bufferevent_free(bev);
    }

    // TODO: by michael
    std::cout << ctx;
}

RpcServer* Connection::GetRpcServer() {
    return (RpcServer*) client_server_;
}

RpcChannel* Connection::GetRpcClient() {
    return (RpcChannel*) client_server_;
}

// Get the readable length of the read buf
int Connection::GetReadBufferLen() {
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_get_length(input);
}

/*
 * Get the len data from read buf and then save them in the give buffer
 * If the data in read buf are less than len, get all of the data
 * Return the length of moved data
 * Note: the len data are deleted from read buf after this operation
 */
int Connection::GetReadData(char *buffer, int len) {
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_remove(input, buffer, len);
}

/*
 * copy data (len) from read buf into the given buffer,
 * if the total data is less than len, then copy all of the data
 * return the length of the copied data
 * the data still exist in the read buf after this operation
 */
int Connection::CopyReadBuffer(char *buffer, int len) {
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_copyout(input, buffer, len);
}

// Get the lengh a write buf
int Connection::GetWriteBufferLen() {
    struct evbuffer *output = bufferevent_get_output(bev_);
    return evbuffer_get_length(output);
}

// Add data to write buff
bool Connection::AddToWriteBuffer(char *buffer, int len) {

    struct evbuffer *output = bufferevent_get_output(bev_);

    int re = evbuffer_add(output, buffer, len);

    if (re == 0) {
        return true;
    } else {
        return false;
    }
}

// put data in read buf into write buf
void Connection::MoveBufferData() {
    struct evbuffer *input = bufferevent_get_input(bev_);
    struct evbuffer *output = bufferevent_get_output(bev_);

    /* Copy all the data from the input buffer to the output buffer. */
    evbuffer_add_buffer(output, input);
}

} // namespace message
} // namespace peloton
