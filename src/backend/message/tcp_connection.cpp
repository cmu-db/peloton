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
        socket_(fd) {

    if (arg == NULL) {
        rpc_server_ = NULL;
    } else {
        rpc_server_ = (RpcServer*)arg;
    }

    base_ = event_base_new();
    bev_ = bufferevent_socket_new(base_, socket_, BEV_OPT_CLOSE_ON_FREE);

    // client passes fd with -1 when new a connection
    if ( fd != -1) { // server
        bufferevent_setcb(bev_, ServerReadCb, NULL, ServerEventCb, this);
    } else { // client
        bufferevent_setcb(bev_, ClientReadCb, NULL, ClientEventCb, this);
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


void Connection::SetMethodName(std::string name) {
    method_name_ = name;
}

const char* Connection::GetMethodName() {
    return method_name_.c_str();
}

void Connection::Dispatch(std::shared_ptr<Connection> conn) {
    event_base_dispatch(conn->base_);
}

void Connection::ClientReadCb(struct bufferevent *bev, void *ctx) {
    LOG_TRACE("ClientReadCb is invoked");
    assert (bev != NULL && ctx != NULL);

    peloton::message::PelotonService service;

    Connection* conn = (Connection*) ctx;

    const std::string methodname = conn->GetMethodName();

    const google::protobuf::DescriptorPool* dspool = google::protobuf::DescriptorPool::generated_pool();

    const google::protobuf::MethodDescriptor* mds = dspool->FindMethodByName(methodname);

    const google::protobuf::Message response = service.GetResponsePrototype(method);

}

void Connection::ServerReadCb(struct bufferevent *bev, void *ctx) {

    /* This callback is invoked when there is data to read on bev. */
    //struct evbuffer *input = bufferevent_get_input(bev);
    //struct evbuffer *output = bufferevent_get_output(bev);
    // TODO: We might use bev in futurn
    assert(bev != NULL);

    Connection* conn = (Connection*) ctx;

    while (conn->GetReadBufferLen()) {

        // Get the total readable data length
        uint32_t readable_len = conn->GetReadBufferLen();

        // If the total readable data is too less
        if (readable_len < HEADERLEN + OPCODELEN) {
            LOG_TRACE("Readable data is too less, return");
            return;
        }

        /*
         * Copy the header.
         * Note: should not remove the header from buffer, cuz we might return
         *       if there are no enough data as a whole message
         */
        uint32_t msg_len = 0;
        int nread = conn->CopyReadBuffer((char*) &msg_len, HEADERLEN);
        assert(nread == HEADERLEN);

        // if readable data is less than a message, return to wait the next callback
        if (readable_len < msg_len + HEADERLEN) {
            LOG_TRACE("Readable data is less than a message, return");
            return;
        }

        // Receive message.
        char buf[msg_len + HEADERLEN];
        LOG_TRACE("Begin to read a request, length is %d", msg_len);

        // Get the data
        conn->GetReadData(buf, sizeof(buf));

        // Get the hashcode of the rpc method
        uint64_t opcode = 0;
        memcpy((char*) (&opcode), buf + HEADERLEN, sizeof(opcode));

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
        request->ParseFromString(buf + sizeof(opcode) + HEADERLEN);

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
        //size_t msg_len = response->ByteSize();

        //char buffer[msg_len];
        //response->SerializeToArray(buf, msg_len);

        // send data
        //conn->AddToWriteBuffer(buffer, msg_len);

        /* Copy all the data from the input buffer to the output buffer. */
        //conn->MoveBufferData();
        conn->AddToWriteBuffer(buf, sizeof(buf));

        delete request;
        delete response;
    }
}

void Connection::ServerEventCb(struct bufferevent *bev, short events, void *ctx) {

    if (events & BEV_EVENT_ERROR) {
        LOG_TRACE("Error from server bufferevent: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    }
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
        bufferevent_free(bev);
    }

    // TODO
    Connection* conn = (Connection*)ctx;
    assert(conn != NULL);
}

void Connection::ClientEventCb(struct bufferevent *bev, short events, void *ctx) {

    if (events & BEV_EVENT_ERROR) {
        LOG_TRACE("Error from client bufferevent: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    }
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
        bufferevent_free(bev);
    }

    // TODO
    Connection* conn = (Connection*)ctx;
    assert(conn != NULL);
}

RpcServer* Connection::GetRpcServer() {
    return rpc_server_;
}
//
//RpcChannel* Connection::GetRpcClient() {
//    return (RpcChannel*) client_server_;
//}

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
