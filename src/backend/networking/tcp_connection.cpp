//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tcp_connection.cpp
//
// Identification: /peloton/src/backend/networking/tcp_connection.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tcp_connection.h"
#include "peloton_service.h"

#include <mutex>

/*** this is for the test of rpc performance
std::mutex send_mutex;
uint64_t server_response_send_number = 0;  // number of rpc
uint64_t server_response_send_bytes = 0;  // bytes
*/

namespace peloton {
namespace networking {

Connection::Connection(int fd, event_base* base, void* arg) :
        socket_(fd), close_(false), status_(INIT), base_(base) {

    if (arg == NULL) {
        rpc_server_ = NULL;
    } else {
        rpc_server_ = (RpcServer*)arg;
    }

//    base_ = event_base_new(); // note: we share the base among different connections

    // BEV_OPT_THREADSAFE must be specified
    bev_ = bufferevent_socket_new(base_, socket_, BEV_OPT_CLOSE_ON_FREE|BEV_OPT_THREADSAFE);

    /* we can add callback function with output and input evbuffer*/
    //evbuffer_add_cb(bufferevent_get_output(bev_), BufferCb, tp);

    // client passes fd with -1 when new a connection
    if ( fd != -1) { // server
        bufferevent_setcb(bev_, ServerReadCb, NULL, ServerEventCb, this);
        LOG_TRACE("Server: connection init");
    } else { // client
        bufferevent_setcb(bev_, ClientReadCb, NULL, ClientEventCb, this);
        LOG_TRACE("Client: connection init");
    }

    bufferevent_enable(bev_, EV_READ | EV_WRITE);
}

Connection::~Connection() {

//    if (rpc_server_ == NULL) {
//        LOG_TRACE("Client: begin connection destroy");
//    } else {
//        LOG_TRACE("Server: begin connection destroy");
//    }

    // We must free event before free base
    this->Close();

    // After free event, base is finally freed
    event_base_free(base_);
}

// set the connection status
void Connection::SetStatus(ConnStatus status) {
    status_ = status;
}

// get the connection status
Connection::ConnStatus Connection::GetStatus() {
    return status_;
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

void Connection::Close() {

    if ( close_ == false ) {
        close_ = true;
        bufferevent_free(bev_);
    }
}


void Connection::SetMethodName(std::string name) {
    method_name_ = name;
}

const char* Connection::GetMethodName() {
    return method_name_.c_str();
}

//void Connection::Dispatch(std::shared_ptr<Connection> conn) {
//    event_base_dispatch(conn->base_);
//
//    if (conn->rpc_server_ == NULL) {
//        LOG_TRACE("Client: exit Dispatch");
//    } else {
//        LOG_TRACE("Server: exit Dispatch");
//    }
//}
void Connection::Dispatch(Connection* conn) {
    event_base_dispatch(conn->base_);

    if (conn->rpc_server_ == NULL) {
        LOG_TRACE("Client: exit Dispatch");
    } else {
        LOG_TRACE("Server: exit Dispatch");
    }
}
void Connection::ClientReadCb(__attribute__((unused)) struct bufferevent *bev,
                              void *ctx) {
    LOG_TRACE("ClientReadCb is invoked");
    assert (bev != NULL && ctx != NULL);

    // ***prepair the callback type message****//

    PelotonService service;

    Connection* conn = (Connection*) ctx;

    const std::string methodname = conn->GetMethodName();

    // use the protobuf lookup mechanism to locate the method
    const google::protobuf::DescriptorPool* dspool = google::protobuf::DescriptorPool::generated_pool();
    const google::protobuf::MethodDescriptor* mds = dspool->FindMethodByName(methodname);
    const google::protobuf::Message *response_type = &service.GetResponsePrototype(mds);

    google::protobuf::Message *response = response_type->New();

    // ***recv data and put the data into message****//

    while (conn->GetReadBufferLen()) {

        // Get the total readable data length
        uint32_t readable_len = conn->GetReadBufferLen();

        // If the total readable data is too less
        if (readable_len < HEADERLEN) {
            LOG_TRACE("ClientReadCb: Readable data is too less, return");
            return;
        }

        /*
         * Copy the header.
         * Note: should not remove the header from buffer, cuz we might return
         *       if there are no enough data as a whole message
         */
        uint32_t msg_len = 0;
        int nread = conn->CopyReadBuffer((char*) &msg_len, HEADERLEN);
        if(nread != HEADERLEN) {
          LOG_ERROR("nread does not match header length \n");
          return;
        }

        // if readable data is less than a message, return to wait the next callback
        if (readable_len < msg_len + HEADERLEN) {
            LOG_TRACE("ClientReadCb: Readable data is less than a message, return");
            return;
        }

        // Receive message.
        char buf[msg_len + HEADERLEN];

        // Get the data
        conn->GetReadData(buf, sizeof(buf));

        // Deserialize the receiving message
        response->ParseFromString(buf + HEADERLEN);

        // process data
        RpcController controller;
        service.CallMethod(mds, &controller, NULL, response, NULL);

        // TODO: controller should be set within rpc method
        if (controller.Failed()) {
            std::string error = controller.ErrorText();
            LOG_TRACE( "ClientReadCb:  with controller failed:%s ", error.c_str());
        }

    }

    delete response;

    conn->Close();
}

////////////////////////////////////////////////////////////////////////////////
//                 Response message structure:
// --Header:  message length of Response,                 uint32_t (4bytes)
// --Response: the serialization result of protobuf       ...
//
// TODO: We did not add checksum code in this version
////////////////////////////////////////////////////////////////////////////////

void Connection::ServerReadCb(__attribute__((unused)) struct bufferevent *bev,
                              void *ctx) {

    /* This callback is invoked when there is data to read on bev. */
    //struct evbuffer *input = bufferevent_get_input(bev);
    //struct evbuffer *output = bufferevent_get_output(bev);
    // TODO: We might use bev in futurn
    assert(bev != NULL);

    Connection* conn = (Connection*) ctx;

    // change the status of the connection
    conn->SetStatus(RECVING);

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
        if(nread != HEADERLEN) {
          LOG_ERROR("nread does not match header length \n");
          return;
        }

        // if readable data is less than a message, return to wait the next callback
        if (readable_len < msg_len + HEADERLEN) {
            LOG_TRACE("Readable data is less than a message, return");
            return;
        }

        // Receive message.
        char buf[msg_len + HEADERLEN];

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
        msg_len = response->ByteSize();

        char send_buf[sizeof(msg_len) + msg_len];
        assert(sizeof(msg_len) == HEADERLEN);

        // copy the header into the buf
        memcpy(send_buf, &msg_len, sizeof(msg_len));

        // call protobuf to serialize the request message into sending buf
        response->SerializeToArray(send_buf + sizeof(msg_len), msg_len);

        // send data
        // Note: if we use raw socket send api, we should loop send
        assert(sizeof(send_buf) == HEADERLEN+msg_len);
        conn->AddToWriteBuffer(send_buf, sizeof(send_buf));

        /* this is for the test of rpc performance
        // test
        {
            std::lock_guard < std::mutex > lock(send_mutex);
            server_response_send_number++;
            server_response_send_bytes += (msg_len + HEADERLEN);

            struct timeval end;
            long useconds;
            gettimeofday(&end, NULL);
            useconds = end.tv_usec - conn->GetRpcServer()->start_time_;

            float rpc_speed = 0;
            float bytes_speed = 0;

            rpc_speed = (float)(server_response_send_number * 1000000)/useconds;
            bytes_speed = (float)(server_response_send_bytes * 1000000)/useconds;

            std::cout << "server_response_send_number: "
                    << server_response_send_number << " speed:-------------------------------------------------"<< rpc_speed << "----------" <<std::endl;
            std::cout << "server_response_send_bytes: "
                    << server_response_send_bytes << " spped:***************************************************" << bytes_speed << "*********" << std::endl;
        }
        // end test
         */

        delete request;
        delete response;
    }
}

void Connection::ServerEventCb(__attribute__((unused)) struct bufferevent *bev, short events, void *ctx) {

    Connection* conn = (Connection*)ctx;
    assert(conn != NULL && bev != NULL);

    if (events & BEV_EVENT_ERROR) {

        LOG_TRACE("Error from server bufferevent: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        conn->Close();
    }

    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {

        // This means client explicitly closes the connection
        LOG_TRACE("ServerEventCb: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

        // free the buffer event
        conn->Close();
    }
}

void Connection::ClientEventCb(__attribute__((unused)) struct bufferevent *bev, short events, void *ctx) {

    Connection* conn = (Connection*)ctx;
    assert(conn != NULL && bev != NULL);

    if (events & BEV_EVENT_ERROR) {

        LOG_TRACE("Error from client bufferevent: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

        // tell the client send error, and the client should send again or do some other things
        if (conn->GetStatus() == SENDING) {
            LOG_TRACE("Send error");
            // TODO: let the client know
        }

        conn->Close();
    }

    // The other side explicitly closes the connection
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {

        // This means server explicitly closes the connection
        LOG_TRACE("ClientEventCb: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())); // the error string is "SUCCESS"

        // free the buffer event
        conn->Close();
    }
}

void Connection::BufferCb(struct evbuffer *buffer,
    const struct evbuffer_cb_info *info, void *arg) {

    std::cout<< arg << buffer<<  "This is BufferCb " <<
            "info->orig_size: "<< info->orig_size
            <<"info->n_deleted: " << info->n_deleted
            << "info->n_added: " << info->n_added
            << std::endl;

//    struct total_processed *tp = (struct total_processed *) arg;
//    size_t old_n = tp->n;
//    int megabytes, i;
//    total_send_ += info->n_deleted;
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
    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffersas well
     */
    bufferevent_lock(bev_);
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_get_length(input);
    bufferevent_unlock(bev_);
}

/*
 * Get the len data from read buf and then save them in the give buffer
 * If the data in read buf are less than len, get all of the data
 * Return the length of moved data
 * Note: the len data are deleted from read buf after this operation
 */
int Connection::GetReadData(char *buffer, int len) {
    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffersas well
     */
    bufferevent_lock(bev_);
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_remove(input, buffer, len);
    bufferevent_unlock(bev_);
}

/*
 * copy data (len) from read buf into the given buffer,
 * if the total data is less than len, then copy all of the data
 * return the length of the copied data
 * the data still exist in the read buf after this operation
 */
int Connection::CopyReadBuffer(char *buffer, int len) {
    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffersas well
     */
    bufferevent_lock(bev_);
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_copyout(input, buffer, len);
    bufferevent_unlock(bev_);
}

// Get the lengh a write buf
int Connection::GetWriteBufferLen() {
    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffersas well
     */
    bufferevent_lock(bev_);
    struct evbuffer *output = bufferevent_get_output(bev_);
    return evbuffer_get_length(output);
    bufferevent_unlock(bev_);
}

// Add data to write buff
bool Connection::AddToWriteBuffer(char *buffer, int len) {

    status_ = SENDING;

    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffersas well
     */
    bufferevent_lock(bev_);
    struct evbuffer *output = bufferevent_get_output(bev_);
    int re = evbuffer_add(output, buffer, len);
    bufferevent_unlock(bev_);

    if (re == 0) {
        return true;
    } else {
        return false;
    }
}

// put data in read buf into write buf
void Connection::MoveBufferData() {

    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffersas well
     */
    bufferevent_lock(bev_);

    struct evbuffer *input = bufferevent_get_input(bev_);
    struct evbuffer *output = bufferevent_get_output(bev_);

    /* Copy all the data from the input buffer to the output buffer. */
    evbuffer_add_buffer(output, input);

    bufferevent_unlock(bev_);
}

} // namespace networking
} // namespace peloton
