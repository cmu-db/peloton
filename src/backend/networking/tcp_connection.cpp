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
#include "rpc_type.h"

#include <pthread.h>

#include <iostream>
#include <mutex>

/*** this is for the test of rpc performance
std::mutex send_mutex;
uint64_t server_response_send_number = 0;  // number of rpc
uint64_t server_response_send_bytes = 0;  // bytes
*/

struct total_processed {
    size_t n;
};

namespace peloton {
namespace networking {

Connection::Connection(int fd, struct event_base* base, void* arg, NetworkAddress& addr) :
      addr_(addr), close_(false), status_(INIT), base_(base) {

    // we must pass rpc_server when new a connection
    assert( arg != NULL );
    rpc_server_ = (RpcServer*)arg;

//    base_ = event_base_new(); // note: we share the base among different connections

    // BEV_OPT_THREADSAFE must be specified
    bev_ = bufferevent_socket_new(base_, fd, BEV_OPT_CLOSE_ON_FREE|BEV_OPT_THREADSAFE);

    int ret = bufferevent_enable(bev_, BEV_OPT_THREADSAFE );
    if( ret < 0 ) { printf( "----------------------------\n" ); }

    struct total_processed *tp = (total_processed *)malloc(sizeof(*tp));
    tp->n = 0;
    /* we can add callback function with output and input evbuffer*/
    evbuffer_add_cb(bufferevent_get_output(bev_), BufferCb, tp);

    // set read callback and event callback
    bufferevent_setcb(bev_, ReadCb, NULL, EventCb, this);

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
//   event_base_free(base_);
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

////////////////////////////////////////////////////////////////////////////////
//                  message structure:
// --Header:  message length (Type+Opcode+request),      uint32_t (4bytes)
// --Type:    message type: REQUEST or RESPONSE          uint16_t (2bytes)
// --Opcode:  std::hash(methodname)-->Opcode,            uint64_t (8bytes)
// --Content: the serialization result of protobuf       Header-8-2
//
// TODO: We did not add checksum code in this version

void* Connection::ProcessMessage(void* connection) {

    assert(connection != NULL);

    Connection* conn = (Connection*)connection;

    while (conn->GetReadBufferLen()) {

        // Get the total readable data length
        uint32_t readable_len = conn->GetReadBufferLen();

        // If the total readable data is too less
        if (readable_len < HEADERLEN + OPCODELEN + TYPELEN) {
            LOG_TRACE("Readable data is too less, return");
            return NULL;
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
          return NULL;
        }

        /*
         * if readable data is less than a message, return to wait the next callback
         * Note: msg_len includes the length of type + opcode + message
         */
        if (readable_len < msg_len + HEADERLEN) {
            LOG_TRACE("Readable data is less than a message, return");
            return NULL;
        }

        /*
         * Get a message.
         * Note: we only get one message each time. so the buf is msg_len + HEADERLEN
         */
        char buf[msg_len + HEADERLEN];

        // Get the data
        conn->GetReadData(buf, sizeof(buf));

        // Get the message type
        uint16_t type = 0;
        memcpy((char*) (&type), buf + HEADERLEN, sizeof(type));

        // Get the hashcode of the rpc method
        uint64_t opcode = 0;
        memcpy((char*) (&opcode), buf + HEADERLEN + TYPELEN, sizeof(opcode));

        // Get the rpc method meta info: method descriptor
        RpcMethod *rpc_method = conn->GetRpcServer()->FindMethod(opcode);

        if (rpc_method == NULL) {
            LOG_TRACE("No method found");
            return NULL;
        }

        const google::protobuf::MethodDescriptor *method = rpc_method->method_;
        RpcController controller;

        switch ( type ) {

          case MSG_TYPE_REQ: {
            LOG_TRACE("Handle MSG_TYPE: Request");

            // Get request and response type and create them
            google::protobuf::Message *message = rpc_method->request_->New();
            google::protobuf::Message *response = rpc_method->response_->New();

            // Deserialize the receiving message
            message->ParseFromString(buf + HEADERLEN + TYPELEN + OPCODELEN);

            //Invoke rpc call.
            rpc_method->service_->CallMethod(method, &controller, message, response,
                    NULL);

            // Send back the response message. The message has been set up when executing rpc method
            msg_len = response->ByteSize() + OPCODELEN + TYPELEN;

            char send_buf[sizeof(msg_len) + msg_len];
            assert(sizeof(msg_len) == HEADERLEN);

            // copy the header into the buf
            memcpy(send_buf, &msg_len, sizeof(msg_len));

            // copy the type into the buf
            type = MSG_TYPE_REP;

            assert(sizeof(type) == TYPELEN);
            memcpy(send_buf + HEADERLEN, &type, TYPELEN);

            // copy the opcode into the buf
            assert(sizeof(opcode) == OPCODELEN);
            memcpy(send_buf + HEADERLEN + TYPELEN, &opcode, OPCODELEN);

            // call protobuf to serialize the request message into sending buf
            response->SerializeToArray(send_buf + HEADERLEN + TYPELEN + OPCODELEN, msg_len);

            // send data
            // Note: if we use raw socket send api, we should loop send
            assert(sizeof(send_buf) == HEADERLEN+msg_len);
            conn->AddToWriteBuffer(send_buf, sizeof(send_buf));

            delete message;
            delete response;

          } break;

          case MSG_TYPE_REP: {
            LOG_TRACE("Handle MSG_TYPE: Response");

            // Get response and response type and create them
            google::protobuf::Message *message = rpc_method->response_->New();

            // Deserialize the receiving message
            message->ParseFromString(buf + HEADERLEN + TYPELEN + OPCODELEN);

            // Invoke rpc call. request is null
            rpc_method->service_->CallMethod(method, &controller, NULL, message,
                    NULL);

            delete message;

          } break;

          default:
            LOG_ERROR("Unrecognized message type %d", type);
            break;
        }

        // TODO: controller should be set within rpc method
        if (controller.Failed()) {
            std::string error = controller.ErrorText();
            LOG_TRACE( "RpcServer with controller failed:%s ", error.c_str());
        }

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
    }

    std::cout << "return after adding data" << std::endl;
    return NULL;
}

void Connection::ReadCb(__attribute__((unused)) struct bufferevent *bev,
                              void *ctx) {

    /* This callback is invoked when there is data to read on bev. */
    //struct evbuffer *input = bufferevent_get_input(bev);
    //struct evbuffer *output = bufferevent_get_output(bev);
    // TODO: We might use bev in future
    assert(bev != NULL);

    Connection* conn = (Connection*) ctx;

    // change the status of the connection
    //conn->SetStatus(RECVING);

    /*
     * Process the message will invoke rpc call.
     * Note: this might take a long time. So we put this in a thread
     */

    // prepaere workers_thread to send and recv data
    std::function<void()> worker_conn =
            std::bind(&Connection::ProcessMessage, conn);

    /*
     * Add workers to thread pool to send and recv data
     * Note: after AddTask, ReadCb will return and another
     * request on this connection can be processed while
     * the former is still being processed
     */
    ThreadPool::GetServerThreadPool().AddTask(worker_conn);
}

void Connection::EventCb(__attribute__((unused)) struct bufferevent *bev, short events, void *ctx) {

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

    struct total_processed *tp = (total_processed *)arg;
    size_t old_n = tp->n;
    int megabytes, i;
    tp->n += info->n_deleted;
    megabytes = ((tp->n) >> 20) - (old_n >> 20);
    for (i=0; i<megabytes; ++i)
        putc('.', stdout);
}

RpcServer* Connection::GetRpcServer() {
    return rpc_server_;
}

// Get the readable length of the read buf
int Connection::GetReadBufferLen() {
    /*
     * Locking the bufferevent with this function will
     * lock its associated evbuffers as well
     * Note: it is automatically locked so we don't need
     *       to explicitly lock it
     *       bufferevent_lock(bev_);
     *       bufferevent_unlock(bev_);
     */
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
    /*
     * Note: it is automatically locked so we don't need
     *       to explicitly lock it
     *       bufferevent_lock(bev_);
     *       bufferevent_unlock(bev_);
     */
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
    /*
     * Note: it is automatically locked so we don't need
     *       to explicitly lock it
     *       bufferevent_lock(bev_);
     *       bufferevent_unlock(bev_);
     */
    struct evbuffer *input = bufferevent_get_input(bev_);
    return evbuffer_copyout(input, buffer, len);
}

// Get the lengh a write buf
int Connection::GetWriteBufferLen() {
    /*
     * Note: it is automatically locked so we don't need
     *       to explicitly lock it
     *       bufferevent_lock(bev_);
     *       bufferevent_unlock(bev_);
     */
    struct evbuffer *output = bufferevent_get_output(bev_);
    return evbuffer_get_length(output);
}

// Add data to write buff
bool Connection::AddToWriteBuffer(char *buffer, int len) {

    //status_ = SENDING;

    /*
     * Note: it is automatically locked so we don't need
     *       to explicitly lock it
     *       bufferevent_lock(bev_);
     *       bufferevent_unlock(bev_);
     */
//    bufferevent_lock(bev_);
//    struct evbuffer *output = bufferevent_get_output(bev_);
//    int re = evbuffer_add(output, buffer, len);
//    bufferevent_unlock(bev_);

    int re = bufferevent_write(bev_, buffer, len);

    if (re == 0) {
        return true;
    } else {
        return false;
    }
}

// put data in read buf into write buf
void Connection::MoveBufferData() {

    /*
     * Note: it is automatically locked so we don't need
     *       to explicitly lock it
     *       bufferevent_lock(bev_);
     *       bufferevent_unlock(bev_);
     */
    struct evbuffer *input = bufferevent_get_input(bev_);
    struct evbuffer *output = bufferevent_get_output(bev_);

    /* Copy all the data from the input buffer to the output buffer. */
    evbuffer_add_buffer(output, input);
}

} // namespace networking
} // namespace peloton
