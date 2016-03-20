//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_network.cpp
//
// Identification: /peloton/src/backend/networking/tcp_network.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "tcp_listener.h"
#include "tcp_connection.h"
#include "rpc_type.h"
#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"
#include "backend/networking/connection_manager.h"

#include <event2/thread.h>
#include <pthread.h>

#include <functional>

namespace peloton {
namespace networking {

//void* ProcessMessage(void* arg) {
//
//    //assert(connection != NULL);
//
//    //Connection* conn = (Connection*)connection;
//
//    struct bufferevent *bev = (struct bufferevent *)arg;
//
///* This callback is invoked when there is data to read on bev. */
//struct evbuffer *input = bufferevent_get_input(bev);
//struct evbuffer *output = bufferevent_get_output(bev);
//
//
//
//
//        /*
//         * Get a message.
//         * Note: we only get one message each time. so the buf is msg_len + HEADERLEN
//         */
//        char buf[100 + HEADERLEN];
//
//        // Get the data
//        bufferevent_lock(bev);
//        evbuffer_remove(input, buf, sizeof(buf)-1 );
//        bufferevent_unlock(bev);
//        // Get the message type
//        uint16_t type = 0;
//        memcpy((char*) (&type), buf + HEADERLEN, sizeof(type));
//
//        // Get the hashcode of the rpc method
//        uint64_t opcode = 0;
//        memcpy((char*) (&opcode), buf + HEADERLEN + TYPELEN, sizeof(opcode));
//
//        // Get the rpc method meta info: method descriptor
////        RpcMethod *rpc_method = GetRpcServer()->FindMethod(opcode);
////
////        if (rpc_method == NULL) {
////            LOG_TRACE("No method found");
////            return NULL;
////        }
////
////        const google::protobuf::MethodDescriptor *method = rpc_method->method_;
////        RpcController controller;
//
//        switch ( type ) {
//
//          case MSG_TYPE_REQ: {
//            LOG_TRACE("Handle MSG_TYPE: Request");
//
////            // Get request and response type and create them
////            google::protobuf::Message *message = rpc_method->request_->New();
////            google::protobuf::Message *response = rpc_method->response_->New();
////
////            // Deserialize the receiving message
////            message->ParseFromString(buf + HEADERLEN + TYPELEN + OPCODELEN);
////
////            //Invoke rpc call.
////            rpc_method->service_->CallMethod(method, &controller, message, response,
////                    NULL);
////
////            // Send back the response message. The message has been set up when executing rpc method
////            uint64_t msg_len = response->ByteSize() + OPCODELEN + TYPELEN;
////
////            char send_buf[sizeof(msg_len) + msg_len];
////            assert(sizeof(msg_len) == HEADERLEN);
////
////            // copy the header into the buf
////            memcpy(send_buf, &msg_len, sizeof(msg_len));
////
////            // copy the type into the buf
////            type = MSG_TYPE_REP;
////
////            assert(sizeof(type) == TYPELEN);
////            memcpy(send_buf + HEADERLEN, &type, TYPELEN);
////
////            // copy the opcode into the buf
////            assert(sizeof(opcode) == OPCODELEN);
////            memcpy(send_buf + HEADERLEN + TYPELEN, &opcode, OPCODELEN);
////
////            // call protobuf to serialize the request message into sending buf
////            response->SerializeToArray(send_buf + HEADERLEN + TYPELEN + OPCODELEN, msg_len);
////
////            // send data
////            // Note: if we use raw socket send api, we should loop send
////            assert(sizeof(send_buf) == HEADERLEN+msg_len);
////            //conn->AddToWriteBuffer(send_buf, sizeof(send_buf));
//            bufferevent_lock(bev);
//            //bufferevent_write(bev, "aaaa", strlen("aaaa"));
//            evbuffer_add(output, "aaaa", strlen("aaaa"));
//            bufferevent_unlock(bev);
//
////            delete message;
////            delete response;
//
//            std::cout << "return after adding data" << std::endl;
//          } break;
//
//          case MSG_TYPE_REP: {
//            LOG_TRACE("Handle MSG_TYPE: Response");
//
////            // Get response and response type and create them
////            google::protobuf::Message *message = rpc_method->response_->New();
////
////            // Deserialize the receiving message
////            message->ParseFromString(buf + HEADERLEN + TYPELEN + OPCODELEN);
////
////            // Invoke rpc call. request is null
////            rpc_method->service_->CallMethod(method, &controller, NULL, message,
////                    NULL);
////
////            delete message;
//
//          } break;
//
//          default:
//            LOG_ERROR("Unrecognized message type %d", type);
//            break;
//        }
//
//        // TODO: controller should be set within rpc method
////        if (controller.Failed()) {
////            std::string error = controller.ErrorText();
////            LOG_TRACE( "RpcServer with controller failed:%s ", error.c_str());
////        }
//
//        /* this is for the test of rpc performance
//        // test
//        {
//            std::lock_guard < std::mutex > lock(send_mutex);
//            server_response_send_number++;
//            server_response_send_bytes += (msg_len + HEADERLEN);
//
//            struct timeval end;
//            long useconds;
//            gettimeofday(&end, NULL);
//            useconds = end.tv_usec - conn->GetRpcServer()->start_time_;
//
//            float rpc_speed = 0;
//            float bytes_speed = 0;
//
//            rpc_speed = (float)(server_response_send_number * 1000000)/useconds;
//            bytes_speed = (float)(server_response_send_bytes * 1000000)/useconds;
//
//            std::cout << "server_response_send_number: "
//                    << server_response_send_number << " speed:-------------------------------------------------"<< rpc_speed << "----------" <<std::endl;
//            std::cout << "server_response_send_bytes: "
//                    << server_response_send_bytes << " spped:***************************************************" << bytes_speed << "*********" << std::endl;
//        }
//        // end test
//         */
//
//
//    return NULL;
//}
//
//void ReadCb(struct bufferevent *bev,
//                              void *ctx) {
//
//    /* This callback is invoked when there is data to read on bev. */
//    //struct evbuffer *input = bufferevent_get_input(bev);
//    //struct evbuffer *output = bufferevent_get_output(bev);
//    // TODO: We might use bev in future
//    assert(ctx != NULL);
//
//    //Connection* conn = (Connection*) ctx;
//
//    /*
//     * Process the message will invoke rpc call.
//     * Note: this might take a long time. So we put this in a thread
//     */
//
//    // prepaere workers_thread to send and recv data
//    //std::function<void()> worker_conn =
//    //        std::bind(&Connection::ProcessMessage, conn);
//
//    /*
//     * Add workers to thread pool to send and recv data
//     * Note: after AddTask, ReadCb will return and another
//     * request on this connection can be processed while
//     * the former is still being processed
//     */
//    //ThreadPool::GetServerThreadPool().AddTask(worker_conn);
//    pthread_t testThread;
//    int ret = pthread_create(&testThread, NULL, ProcessMessage, (void*)bev);
//        if( -1 == ret ) { printf( "create thread error\n" ); }
//
//    //ProcessMessage(conn);
//}
//
//void EventCb(__attribute__((unused)) struct bufferevent *bev, short events, void *ctx) {
//
//    Connection* conn = (Connection*)ctx;
//    assert(conn != NULL && bev != NULL);
//
//    if (events & BEV_EVENT_ERROR) {
//
//        LOG_TRACE("Error from client bufferevent: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
//
//        conn->Close();
//    }
//
//    // The other side explicitly closes the connection
//    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
//
//        // This means server explicitly closes the connection
//        LOG_TRACE("ClientEventCb: %s",evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())); // the error string is "SUCCESS"
//
//        // free the buffer event
//        conn->Close();
//    }
//}

Listener::Listener(int port) :
    port_(port),
    listen_base_(event_base_new()),
    listener_(NULL) {

    // make libevent support multiple threads (pthread)
    // TODO: put this before event_base_new()?

    assert(listen_base_ != NULL);
    assert(port_ > 0 && port_ < 65535);
}

Listener::~Listener() {

    if (listener_ != NULL) {
        evconnlistener_free(listener_);
    }

    if (listen_base_ != NULL) {
        event_base_free(listen_base_);
    }
}

/*
 * @breif Run is invoked by server. It uses libevent to listen
 *        When a new connection is accepted, AcceptConnCb is invoked
 *        to process the new connection
 * @pram  arg is the rpc_server pointer
 */
void Listener::Run(void* arg) {

    struct sockaddr_in sin;

    /* Clear the sockaddr before using it, in case there are extra
     *          * platform-specific fields that can mess us up. */
    memset(&sin, 0, sizeof(sin));

    /* This is an INET address */
    sin.sin_family = AF_INET;
    /* Listen on 0.0.0.0 */
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    /* Listen on the given port. */
    sin.sin_port = htons(port_);

    evthread_use_pthreads();

    // TODO: LEV_OPT_THREADSAFE is necessary here?
    listener_ = evconnlistener_new_bind(listen_base_, AcceptConnCb, arg,
        LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE|LEV_OPT_THREADSAFE, -1,
        (struct sockaddr*)&sin, sizeof(sin));

    if (!listener_) {
      LOG_ERROR("Couldn't create listener");
      return;
    }

    evconnlistener_set_error_cb(listener_, AcceptErrorCb);

    event_base_dispatch(listen_base_);

    evconnlistener_free(listener_);
    event_base_free(listen_base_);
    LOG_TRACE("Serving is done");
    return;
}

/*
 * @breif AcceptConnCb processes the new connection.
 *        First it new a connection with the passing by socket and ctx
 *        where ctx is passed by Run which is rpc_server pointer
 */
void Listener::AcceptConnCb(struct evconnlistener *listener,
                            evutil_socket_t fd,
                            struct sockaddr *address,
                            __attribute__((unused)) int socklen,
                            void *ctx) {

    assert(listener != NULL && address != NULL && socklen >= 0 && ctx != NULL);

    LOG_INFO ("Server: connection received");

    /* We got a new connection! Set up a bufferevent for it. */
    struct event_base *base = evconnlistener_get_base(listener);

    NetworkAddress addr(*address);

    /* Each connection has a bufferevent which is use to recv and send data*/
    Connection* conn = new Connection(fd, base, ctx, addr);

    /* The connection is added in the conn pool, which can be used in the future*/
    ConnectionManager::GetInstance().AddConn(*address, conn);

    LOG_INFO ("Server: connection received from fd: %d, address: %s, port:%d",
            fd, addr.IpToString().c_str(), addr.GetPort());
}

void Listener::AcceptErrorCb(struct evconnlistener *listener,
                             __attribute__((unused)) void *ctx) {

    assert(ctx != NULL);

    struct event_base *base = evconnlistener_get_base(listener);

    // Debug info
    LOG_ERROR("Got an error %d (%s) on the listener. "
            "Shutting down", EVUTIL_SOCKET_ERROR(),
            evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));

    event_base_loopexit(base, NULL);
}

}  // namespace networking
}  // namespace peloton
