//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_client_manager.h
//
// Identification: /peloton/src/backend/message/rpc_client_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once


class RpcClientManager {

public:

private:

    static void Loop
    static void Callback(PelotonClient* client,
            google::protobuf::Closure* callback);
    static void HearbeatCallback(PelotonClient* client) {
        LOG_TRACE("This is client Hearbeat callback: socket: %s",
                (char *) client);
        HearbeatResponse response;
        client->GetResponse(&response);

        // process heartbeat response

        if (client != nullptr) {
            delete client;
        }
    }

    RpcChannel* channel_;

    //TODO: controller might be moved out if needed
    RpcController* controller_;
    AbstractPelotonService::Stub* stub_;

    static pollfd poll_sockets_;static map(socket, callback);

};
