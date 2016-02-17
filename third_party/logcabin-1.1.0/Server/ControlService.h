/* Copyright (c) 2015 Diego Ongaro
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "RPC/Service.h"

#ifndef LOGCABIN_SERVER_CONTROLSERVICE_H
#define LOGCABIN_SERVER_CONTROLSERVICE_H

namespace LogCabin {

// forward declaration
namespace Protocol {
namespace ServerControl {
class Settings;
} // LogCabin::Protocol::ServerControl
} // LogCabin::Protocol


namespace Server {

// forward declaration
class Globals;

/**
 * Invoked by logcabinctl client to inspect and manipulate internal server
 * state.
 */
class ControlService : public RPC::Service {
  public:
    /// Constructor.
    explicit ControlService(Globals& globals);

    /// Destructor.
    ~ControlService();

    void handleRPC(RPC::ServerRPC rpc);
    std::string getName() const;

  private:

    ////////// RPC handlers //////////

    void debugFilenameGet(RPC::ServerRPC rpc);
    void debugFilenameSet(RPC::ServerRPC rpc);
    void debugPolicyGet(RPC::ServerRPC rpc);
    void debugPolicySet(RPC::ServerRPC rpc);
    void debugRotate(RPC::ServerRPC rpc);
    void serverInfoGet(RPC::ServerRPC rpc);
    void serverStatsDump(RPC::ServerRPC rpc);
    void serverStatsGet(RPC::ServerRPC rpc);
    void snapshotControl(RPC::ServerRPC rpc);
    void snapshotInhibitGet(RPC::ServerRPC rpc);
    void snapshotInhibitSet(RPC::ServerRPC rpc);

    /**
     * The LogCabin daemon's top-level objects.
     */
    Globals& globals;

  public:

    // ControlService is non-copyable.
    ControlService(const ControlService&) = delete;
    ControlService& operator=(const ControlService&) = delete;
}; // class ControlService

} // namespace LogCabin::Server
} // namespace LogCabin

#endif /* LOGCABIN_SERVER_CONTROLSERVICE_H */
