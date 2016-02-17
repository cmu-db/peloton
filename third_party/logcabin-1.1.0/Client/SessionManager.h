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

#ifndef LOGCABIN_CLIENT_SESSIONMANAGER_H
#define LOGCABIN_CLIENT_SESSIONMANAGER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "RPC/Address.h"

namespace LogCabin {

// forward declaration
namespace Core {
class Config;
}

// forward declaration
namespace Event {
class Loop;
}

// forward declaration
namespace RPC {
class ClientSession;
}

namespace Client {

/**
 * Used to create RPC::ClientSession objects and then immediately call
 * VerifyRecipient RPCs on them.
 *
 * TODO(ongaro): Consider encapsulating Backoff mechanism in here as well,
 * since session creation ought to be paired with Backoff.
 */
class SessionManager {
  public:

    /**
     * Gets and sets a value while holding a mutex.
     */
    template<typename T>
    class LockedAssignment {
      public:
        /**
         * Default constructor: empty.
         */
        LockedAssignment()
            : mutex()
            , empty(true)
            , value()
        {
        }

        /**
         * Constructor that initializes this object with the given value.
         */
        explicit LockedAssignment(T value)
            : mutex()
            , empty(false)
            , value(value)
        {
        }

        /**
         * Returns true and the value if there is one, or false and a default
         * constructed value otherwise.
         */
        std::pair<bool, T> get() const {
            std::lock_guard<std::mutex> lockGuard(mutex);
            if (empty)
                return {false, T()};
            T copy = value;
            return {true, copy};
        }

        /**
         * Returns the value if there is one, or a default constructed value
         * otherwise.
         */
        T getOrDefault() const {
            std::lock_guard<std::mutex> lockGuard(mutex);
            if (empty)
                return T();
            T copy = value;
            return copy;
        }

        /**
         * Clears out the value, if any.
         */
        void clear() {
            std::lock_guard<std::mutex> lockGuard(mutex);
            empty = true;
            value = T();
        }

        /**
         * Overwrites the value with the given one.
         */
        void set(const T& newValue) {
            std::lock_guard<std::mutex> lockGuard(mutex);
            empty = false;
            value = newValue;
        }

      private:
        mutable std::mutex mutex;
        bool empty;
        T value;
    };

    typedef LockedAssignment<std::string> ClusterUUID;
    typedef LockedAssignment<uint64_t> ServerId;

    /**
     * Construtor. Takes a couple of parameters that are common to all sessions
     * so that you don't have to repeatedly provide them to createSession.
     * \param eventLoop
     *      Event::Loop that will be used to find out when the session's
     *      underlying socket may be read from or written to without blocking.
     *      This object keeps a reference.
     * \param config
     *      General settings. This object keeps a reference.
     */
    SessionManager(Event::Loop& eventLoop,
                   const Core::Config& config);


    /**
     * Connect to the given address.
     * \param address
     *      The RPC server address on which to connect.
     * \param timeout
     *      After this time has elapsed, stop trying to initiate the connection
     *      and leave the session in an error state.
     * \param[in,out] clusterUUID
     *      If set, the recipient will confirm that it matches this cluster
     *      UUID. If empty and the recipient returns one, this will be set.
     * \param[in,out] serverId
     *      If set, the recipient will confirm that it has this server ID. If
     *      empty and the recipient returns one, this will be set.
     */
    std::shared_ptr<RPC::ClientSession>
    createSession(const RPC::Address& address,
                  RPC::Address::TimePoint timeout,
                  ClusterUUID* clusterUUID = NULL,
                  ServerId* serverId = NULL);

    Event::Loop& eventLoop;
  private:
    const Core::Config& config;
    /**
     * Used only for unit testing. Set to false, normally.
     */
    bool skipVerify;
};

} // namespace LogCabin::Client
} // namespace LogCabin

#endif /* LOGCABIN_CLIENT_SESSIONMANAGER_H */
