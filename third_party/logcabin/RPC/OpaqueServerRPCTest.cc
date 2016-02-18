/* Copyright (c) 2012 Stanford University
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

#include <gtest/gtest.h>

#include "RPC/OpaqueServerRPC.h"

namespace LogCabin {
namespace RPC {
namespace {

// There's not much here to test except for sendReply, which is difficult to
// test.

TEST(RPCOpaqueServerRPCTest, constructor_default)
{
    // make sure a default-constructed OpaqueServerRPC is harmless
    OpaqueServerRPC rpc;
    rpc.sendReply();
    rpc.closeSession();
}


} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
