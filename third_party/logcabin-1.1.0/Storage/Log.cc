/* Copyright (c) 2012-2013 Stanford University
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

#include <algorithm>
#include <fcntl.h>
#include <ostream>
#include <sys/stat.h>
#include <unistd.h>

#include "build/Protocol/Client.pb.h"
#include "build/Protocol/Raft.pb.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Storage/Log.h"

namespace LogCabin {
namespace Storage {

////////// Log::Sync //////////

Log::Sync::Sync(uint64_t lastIndex)
    : lastIndex(lastIndex)
    , completed(false) {
}

Log::Sync::~Sync()
{
    assert(completed);
}

////////// Log //////////

Log::Log()
    : metadata()
{
}

Log::~Log()
{
}

std::ostream&
operator<<(std::ostream& os, const Log& log)
{
    os << "Log:" << std::endl;
    os << "metadata start: " << std::endl;
    os << Core::ProtoBuf::dumpString(log.metadata);
    os << "end of metadata" << std::endl;
    os << "startIndex: " << log.getLogStartIndex() << std::endl;
    os << std::endl;
    for (uint64_t i = log.getLogStartIndex();
         i <= log.getLastLogIndex();
         ++i) {
        os << "Entry " << i << " start:" << std::endl;
        Log::Entry e = log.getEntry(i);
        if (e.type() == Protocol::Raft::EntryType::DATA) {
            std::string data = e.data();
            Core::Buffer buffer(const_cast<char*>(data.data()),
                                data.length(), NULL);
            Protocol::Client::StateMachineCommand::Request command;
            if (!Core::ProtoBuf::parse(buffer, command)) {
                WARNING("Could not parse protobuf in log entry %lu", i);
                os << Core::ProtoBuf::dumpString(e);
            } else {
                e.clear_data();
                os << Core::ProtoBuf::dumpString(e);
                os << "data: " << Core::ProtoBuf::dumpString(command);
            }
        } else {
            os << Core::ProtoBuf::dumpString(e);
        }
        os << "end of entry " << i << std::endl;
        os << std::endl;
    }
    os << std::endl;
    return os;
}

} // namespace LogCabin::Storage
} // namespace LogCabin
