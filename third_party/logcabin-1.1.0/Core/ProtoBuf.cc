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

#include <google/protobuf/text_format.h>
#include <memory>
#include <sstream>

#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/StringUtil.h"
#include "Core/Util.h"

namespace google {
namespace protobuf {

bool
operator==(const Message& a, const Message& b)
{
    // This is a close enough approximation of equality.
    return (a.GetTypeName() == b.GetTypeName() &&
            a.DebugString() == b.DebugString());
}

bool
operator!=(const Message& a, const Message& b)
{
    return !(a == b);
}

bool
operator==(const Message& a, const std::string& bStr)
{
    std::unique_ptr<Message> b(a.New());
    LogSilencer _;
    TextFormat::ParseFromString(bStr, b.get());
    return (a == *b);
}

bool
operator==(const std::string& a, const Message& b)
{
    return (b == a);
}

bool
operator!=(const Message& a, const std::string& b)
{
    return !(a == b);
}

bool
operator!=(const std::string& a, const Message& b)
{
    return !(a == b);
}

} // namespace google::protobuf
} // namespace google

namespace LogCabin {
namespace Core {
namespace ProtoBuf {

using Core::StringUtil::replaceAll;

namespace Internal {

void
fromString(const std::string& str, google::protobuf::Message& protoBuf)
{
    google::protobuf::LogSilencer _;
    google::protobuf::TextFormat::ParseFromString(str, &protoBuf);
}

} // namespace LogCabin::ProtoBuf::Internal

std::string
dumpString(const google::protobuf::Message& protoBuf,
           bool forCopyingIntoTest)
{
    std::string output;
    google::protobuf::TextFormat::Printer printer;
    if (forCopyingIntoTest) {
        // Most lines that use these strings will look like this:
        // ^    EXPECT_EQ(...,$
        // ^              "..."
        // ^              "...");
        //  12345678901234
        // Therefore, we want 14 leading spaces. Tell gtest we want 16, though,
        // so that when we add in the surrounding quotes later, lines won't
        // wrap.
        printer.SetInitialIndentLevel(8);
    }
    printer.SetUseShortRepeatedPrimitives(true);
    printer.PrintToString(protoBuf, &output);
    if (forCopyingIntoTest) {
        // TextFormat::Printer escapes ' already.
        replaceAll(output, "\"", "'");
        replaceAll(output, "                ", "              \"");
        replaceAll(output, "\n", "\"\n");
    }
    if (!protoBuf.IsInitialized()) {
        std::vector<std::string> errors;
        protoBuf.FindInitializationErrors(&errors);
        std::ostringstream outputBuf;
        outputBuf << output;
        for (auto it = errors.begin(); it != errors.end(); ++it) {
            if (forCopyingIntoTest) {
                 outputBuf << "              \"" << *it << ": UNDEFINED\"\n";
            } else {
                 outputBuf << *it << ": UNDEFINED\n";
            }
        }
        return output + outputBuf.str();
    }
    return output;
}

std::unique_ptr<google::protobuf::Message>
copy(const google::protobuf::Message& protoBuf)
{
    std::unique_ptr<google::protobuf::Message> ret(protoBuf.New());
    ret->CopyFrom(protoBuf);
    return ret;
}

bool
parse(const Core::Buffer& from,
      google::protobuf::Message& to,
      uint32_t skipBytes)
{
    google::protobuf::LogSilencer logSilencer;
    if (!to.ParseFromArray(
                        static_cast<const char*>(from.getData()) + skipBytes,
                        Util::downCast<int>(from.getLength() - skipBytes))) {
        WARNING("Missing fields in protocol buffer of type %s: %s",
                to.GetTypeName().c_str(),
                to.InitializationErrorString().c_str());
        return false;
    }
    return true;
}

void
serialize(const google::protobuf::Message& from,
          Core::Buffer& to,
          uint32_t skipBytes)
{
    // SerializeToArray seems to always return true, so we explicitly check
    // IsInitialized to make sure all required fields are set.
    if (!from.IsInitialized()) {
        PANIC("Missing fields in protocol buffer of type %s: %s (have %s)",
              from.GetTypeName().c_str(),
              from.InitializationErrorString().c_str(),
              dumpString(from).c_str());
    }
    uint32_t length = uint32_t(from.ByteSize());
    char* data = new char[skipBytes + length];
    from.SerializeToArray(data + skipBytes, int(length));
    to.setData(data, skipBytes + length, Core::Buffer::deleteArrayFn<char>);
}


} // namespace LogCabin::Core::ProtoBuf
} // namespace LogCabin::Core
} // namespace LogCabin
