/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include <memory>
#include <gtest/gtest.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "RPC/MessageSocket.h"

namespace LogCabin {
namespace RPC {
namespace {

const char* payload = "abcdefghijklmnopqrstuvwxyz0123456789"
                      "ABCDEFGHIJKLMNOPQRSTUVWXYZ+-";

using Core::Buffer;

class MyMessageSocketHandler : public MessageSocket::Handler {
    MyMessageSocketHandler()
        : lastReceivedId(~0UL)
        , lastReceivedPayload()
        , disconnected(false)
    {
    }
    void handleReceivedMessage(MessageId messageId, Buffer message) {
        lastReceivedId = messageId;
        lastReceivedPayload = std::move(message);
    }
    void handleDisconnect() {
        EXPECT_FALSE(disconnected);
        disconnected = true;
    }
    MessageId lastReceivedId;
    Buffer lastReceivedPayload;
    bool disconnected;
};

class RPCMessageSocketTest : public ::testing::Test {
    RPCMessageSocketTest()
        : loop()
        , handler()
        , msgSocket()
        , remote(-1)
    {
        int socketPair[2];
        EXPECT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM|SOCK_NONBLOCK, 0,
                                socketPair));
        remote = socketPair[1];
        msgSocket.reset(new MessageSocket(handler, loop, socketPair[0], 64));
    }
    ~RPCMessageSocketTest()
    {
        closeRemote();
    }
    void
    closeRemote()
    {
        if (remote >= 0) {
            EXPECT_EQ(0, close(remote));
            remote = -1;
        }
    }

    Event::Loop loop;
    MyMessageSocketHandler handler;
    std::unique_ptr<MessageSocket> msgSocket;
    int remote;
};

TEST_F(RPCMessageSocketTest, close) {
    msgSocket->close();
    EXPECT_TRUE(handler.disconnected);
}


TEST_F(RPCMessageSocketTest, sendMessage) {
    EXPECT_DEATH(msgSocket->sendMessage(0, Buffer(NULL, ~0U, NULL)),
                 "too long to send");

    char hi[3];
    strncpy(hi, "hi", sizeof(hi));
    msgSocket->sendMessage(123, Buffer(hi, 3, NULL));
    ASSERT_EQ(1U, msgSocket->outboundQueue.size());
    MessageSocket::Outbound& outbound = msgSocket->outboundQueue.front();
    EXPECT_EQ(0U, outbound.bytesSent);
    outbound.header.fromBigEndian();
    EXPECT_EQ(123U, outbound.header.messageId);
    EXPECT_EQ(3U, outbound.header.payloadLength);
    EXPECT_EQ(hi, outbound.message.getData());
    EXPECT_EQ(3U, outbound.message.getLength());
}

TEST_F(RPCMessageSocketTest, readableSpurious) {
    msgSocket->readable();
    EXPECT_FALSE(handler.disconnected);
    msgSocket->readable();
    EXPECT_EQ(0U, msgSocket->inbound.bytesRead);
    EXPECT_FALSE(handler.disconnected);
}

TEST_F(RPCMessageSocketTest, readableSenderDisconnectInHeader) {
    closeRemote();
    msgSocket->readable();
    EXPECT_TRUE(handler.disconnected);
}

TEST_F(RPCMessageSocketTest, readableMessageTooLong) {
    MessageSocket::Header header;
    header.fixed = 0xdaf4;
    header.version = 1;
    header.payloadLength = 65;
    header.messageId = 0;
    header.toBigEndian();
    EXPECT_EQ(ssize_t(sizeof(header)),
              send(remote, &header, sizeof(header), 0));
    LogCabin::Core::Debug::setLogPolicy({{"", "ERROR"}});
    msgSocket->readable();
    ASSERT_TRUE(handler.disconnected);
}

TEST_F(RPCMessageSocketTest, readableEmptyPayload) {
    // This test exists to prevent a regression. Before, sending a message with
    // a length of 0 was not handled correctly.
    MessageSocket::Header header;
    header.fixed = 0xdaf4;
    header.version = 1;
    header.payloadLength = 0;
    header.messageId = 12;
    header.toBigEndian();
    EXPECT_EQ(ssize_t(sizeof(header)),
              send(remote, &header, sizeof(header), 0));
    msgSocket->readable();
    ASSERT_FALSE(handler.disconnected);
    EXPECT_EQ(0U, msgSocket->inbound.bytesRead);
    EXPECT_EQ(12U, handler.lastReceivedId);
    EXPECT_EQ(0U, handler.lastReceivedPayload.getLength());
}


TEST_F(RPCMessageSocketTest, readableSenderDisconnectInPayload) {
    MessageSocket::Header header;
    header.fixed = 0xdaf4;
    header.version = 1;
    header.payloadLength = 1;
    header.messageId = 0;
    header.toBigEndian();
    EXPECT_EQ(ssize_t(sizeof(header)),
              send(remote, &header, sizeof(header), 0));
    msgSocket->readable();
    ASSERT_FALSE(handler.disconnected);
    EXPECT_EQ(sizeof(header), msgSocket->inbound.bytesRead);
    closeRemote();
    msgSocket->readable();
    EXPECT_TRUE(handler.disconnected);
}

TEST_F(RPCMessageSocketTest, readableAllAtOnce) {
    MessageSocket::Header header;
    header.fixed = 0xdaf4;
    header.version = 1;
    header.payloadLength = 64;
    header.messageId = 0xdeadbeef8badf00d;
    header.toBigEndian();
    char buf[sizeof(header) + 64];
    memcpy(buf, &header, sizeof(header));
    header.fromBigEndian();
    strncpy(buf + sizeof(header), payload, 64);
    EXPECT_EQ(ssize_t(sizeof(buf)), send(remote, buf, sizeof(buf), 0));
    // will do one read for the header
    msgSocket->readable();
    ASSERT_FALSE(handler.disconnected);
    // and a second read for the data
    msgSocket->readable();
    ASSERT_FALSE(handler.disconnected);
    EXPECT_EQ(header.messageId, handler.lastReceivedId);
    EXPECT_EQ(payload,
              std::string(static_cast<const char*>(
                            handler.lastReceivedPayload.getData()),
                          handler.lastReceivedPayload.getLength()));

    EXPECT_EQ(1, send(remote, "x", 1, 0));
    msgSocket->readable();
    EXPECT_FALSE(handler.disconnected);
    EXPECT_EQ(1U, msgSocket->inbound.bytesRead);
}

TEST_F(RPCMessageSocketTest, readableBytewise) {
    MessageSocket::Header header;
    header.fixed = 0xdaf4;
    header.version = 1;
    header.payloadLength = 64;
    header.messageId = 0xdeadbeef8badf00d;
    header.toBigEndian();
    char buf[sizeof(header) + 64];
    memcpy(buf, &header, sizeof(header));
    header.fromBigEndian();
    strncpy(buf + sizeof(header), payload, 64);
    for (uint32_t i = 0; i < sizeof(buf); ++i) {
        EXPECT_EQ(1, send(remote, &buf[i], 1, 0));
        msgSocket->readable();
        ASSERT_FALSE(handler.disconnected) << "Disconnected at byte " << i;
        msgSocket->readable(); // spurious
        ASSERT_FALSE(handler.disconnected) << "Disconnected at byte " << i;
    }
    EXPECT_EQ(header.messageId, handler.lastReceivedId);
    EXPECT_EQ(payload,
              std::string(static_cast<const char*>(
                            handler.lastReceivedPayload.getData()),
                          handler.lastReceivedPayload.getLength()));

    EXPECT_EQ(1, send(remote, "x", 1, 0));
    msgSocket->readable();
    EXPECT_FALSE(handler.disconnected);
    EXPECT_EQ(1U, msgSocket->inbound.bytesRead);
}

TEST_F(RPCMessageSocketTest, writableSpurious) {
    msgSocket->writable();
}

TEST_F(RPCMessageSocketTest, writableDisconnect) {
    closeRemote();
    msgSocket->sendMessage(123,
                           Buffer(const_cast<char*>(payload), 64, NULL));
    ASSERT_EQ(1U, msgSocket->outboundQueue.size());
    ASSERT_FALSE(handler.disconnected);
    msgSocket->writable();
    ASSERT_TRUE(handler.disconnected);
}

TEST_F(RPCMessageSocketTest, writableNormal) {
    MessageSocket::Header header;
    header.fixed = 0xdaf4;
    header.version = 1;
    header.payloadLength = 64;
    header.messageId = 123;
    header.toBigEndian();
    char expected[sizeof(header) + 64];
    memcpy(expected, &header, sizeof(header));
    strncpy(expected + sizeof(header), payload, 64);

    for (uint32_t i = 0; i < sizeof(MessageSocket::Header) + 64; ++i) {
        msgSocket->sendMessage(123,
                               Buffer(const_cast<char*>(payload), 64, NULL));
        ASSERT_EQ(1U, msgSocket->outboundQueue.size());
        msgSocket->outboundQueue.front().bytesSent = i;
        msgSocket->writable();
        ASSERT_FALSE(handler.disconnected);
        ASSERT_EQ(0U, msgSocket->outboundQueue.size());
        char buf[sizeof(MessageSocket::Header) + 64 + 1];
        ASSERT_EQ(ssize_t(sizeof(buf)) - i - 1,
                  recv(remote, buf + i, sizeof(buf) - i, 0));
        ASSERT_EQ(0, memcmp(expected + i,
                            buf + i,
                            sizeof(buf) - i - 1))
            << "suffix of packet does not match from byte " << i;
    }
}

} // namespace LogCabin::RPC::<anonymous>
} // namespace LogCabin::RPC
} // namespace LogCabin
