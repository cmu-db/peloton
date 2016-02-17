/* Copyright (c) 2014 Stanford University
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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include "Core/Config.h"
#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Storage/Layout.h"
#include "Storage/LogFactory.h"
#include "Storage/MemoryLog.h"
#include "Storage/SegmentedLog.h"
#include "Storage/SimpleFileLog.h"

namespace LogCabin {
namespace Storage {

namespace {

class StorageLogFactoryTest : public ::testing::Test {
  public:
    StorageLogFactoryTest()
        : layout()
        , config()
    {
        layout.initTemporary();
    }
    ~StorageLogFactoryTest() {
    }
    Storage::Layout layout;
    Core::Config config;
};

TEST_F(StorageLogFactoryTest, makeLog_Memory)
{
    config.set("storageModule", "Memory");
    std::unique_ptr<Log> log = LogFactory::makeLog(config, layout);
    EXPECT_EQ("Memory", log->getName());
}

TEST_F(StorageLogFactoryTest, makeLog_SimpleFile)
{
    // expect warning
    Core::Debug::setLogPolicy({
        {"Storage/SimpleFileLog.cc", "ERROR"}
    });

    config.set("storageModule", "SimpleFile");
    std::unique_ptr<Log> log = LogFactory::makeLog(config, layout);
    EXPECT_EQ("SimpleFile", log->getName());
}

TEST_F(StorageLogFactoryTest, makeLog_Segmented_Binary)
{
    // expect warning
    Core::Debug::setLogPolicy({
        {"Storage/SegmentedLog.cc", "ERROR"}
    });

    // default
    std::unique_ptr<Log> log = LogFactory::makeLog(config, layout);
    EXPECT_EQ("Segmented-Binary", log->getName());
    log.reset();

    config.set("storageModule", "Segmented");
    log = LogFactory::makeLog(config, layout);
    EXPECT_EQ("Segmented-Binary", log->getName());
    log.reset();

    config.set("storageModule", "Segmented-Binary");
    log = LogFactory::makeLog(config, layout);
    EXPECT_EQ("Segmented-Binary", log->getName());

}

TEST_F(StorageLogFactoryTest, makeLog_Segmented_Text)
{
    // expect warning
    Core::Debug::setLogPolicy({
        {"Storage/SegmentedLog.cc", "ERROR"}
    });

    config.set("storageModule", "Segmented-Text");
    std::unique_ptr<Log> log = LogFactory::makeLog(config, layout);
    EXPECT_EQ("Segmented-Text", log->getName());
}

TEST_F(StorageLogFactoryTest, makeLog_notfound)
{
    config.set("storageModule", "punchcard");
    EXPECT_DEATH(LogFactory::makeLog(config, layout),
                 "Unknown storage module");
}

} // namespace LogCabin::Storage::<anonymous>
} // namespace LogCabin::Storage
} // namespace LogCabin
