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

#include <fcntl.h>
#include <sys/stat.h>

#include <gtest/gtest.h>

#include "Core/Debug.h"
#include "Core/Config.h"
#include "Storage/FilesystemUtil.h"

namespace LogCabin {
namespace Core {

using std::map;
using std::string;

class CoreConfigTest : public ::testing::Test {
  public:
    CoreConfigTest()
        : tmpdir(Storage::FilesystemUtil::mkdtemp())
    {
        int fd = open((tmpdir + "/a").c_str(), O_WRONLY|O_CREAT, 0644);
        if (fd < 0)
            PANIC("Couldn't open temp file");
        const char* file = "# this is a sample config file \n"
            "        # here's a comment in a random place \n"
            " double  =  3.14  \n"
            " int  =  314  #  comment  \n"
            " string  =  a = b = c = d  #  newline comes next  \n"
            "\n"
            " multiline  =  \n"
            "                strange\n"
            "# just a comment\n"
            " empty  =  \n";
        using Storage::FilesystemUtil::write;
        if (write(fd, file, uint32_t(strlen(file))) == -1)
            PANIC("Couldn't write to temp file");
        close(fd);
    }
    ~CoreConfigTest() {
        Storage::FilesystemUtil::remove(tmpdir);
    }
    std::string tmpdir;
};

TEST_F(CoreConfigTest, constructor_delim) {
    Config config;
    EXPECT_EQ("=", config.delimiter);
    EXPECT_EQ("#", config.comment);
    EXPECT_TRUE(config.contents.empty());

    Config config2(":", "//");
    EXPECT_EQ(":", config2.delimiter);
    EXPECT_EQ("//", config2.comment);
    EXPECT_TRUE(config2.contents.empty());
}

TEST_F(CoreConfigTest, constructor_withOptions) {
    map<string, string> options = {
                {"double", "3.14"},
                {"int", "314"},
                {"string", "a = b = c = d"}
    };
    Config config(options);
    EXPECT_EQ(options, config.contents);
}

TEST_F(CoreConfigTest, readFile) {
    Config config;
    config.readFile(tmpdir + "/a");
    EXPECT_THROW(config.readFile(tmpdir + "/bogus"),
                 Config::FileNotFound);
}

TEST_F(CoreConfigTest, streamIn) {
    Config config;
    config.readFile(tmpdir + "/a");
    EXPECT_EQ((map<string, string> {
                {"double", "3.14"},
                {"int", "314"},
                {"string", "a = b = c = d"},
                {"multiline", "\nstrange"},
                {"empty", ""},
              }), config.contents);
}

TEST_F(CoreConfigTest, streamOut) {
    Config config;
    config.set("k", "v");
    config.set("x", "z");
    std::stringstream ss;
    ss << config;
    EXPECT_EQ("k = v\nx = z\n", ss.str());
}

TEST_F(CoreConfigTest, keyExists) {
    Config config;
    config.set("k", "v");
    EXPECT_TRUE(config.keyExists("k"));
    EXPECT_FALSE(config.keyExists("bogus"));
}

TEST_F(CoreConfigTest, readWithoutDefault) {
    Config config;
    config.set("k", "v");
    EXPECT_THROW(config.read("bogus"),
                 Config::KeyNotFound);
    EXPECT_EQ("v", config.read("k"));
    EXPECT_THROW(config.read<int>("k"),
                 Config::ConversionError);
}

TEST_F(CoreConfigTest, readWithDefault) {
    Config config;
    config.set("k", "v");
    EXPECT_EQ("foo", config.read<string>("bogus", "foo"));
    EXPECT_EQ("v", config.read<string>("k", "bar"));
    EXPECT_THROW(config.read("k", 7),
                 Config::ConversionError);
}

TEST_F(CoreConfigTest, set) {
    Config config;
    config.set("  k  ", "  v  ");
    EXPECT_EQ((map<string, string> {
                {"k", "v"},
              }), config.contents);
}

TEST_F(CoreConfigTest, remove) {
    Config config;
    config.set("k", "v");
    config.set("k2", "v2");
    config.remove("k");
    config.remove("bogus");
    EXPECT_EQ((map<string, string> {
                {"k2", "v2"},
              }), config.contents);
}

TEST_F(CoreConfigTest, toString) {
    EXPECT_EQ("1", Config::toString(1));
    EXPECT_EQ("1", Config::toString(1U));
    EXPECT_EQ("18446744073709551615", Config::toString(~0UL));
    EXPECT_EQ("3.14", Config::toString(3.14));
    EXPECT_EQ("-9", Config::toString(-9));
    EXPECT_EQ("pie", Config::toString("pie"));
    EXPECT_EQ("0", Config::toString(0));
    EXPECT_EQ("false", Config::toString(false));
    EXPECT_EQ("true", Config::toString(true));
}

TEST_F(CoreConfigTest, fromString) {
    EXPECT_EQ("foo", Config::fromString<string>("k", "foo"));
    EXPECT_EQ(-1, Config::fromString<int>("k", "-1"));
    EXPECT_EQ(0, Config::fromString<int>("k", "0"));
    EXPECT_EQ(1, Config::fromString<int>("k", "1"));
    EXPECT_EQ(~0U, Config::fromString<uint32_t>("k", "4294967295"));
    EXPECT_EQ(5.3, Config::fromString<double>("k", "5.3"));
    EXPECT_THROW(Config::fromString<int>("k", ""),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<int>("k", "a"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<int>("k", "5a"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<int>("k", "5.3"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<double>("k", "a"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<int>("k", "2147483648"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<int>("k", "-2147483649"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<uint32_t>("k", "4294967296"),
                 Config::ConversionError);
    EXPECT_THROW(Config::fromString<int>("k", "0xFFFFFFFF"),
                 Config::ConversionError);
    try {
        Config::fromString<int>("k", "1.5");
    } catch (const Config::ConversionError& e) {
        EXPECT_STREQ("The value k for key 1.5 could not be converted to a int",
                     e.what());
    }

    EXPECT_FALSE(Config::fromString<bool>("k", "false"));
    EXPECT_FALSE(Config::fromString<bool>("k", "f"));
    EXPECT_FALSE(Config::fromString<bool>("k", "no"));
    EXPECT_FALSE(Config::fromString<bool>("k", "n"));
    EXPECT_FALSE(Config::fromString<bool>("k", "FALSE"));
    EXPECT_FALSE(Config::fromString<bool>("k", "F"));
    EXPECT_FALSE(Config::fromString<bool>("k", "NO"));
    EXPECT_FALSE(Config::fromString<bool>("k", "N"));
    EXPECT_FALSE(Config::fromString<bool>("k", "0"));
    EXPECT_TRUE(Config::fromString<bool>("k", "true"));
    EXPECT_TRUE(Config::fromString<bool>("k", "t"));
    EXPECT_TRUE(Config::fromString<bool>("k", "yes"));
    EXPECT_TRUE(Config::fromString<bool>("k", "y"));
    EXPECT_TRUE(Config::fromString<bool>("k", "TRUE"));
    EXPECT_TRUE(Config::fromString<bool>("k", "T"));
    EXPECT_TRUE(Config::fromString<bool>("k", "YES"));
    EXPECT_TRUE(Config::fromString<bool>("k", "Y"));
    EXPECT_TRUE(Config::fromString<bool>("k", "1"));
    EXPECT_THROW(Config::fromString<bool>("k", "-1"),
                 Config::ConversionError);
}

TEST_F(CoreConfigTest, readLine) {
    // This is tested sufficiently in streamIn
}

} // namespace LogCabin::Core
} // namespace LogCabin
