/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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
#include <sys/stat.h>
#include <unordered_map>

#include "Core/Debug.h"
#include "Core/STLUtil.h"
#include "Core/Util.h"
#include "Storage/FilesystemUtil.h"
#include "include/LogCabin/Debug.h"

namespace LogCabin {
namespace Core {
namespace Debug {

namespace Internal {
extern std::unordered_map<const char*, LogLevel> isLoggingCache;
const char* logLevelToString(LogLevel);
LogLevel logLevelFromString(const std::string& level);
LogLevel getLogLevel(const char* fileName);
const char* relativeFileName(const char* fileName);
}

namespace {

class CoreDebugTest : public ::testing::Test {
  public:
    CoreDebugTest()
        : tmpdir(Storage::FilesystemUtil::mkdtemp())
    {
        setLogPolicy({});
        setLogFile(stderr);
    }
    ~CoreDebugTest() {
        FILE* prev = setLogFile(stderr);
        if (prev != stderr)
            fclose(prev);
        Storage::FilesystemUtil::remove(tmpdir);
    }

    std::string tmpdir;
};


TEST_F(CoreDebugTest, logLevelToString) {
    EXPECT_STREQ("SILENT",
                 Internal::logLevelToString(LogLevel::SILENT));
    EXPECT_STREQ("ERROR",
                 Internal::logLevelToString(LogLevel::ERROR));
    EXPECT_STREQ("WARNING",
                 Internal::logLevelToString(LogLevel::WARNING));
    EXPECT_STREQ("NOTICE",
                 Internal::logLevelToString(LogLevel::NOTICE));
    EXPECT_STREQ("VERBOSE",
                 Internal::logLevelToString(LogLevel::VERBOSE));
}

TEST_F(CoreDebugTest, logLevelFromString) {
    EXPECT_EQ(LogLevel::SILENT,
              Internal::logLevelFromString("SILeNT"));
    EXPECT_EQ(LogLevel::ERROR,
              Internal::logLevelFromString("ERrOR"));
    EXPECT_EQ(LogLevel::WARNING,
              Internal::logLevelFromString("WARNiNG"));
    EXPECT_EQ(LogLevel::NOTICE,
              Internal::logLevelFromString("NOTIcE"));
    EXPECT_EQ(LogLevel::VERBOSE,
              Internal::logLevelFromString("VERBOsE"));
    EXPECT_DEATH(Internal::logLevelFromString("asdlf"),
                 "'asdlf' is not a valid log level.");
}

TEST_F(CoreDebugTest, getLogLevel) {
    // verify default is NOTICE
    EXPECT_EQ(LogLevel::NOTICE, Internal::getLogLevel(__FILE__));

    setLogPolicy({{"prefix", "VERBOSE"},
                  {"suffix", "ERROR"},
                  {"", "WARNING"}});
    EXPECT_EQ(LogLevel::VERBOSE, Internal::getLogLevel("prefixabcsuffix"));
    EXPECT_EQ(LogLevel::ERROR, Internal::getLogLevel("abcsuffix"));
    EXPECT_EQ(LogLevel::WARNING, Internal::getLogLevel("asdf"));
}

TEST_F(CoreDebugTest, relativeFileName) {
    EXPECT_STREQ("Core/DebugTest.cc",
                 Internal::relativeFileName(__FILE__));
    EXPECT_STREQ("/a/b/c",
                 Internal::relativeFileName("/a/b/c"));
}

TEST_F(CoreDebugTest, isLogging) {
    EXPECT_TRUE(isLogging(LogLevel::ERROR, "abc"));
    EXPECT_TRUE(isLogging(LogLevel::ERROR, "abc"));
    EXPECT_FALSE(isLogging(LogLevel::VERBOSE, "abc"));
    EXPECT_EQ((std::vector<std::pair<const char*, LogLevel>> {
                    { "abc", LogLevel::NOTICE },
              }),
              STLUtil::getItems(Internal::isLoggingCache));
}

TEST_F(CoreDebugTest, getLogFilename) {
    EXPECT_EQ("", getLogFilename());
    EXPECT_EQ("", setLogFilename(tmpdir + "/x"));
    EXPECT_EQ(tmpdir + "/x", getLogFilename());
    EXPECT_NE("", setLogFilename(tmpdir + "/bogus/x"));
    EXPECT_EQ(tmpdir + "/x", getLogFilename());
}

TEST_F(CoreDebugTest, setLogFilename) {
    EXPECT_EQ("", setLogFilename(tmpdir + "/x"));
    EXPECT_EQ(std::string() +
              "Could not open " + tmpdir + "/bogus/x for writing debug "
              "log messages: No such file or directory",
              setLogFilename(tmpdir + "/bogus/x"));

    EXPECT_EQ(tmpdir + "/x", getLogFilename());
    ERROR("If you see this on your terminal, this test has failed");
    struct stat stats;
    EXPECT_EQ(0, stat((tmpdir + "/x").c_str(), &stats)) << strerror(errno);
    EXPECT_LT(10, stats.st_size);
}

TEST_F(CoreDebugTest, reopenLogFromFilename) {
    EXPECT_EQ("", reopenLogFromFilename());
    EXPECT_EQ("", setLogFilename(tmpdir + "/x"));
    EXPECT_EQ("", reopenLogFromFilename());
}

TEST_F(CoreDebugTest, setLogFile) {
    EXPECT_EQ(stderr, setLogFile(stdout));
    EXPECT_EQ(stdout, setLogFile(stderr));
}

TEST_F(CoreDebugTest, setLogFile_clearsFilename) {
    EXPECT_EQ("", setLogFilename(tmpdir + "/x"));
    EXPECT_EQ(0, fclose(setLogFile(stderr)));
    EXPECT_EQ("", getLogFilename());
}

struct VectorHandler {
    VectorHandler()
        : messages()
    {
    }
    void operator()(DebugMessage message) {
        messages.push_back(message);
    }
    std::vector<DebugMessage> messages;
};

void
removeLogHandler()
{
    Core::Debug::setLogHandler(std::function<void(DebugMessage)>());
}

TEST_F(CoreDebugTest, setLogHandler) {
    Core::Util::Finally _(removeLogHandler);
    VectorHandler handler;
    setLogHandler(std::ref(handler));
    ERROR("Hello, world! %d", 9);
    EXPECT_EQ(1U, handler.messages.size());
    const DebugMessage& m = handler.messages.at(0);
    EXPECT_STREQ("Core/DebugTest.cc", m.filename);
    EXPECT_LT(1, m.linenum);
    EXPECT_STREQ("TestBody", m.function);
    EXPECT_EQ(int(LogLevel::ERROR), m.logLevel);
    EXPECT_STREQ("ERROR", m.logLevelString);
    EXPECT_EQ("Hello, world! 9", m.message);
}

TEST_F(CoreDebugTest, setLogPolicy) {
    setLogPolicy({{"prefix", "VERBOSE"},
                  {"suffix", "ERROR"},
                  {"", "WARNING"}});
    EXPECT_EQ(LogLevel::VERBOSE, Internal::getLogLevel("prefixabcsuffix"));
    EXPECT_EQ(LogLevel::ERROR, Internal::getLogLevel("abcsuffix"));
    EXPECT_EQ(LogLevel::WARNING, Internal::getLogLevel("asdf"));
}

std::string normalize(const std::string& in) {
    return logPolicyToString(logPolicyFromString(in));
}


TEST_F(CoreDebugTest, logPolicyFromString) {
    EXPECT_EQ("NOTICE", normalize(""));
    EXPECT_EQ("ERROR", normalize("ERROR"));
    EXPECT_EQ("ERROR", normalize("@ERROR"));
    EXPECT_EQ("prefix@VERBOSE,suffix@ERROR,WARNING",
              normalize("prefix@VERBOSE,suffix@ERROR,WARNING"));
    EXPECT_EQ("prefix@VERBOSE,suffix@ERROR,NOTICE",
              normalize("prefix@VERBOSE,suffix@ERROR,@NOTICE"));
    EXPECT_EQ("prefix@VERBOSE,suffix@ERROR,NOTICE",
              normalize("prefix@VERBOSE,suffix@ERROR,NOTICE"));
}

TEST_F(CoreDebugTest, logPolicyToString) {
    EXPECT_EQ("NOTICE",
              logPolicyToString(getLogPolicy()));
    setLogPolicy({{"", "ERROR"}});
    EXPECT_EQ("ERROR",
              logPolicyToString(getLogPolicy()));
    setLogPolicy({{"prefix", "VERBOSE"},
                  {"suffix", "ERROR"},
                  {"", "WARNING"}});
    EXPECT_EQ("prefix@VERBOSE,suffix@ERROR,WARNING",
              logPolicyToString(getLogPolicy()));
    setLogPolicy({{"prefix", "VERBOSE"},
                  {"suffix", "ERROR"}});
    EXPECT_EQ("prefix@VERBOSE,suffix@ERROR,NOTICE",
              logPolicyToString(getLogPolicy()));
}


// log: low cost-benefit in testing

} // namespace LogCabin::Core::Debug::<anonymous>
} // namespace LogCabin::Core::Debug
} // namespace LogCabin::Core
} // namespace LogCabin

