/* Copyright (c) 2009-2011 Stanford University
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

#include <cstdlib>
#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <memory>

#include "Core/Debug.h"
#include "Storage/FilesystemUtil.h"

namespace {

/**
 * Replaces gtest's PrettyUnitTestResultPrinter with something less verbose.
 * This forwards callbacks to the default printer if and when they might be
 * interesting.
 *
 * This comes from the RAMCloud project.
 */
class QuietUnitTestResultPrinter : public testing::TestEventListener {
  public:
    /**
     * Constructor.
     * \param prettyPrinter
     *      gtest's default unit test result printer. This object takes
     *      ownership of prettyPrinter and will delete it later.
     */
    explicit QuietUnitTestResultPrinter(TestEventListener* prettyPrinter)
        : prettyPrinter(prettyPrinter)
        , currentTestCase(NULL)
        , currentTestInfo(NULL)
    {}
    void OnTestProgramStart(const testing::UnitTest& unit_test) {
        prettyPrinter->OnTestProgramStart(unit_test);
    }
    void OnTestIterationStart(const testing::UnitTest& unit_test,
                              int iteration) {
        prettyPrinter->OnTestIterationStart(unit_test, iteration);
    }
    void OnEnvironmentsSetUpStart(const testing::UnitTest& unit_test) {}
    void OnEnvironmentsSetUpEnd(const testing::UnitTest& unit_test) {}
    void OnTestCaseStart(const testing::TestCase& test_case) {
        currentTestCase = &test_case;
    }
    void OnTestStart(const testing::TestInfo& test_info) {
        currentTestInfo = &test_info;
    }
    void OnTestPartResult(const testing::TestPartResult& test_part_result) {
        if (test_part_result.type() != testing::TestPartResult::kSuccess) {
            if (currentTestCase != NULL) {
                prettyPrinter->OnTestCaseStart(*currentTestCase);
                currentTestCase = NULL;
            }
            if (currentTestInfo != NULL) {
                prettyPrinter->OnTestStart(*currentTestInfo);
                currentTestInfo = NULL;
            }
            prettyPrinter->OnTestPartResult(test_part_result);
        }
    }
    void OnTestEnd(const testing::TestInfo& test_info) {
        currentTestInfo = NULL;
    }
    void OnTestCaseEnd(const testing::TestCase& test_case) {
        currentTestCase = NULL;
    }
    void OnEnvironmentsTearDownStart(const testing::UnitTest& unit_test) {}
    void OnEnvironmentsTearDownEnd(const testing::UnitTest& unit_test) {}
    void OnTestIterationEnd(const testing::UnitTest& unit_test,
                            int iteration) {
        prettyPrinter->OnTestIterationEnd(unit_test, iteration);
    }
    void OnTestProgramEnd(const testing::UnitTest& unit_test) {
        prettyPrinter->OnTestProgramEnd(unit_test);
    }
  private:
    /// gtest's default unit test result printer.
    std::unique_ptr<TestEventListener> prettyPrinter;
    /// The currently running TestCase that hasn't been printed, or NULL.
    const testing::TestCase* currentTestCase;
    /// The currently running TestInfo that hasn't been printed, or NULL.
    const testing::TestInfo* currentTestInfo;

    QuietUnitTestResultPrinter(const QuietUnitTestResultPrinter&) = delete;
    QuietUnitTestResultPrinter&
    operator=(const QuietUnitTestResultPrinter&) = delete;
};

class GTestSetupListener : public ::testing::EmptyTestEventListener {
  public:
    // this fires before each test fixture's constructor
    void OnTestStart(const ::testing::TestInfo& testInfo) {
        LogCabin::Core::Debug::setLogPolicy({
            {"", "WARNING"}
        });
        LogCabin::Storage::FilesystemUtil::skipFsync = true;
    }
};

bool
verbose()
{
    const char* env = getenv("VERBOSE");
    if (env == NULL)
        return false;
    return (strcmp(env, "1") == 0);
}

} // anonymous namespace

int
main(int argc, char *argv[])
{
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::UnitTest& unitTest = *::testing::UnitTest::GetInstance();
    ::testing::TestEventListeners& listeners = unitTest.listeners();

    // add global set up routine
    listeners.Append(new GTestSetupListener());

    if (!verbose()) {
        // replace default output printer with quiet one
        listeners.Append(new QuietUnitTestResultPrinter(
                                listeners.Release(
                                   listeners.default_result_printer())));
    }

    int ret = RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
