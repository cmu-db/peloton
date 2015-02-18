#include "backend/tester.h"
#include "gtest/gtest.h"

TEST(TestCase1, Test1) {
    EXPECT_EQ(3, TestFunc(1,2));
}
