#include "backend/tester.h"
#include "gtest/gtest.h"


namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Func Test
//===--------------------------------------------------------------------===//

TEST(TestCase1, Test1) {

	EXPECT_EQ(3, TestFunc(1,2));
}

} // End test namespace
} // End nstore namespace
