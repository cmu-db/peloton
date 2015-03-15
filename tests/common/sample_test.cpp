#include "backend/nstore.h"
#include "gtest/gtest.h"


namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Sample Test Example
//===--------------------------------------------------------------------===//

TEST(SampleTest, Test1) {

	EXPECT_EQ(3, SampleFunc(1,2));

}

TEST(SampleTest, Test2) {

	EXPECT_NE(1, SampleFunc(1,2));

}

} // End test namespace
} // End nstore namespace
