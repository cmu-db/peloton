//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// sample_test.cpp
//
// Identification: tests/common/sample_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <iostream>
#include <memory>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Sample Test Example
//===--------------------------------------------------------------------===//

TEST(SampleTest, Test1) { EXPECT_EQ(3, 1 + 2); }

TEST(SampleTest, Test2) { EXPECT_NE(1, 1 + 2); }

struct Foo {
    Foo() { std::cout << "Foo...\n"; }
    ~Foo() { std::cout << "~Foo...\n"; }
};

struct D {
    void operator()(Foo* p) const {
        std::cout << "Call delete for Foo object...\n";
        delete p;
    }
};

TEST(SampleTest, SharedPtr) {

  {
    std::shared_ptr<Foo> sh1;

    EXPECT_EQ(sh1.use_count(), 0);
  }

  {
    Foo *foo = new Foo();

    std::shared_ptr<Foo> sh2(foo);
    std::shared_ptr<Foo> sh3(sh2);
    std::shared_ptr<Foo> sh4;
    std::shared_ptr<Foo> sh5;

    sh4 = sh2;

    sh5 = std::shared_ptr<Foo>(sh2);

    EXPECT_EQ(sh2.use_count(), 4);
    EXPECT_EQ(sh3.use_count(), 4);
    EXPECT_EQ(sh4.use_count(), 4);
    EXPECT_EQ(sh5.use_count(), 4);
  }

}

}  // End test namespace
}  // End peloton namespace
