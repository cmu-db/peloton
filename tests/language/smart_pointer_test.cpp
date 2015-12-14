//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// smart_pointer_test.cpp
//
// Identification: tests/common/feature_test.cpp
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
// SmartPointer Test
//===--------------------------------------------------------------------===//

struct Foo {
    Foo() { std::cout << "Foo...\n"; }
    ~Foo() { std::cout << "~Foo...\n"; }
};

TEST(SmartPointerTests, UniquePtr) {

    std::cout << "Creating new Foo...\n";
    std::unique_ptr<Foo> up(new Foo());

    std::cout << "About to release Foo...\n";
    Foo* fp = up.release();

    assert (up.get() == nullptr);
    std::cout << "Foo is no longer owned by unique_ptr...\n";

    std::cout << "Add a new Foo...\n";
    up.reset(new Foo());  // calls deleter for the old one

    std::cout << "Release and delete the owned Foo...\n";
    up.reset(nullptr);

    EXPECT_EQ(up.get(), nullptr);
    delete fp;

}

TEST(SmartPointerTests, SharedPtr) {

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
