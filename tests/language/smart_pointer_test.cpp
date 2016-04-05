//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// smart_pointer_test.cpp
//
// Identification: tests/language/smart_pointer_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <iostream>
#include <memory>

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SmartPointer Test
//===--------------------------------------------------------------------===//

struct Foo {
  Foo() { LOG_INFO("Foo..."); }
  ~Foo() { LOG_INFO("~Foo..."); }
};

TEST(SmartPointerTests, UniquePtr) {
  LOG_INFO("Creating new Foo...");
  std::unique_ptr<Foo> up(new Foo());

  LOG_INFO("About to release Foo...");
  Foo* fp = up.release();

  assert(up.get() == nullptr);
  LOG_INFO("Foo is no longer owned by unique_ptr...");

  LOG_INFO("Add a new Foo...");
  up.reset(new Foo());  // calls deleter for the old one

  LOG_INFO("Release and delete the owned Foo...");
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
    Foo* foo = new Foo();

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
