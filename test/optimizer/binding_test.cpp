//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binding_test.cpp
//
// Identification: tests/optimizer/binding_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#define private public

#include "optimizer/optimizer.h"
#include "optimizer/binding.h"
#include "optimizer/op_expression.h"
#include "optimizer/operators.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class BindingTests : public PelotonTest {};

TEST_F(BindingTests, SimpleMatchTest) {
  Optimizer optimizer;

  // Make groups to match against
  std::vector<Group> &groups = optimizer.memo.Groups();

  GroupID join_id;
  GroupID root_group_id;
  {
    Group right_group;
    right_group.add_item(LogicalGet::make(0, {0, 1}));
    groups.push_back(right_group);
    GroupID right_id = groups.size() - 1;

    Group left_group;
    left_group.add_item(LogicalGet::make(1, {0}));
    groups.push_back(left_group);
    GroupID left_id = groups.size() - 1;

    Group join_group;
    join_group.add_item(LogicalInnerJoin::make(left_id, right_id));
    groups.push_back(join_group);
    join_id = groups.size() - 1;

    Group root_group;
    root_group.add_item(LogicalProject::make(join_id));
    groups.push_back(root_group);
    root_group_id = groups.size() - 1;
  }

  // Make pattern to detect
  auto right_relation = std::make_shared<Pattern>(OpType::Get);
  auto left_relation = std::make_shared<Pattern>(OpType::Get);
  auto join = std::make_shared<Pattern>(OpType::InnerJoin);
  auto root = std::make_shared<Pattern>(OpType::Project);

  join->AddChild(left_relation);
  root->AddChild(join);

  {
    GroupBindingIterator iter(optimizer, root_group_id, root);

    EXPECT_FALSE(iter.HasNext());
  }

  join->AddChild(right_relation);

  {
    GroupBindingIterator iter(optimizer, root_group_id, root);

    EXPECT_TRUE(iter.HasNext());
    std::shared_ptr<OpExpression> binding = iter.Next();
    EXPECT_EQ(binding->Op().type(), OpType::Project);
    EXPECT_EQ(binding->Children().size(), 1);

    EXPECT_FALSE(iter.HasNext());
  }

  groups[root_group_id].add_item(LogicalProject::make(join_id));

  {
    GroupBindingIterator iter(optimizer, root_group_id, root);

    EXPECT_TRUE(iter.HasNext());
    std::shared_ptr<OpExpression> binding = iter.Next();
    EXPECT_EQ(binding->Op().type(), OpType::Project);
    EXPECT_EQ(binding->Children().size(), 1);

    EXPECT_TRUE(iter.HasNext());
    binding = iter.Next();
    EXPECT_EQ(binding->Op().type(), OpType::Project);
    EXPECT_EQ(binding->Children().size(), 1);

    EXPECT_FALSE(iter.HasNext());
  }
}

} /* namespace test */
} /* namespace peloton */
