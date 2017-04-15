#include <algorithm>
#include "common/harness.h"

#define private public

#include "optimizer/properties.h"


namespace peloton {

namespace test {


using namespace optimizer;

class PropertyTests : public PelotonTest {};

// TODO: Test complex expressions
TEST_F(PropertyTests, PropertyColHashAndEqualTest){
  const size_t num_exprs = 100;
  std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
  for (size_t i = 0; i<num_exprs; i++) {
    auto tv_expr = std::make_shared<expression::TupleValueExpression>(std::to_string(i));
    auto oids = std::make_tuple<oid_t, oid_t ,oid_t>(0,0,i);
    tv_expr->SetBoundOid(oids);
    cols.push_back(tv_expr);
  }

  const size_t num_iter = 1000;
  PropertySet l_set;
  l_set.AddProperty(std::shared_ptr<PropertyColumns>(
      new PropertyColumns(cols)));

  for (size_t i = 0; i<num_iter; i++) {
    PropertySet r_set;
    std::random_shuffle(cols.begin(), cols.end());
    r_set.AddProperty(std::shared_ptr<PropertyColumns>(
        new PropertyColumns(cols)));

    EXPECT_EQ(l_set.Hash(), r_set.Hash());
    EXPECT_TRUE(l_set == r_set);
  }
}

} /* namespace test */
} /* namespace peloton */
