//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// join_tests_util.cpp
//
// Identification: tests/executor/join_tests_util.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/join_tests_util.h"

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace test {

bool JoinTestsUtil::ExecuteJoinTest(storage::DataTable *leftTable,
                                    storage::DataTable *rightTable,
                                    storage::DataTable *expected,
                                    PelotonJoinType joinType,
                                    expression::AbstractExpression *predicate) {
  storage::DataTable *result;  // FIXME

  // Do a tuple-by-tuple comparison
  //     TupleIterator leftItr(&tile);
  //     Tuple leftTuple(&tile.schema);
  //     TupleIterator rightItr(&tile);
  //     Tuple rightTuple(&tile.schema);
  //     while (tile_itr.Next(tuple)) {
  //
  //     }

  return (true);
}

}  // namespace test
}  // namespace peloton
