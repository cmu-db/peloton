/**
* @brief Implementation of utility functions for join tests.
*
* Copyright(c) 2015, CMU
*/

#include "executor/join_tests_util.h"

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace test {
    
bool JoinTestsUtil::ExecuteJoinTest(
        storage::DataTable *leftTable,
        storage::DataTable *rightTable,
        storage::DataTable *expected,
        PelotonJoinType joinType,
        expression::AbstractExpression *predicate) {
    
    
    storage::DataTable *result; // FIXME
    
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



} // namespace test
} // namespace peloton
