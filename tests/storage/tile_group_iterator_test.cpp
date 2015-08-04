/*-------------------------------------------------------------------------
*
* tile_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tile_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_iterator.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TileGroupIterator Tests
//===--------------------------------------------------------------------===//

TEST(TileGroupIteratorTests, BasicTest) {

    // Columns
    std::vector<catalog::Column> columns;
    catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
    columns.push_back(column1);

    // Column Names
    std::vector<std::string> column_names;
    column_names.push_back("COL 1");

    // Schema
    catalog::Schema *schema = new catalog::Schema(columns);

    // Allocated Tuple Count
    const int tuple_count = 6;

    storage::AbstractBackend *backend = new storage::VMBackend();
    storage::TileGroupHeader *header = new storage::TileGroupHeader(backend, tuple_count);

    storage::Tile *tile = storage::TileFactory::GetTile(
                              INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID, header, backend,
                              *schema, nullptr, tuple_count);

    for (int i = 0; i < tuple_count; i++) {
        storage::Tuple *tuple = new storage::Tuple(schema, true);
        tuple->SetValue(0, ValueFactory::GetIntegerValue(i));
        tile->InsertTuple(0, tuple);
    } // FOR

    
//     delete tuple;
    delete tile;
    delete header;
    delete schema;
    delete backend;
}

}  // End test namespace
}  // End peloton namespace
