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
#include "backend/storage/tile_iterator.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Tests
//===--------------------------------------------------------------------===//

TEST(TileTests, BasicTest) {

	std::vector<catalog::ColumnInfo> columns;

	catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
	catalog::ColumnInfo column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", true);
	catalog::ColumnInfo column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", true);
	//catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, "D", false);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);
	//columns.push_back(column4);

	catalog::Schema *schema = new catalog::Schema(columns);

	std::vector<std::string> column_names;

	column_names.push_back("COL 1");
	column_names.push_back("COL 2");
	column_names.push_back("COL 3");
	//column_names.push_back("COL 4");

	const int tuple_count = 6;

	storage::AbstractBackend *backend = new storage::VMBackend();
	storage::TileGroupHeader *header = new storage::TileGroupHeader(backend, tuple_count);

	storage::Tile *tile = storage::TileFactory::GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
													  	  header, backend, *schema, nullptr, tuple_count);


	std::cout << "tile size: " << tile->GetInlinedSize() << std::endl;
	std::cout << "allocated tuple count: " << tile->GetAllocatedTupleCount() << std::endl;


	storage::Tuple *tuple1 = new storage::Tuple(schema, true);
	storage::Tuple *tuple2 = new storage::Tuple(schema, true);

	tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
	//tuple1->SetValue(3, ValueFactory::GetStringValue("tuple 1", tile->GetPool()));

	tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
	//tuple2->SetValue(3, ValueFactory::GetStringValue("tuple 2", tile->GetPool()));


	tile->InsertTuple(0, tuple1);
	tile->InsertTuple(1, tuple2);
	tile->InsertTuple(2, tuple2);

	std::cout << (*tile);

	//tile->InsertTuple(2, tuple1);

	//std::cout << (*tile);

	/*
	 * Trying to create a copy of a tile when all the data is inlined
	 */
	const catalog::Schema *old_schema;
	bool tile_is_inlined;
	uint16_t tile_allocated_tuple_count, tile_active_tuple_count;
	uint32_t old_tile_size;
	uint32_t old_tile_tuple_size;

	std::cout << "base address of old tile: " << static_cast<void *>(tile) << std::endl;

	old_tile_size = tile->GetInlinedSize();
	std::cout << "old tile size: " << old_tile_size << std:: endl;

	tile_allocated_tuple_count = tile->GetAllocatedTupleCount();
	std::cout << "old tile allocated tuple count: " << tile_allocated_tuple_count << std:: endl;

	tile_active_tuple_count = tile->GetActiveTupleCount();
	std::cout << "old tile active tuple count: " << tile_active_tuple_count << std:: endl;

	old_tile_tuple_size = old_tile_size / tile_allocated_tuple_count;
	std::cout << "size of each tuple: " << old_tile_tuple_size << std::endl;

	old_schema = tile->GetSchema();
	tile_is_inlined = old_schema->IsInlined();
	std::cout << "Is the entire tile inlined? " << tile_is_inlined << std::endl;


	storage::TileGroupHeader *new_header = new storage::TileGroupHeader(backend, tuple_count);
	storage::Tile *new_tile = storage::TileFactory::GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
											  new_header, backend, *old_schema, nullptr, tile_allocated_tuple_count);

	std::cout << "base address of new tile: " << static_cast<void *>(new_tile) << std::endl;

	uint32_t new_tile_size = new_tile->GetInlinedSize();
	std::cout << "new tile size: " << new_tile_size << std:: endl;

	::memcpy(static_cast<void *>(new_tile), static_cast<void *>(tile), old_tile_size);

	std::cout << (*new_tile);

	/*
	 * Iterate over the tile and print the tuples
	 */
	storage::TileIterator tile_itr(new_tile);
	storage::Tuple *new_tuple = new storage::Tuple(old_schema, true);
	//storage::Tuple& new_tuple_ref = *new_tuple;

/*
	while(tile_itr.Next(new_tuple_ref) == true)
	{
		std::cout << (new_tuple_ref);
		tile_itr++;
	}




	if(!tile_is_inlined)
	{
		int col_cnt = tile->GetColumnCount();
		for(int col_itr=0; col_itr<col_cnt; col_itr++)
		{

		}
	}
*/



	delete tuple1;
	delete tuple2;
	delete tile;
	delete header;
	delete schema;
	delete backend;
}

} // End test namespace
} // End peloton namespace


