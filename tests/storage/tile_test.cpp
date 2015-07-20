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
	catalog::ColumnInfo column4(VALUE_TYPE_VARCHAR, 25, "D", false);
	catalog::ColumnInfo column5(VALUE_TYPE_VARCHAR, 25, "E", false);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);
	columns.push_back(column4);
	columns.push_back(column4);

	catalog::Schema *schema = new catalog::Schema(columns);

	std::vector<std::string> column_names;

	column_names.push_back("COL 1");
	column_names.push_back("COL 2");
	column_names.push_back("COL 3");
	column_names.push_back("COL 4");
	column_names.push_back("COL 5");

	const int tuple_count = 6;

	storage::AbstractBackend *backend = new storage::VMBackend();
	storage::TileGroupHeader *header = new storage::TileGroupHeader(backend, tuple_count);

	storage::Tile *tile = storage::TileFactory::GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
													  	  header, backend, *schema, nullptr, tuple_count);


	//std::cout << "tile size: " << tile->GetInlinedSize() << std::endl;
	//std::cout << "allocated tuple count: " << tile->GetAllocatedTupleCount() << std::endl;


	storage::Tuple *tuple1 = new storage::Tuple(schema, true);
	storage::Tuple *tuple2 = new storage::Tuple(schema, true);
	storage::Tuple *tuple3 = new storage::Tuple(schema, true);

	tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
	tuple1->SetValue(3, ValueFactory::GetStringValue("vivek sengupta", tile->GetPool()));
	tuple1->SetValue(4, ValueFactory::GetStringValue("vivek sengupta again", tile->GetPool()));

	tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
	tuple2->SetValue(3, ValueFactory::GetStringValue("ming fang", tile->GetPool()));
	tuple2->SetValue(4, ValueFactory::GetStringValue("ming fang again", tile->GetPool()));

	tuple3->SetValue(0, ValueFactory::GetIntegerValue(3));
	tuple3->SetValue(1, ValueFactory::GetIntegerValue(3));
	tuple3->SetValue(2, ValueFactory::GetTinyIntValue(3));
	tuple3->SetValue(3, ValueFactory::GetStringValue("jinwoong kim", tile->GetPool()));
	tuple3->SetValue(4, ValueFactory::GetStringValue("jinwoong kim again", tile->GetPool()));

	tile->InsertTuple(0, tuple1);
	tile->InsertTuple(1, tuple2);
	tile->InsertTuple(2, tuple3);

	//std::cout << (*tile);

	//tile->InsertTuple(2, tuple1);

	//std::cout << (*tile);


	/*
	 * Print details of old tile
	 */

	const catalog::Schema *old_schema;
	bool old_tile_is_inlined;
	uint16_t old_tile_allocated_tuple_count, old_tile_active_tuple_count;
	uint32_t old_tile_size;
	uint32_t old_tile_tuple_size;


	std::cout << "-------------------" << std::endl;
	std::cout << "Details of old tile" << std::endl;
	std::cout << "-------------------" << std::endl;

	std::cout << "base address of old tile: " << static_cast<void *>(tile) << std::endl;

	old_tile_size = tile->GetInlinedSize();
	std::cout << "old tile size: " << old_tile_size << std:: endl;

	old_tile_allocated_tuple_count = tile->GetAllocatedTupleCount();
	std::cout << "old tile allocated tuple count: " << old_tile_allocated_tuple_count << std:: endl;

	old_tile_active_tuple_count = tile->GetActiveTupleCount();
	std::cout << "old tile active tuple count: " << old_tile_active_tuple_count << std:: endl;
	old_tile_active_tuple_count = 3;

	old_tile_tuple_size = old_tile_size / old_tile_allocated_tuple_count;
	std::cout << "size of each tuple in old tile: " << old_tile_tuple_size << std::endl;

	old_schema = tile->GetSchema();
	old_tile_is_inlined = old_schema->IsInlined();
	std::cout << "Is the entire old tile inlined? " << (old_tile_is_inlined? "true" : "false") << std::endl;

	Pool *old_pool = tile->GetPool();
	std::cout << "Pool associated with old tile is at address: " << static_cast<void *>(old_pool) << std::endl;

	int64_t old_pool_size = old_pool->GetAllocatedMemory();
	std::cout << "size of old pool: " << old_pool_size << std::endl;


	/*
	 * Create a new tile from the old tile
	 */
	std::vector<catalog::ColumnInfo> new_columns = old_schema->GetColumns();
	const catalog::Schema *new_schema = new catalog::Schema(columns);

	storage::AbstractBackend *new_backend = new storage::VMBackend();
	storage::TileGroupHeader *new_header = new storage::TileGroupHeader(new_backend, tuple_count);
	storage::Tile *new_tile = storage::TileFactory::GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
											  new_header, new_backend, *new_schema, nullptr, old_tile_allocated_tuple_count);
	Pool *new_pool = new_tile->GetPool();

	//::memcpy(new_pool,old_pool,old_pool_size);
	//::memcpy(static_cast<void *>(new_tile), static_cast<void *>(tile), old_tile_size);

	storage::Tuple *old_tuple;

	for(int old_tup_itr=0; old_tup_itr<old_tile_active_tuple_count; old_tup_itr++)
	{
		old_tuple = tile->GetTuple(old_tup_itr);
		new_tile->InsertTuple(old_tup_itr,old_tuple);
	}



	/*
	 * Print details of new tile
	 */


	bool new_tile_is_inlined;
	uint16_t new_tile_allocated_tuple_count, new_tile_active_tuple_count;
	uint32_t new_tile_size;
	uint32_t new_tile_tuple_size;


	std::cout << "-------------------" << std::endl;
	std::cout << "Details of new tile" << std::endl;
	std::cout << "-------------------" << std::endl;

	std::cout << "base address of new tile: " << static_cast<void *>(new_tile) << std::endl;

	new_tile_size = new_tile->GetInlinedSize();
	std::cout << "new tile size: " << new_tile_size << std:: endl;

	new_tile_allocated_tuple_count = tile->GetAllocatedTupleCount();
	std::cout << "new tile allocated tuple count: " << new_tile_allocated_tuple_count << std:: endl;

	new_tile_active_tuple_count = tile->GetActiveTupleCount();
	std::cout << "new tile active tuple count: " << new_tile_active_tuple_count << std:: endl;
	new_tile_active_tuple_count = 3;

	new_tile_tuple_size = new_tile_size / new_tile_allocated_tuple_count;
	std::cout << "size of each tuple in new tile: " << new_tile_tuple_size << std::endl;

	new_schema = new_tile->GetSchema();
	new_tile_is_inlined = new_schema->IsInlined();
	std::cout << "Is the entire new tile inlined? " << (new_tile_is_inlined? "true" : "false") << std::endl;

	std::cout << "Pool associated with new tile is at address: " << static_cast<void *>(new_pool) << std::endl;

	int64_t new_pool_size = new_pool->GetAllocatedMemory();
	std::cout << "size of new pool: " << new_pool_size << std::endl;

	std::cout << "------------------------------------" << std:: endl;
	std::cout << "In case some columns are not inlined" << std:: endl;
	std::cout << "------------------------------------" << std:: endl;

	//std::cout << (*new_tile);

	/*
	 * Iterate over the tile and print the tuples
	 */
	//storage::TileIterator tile_itr(new_tile);
	//storage::Tuple *new_tuple = new storage::Tuple(old_schema, true);
	//storage::Tuple& new_tuple_ref = *new_tuple;

/*
	while(tile_itr.Next(new_tuple_ref) == true)
	{
		std::cout << (new_tuple_ref);
		tile_itr++;
	}
*/



	if(!new_tile_is_inlined)
	{
		//int col_cnt = old_schema->GetColumnCount();
		//std::cout << "total column count: " << col_cnt << std::endl;

		int uninlined_col_cnt = new_schema->GetUninlinedColumnCount();
		//std::cout << "uninlined column count: " << uninlined_col_cnt << std::endl;

		int uninlined_col_index;
		Value uninlined_col_value, new_uninlined_col_value;
		storage::Tuple *new_tuple;

		for(int col_itr=0; col_itr<uninlined_col_cnt; col_itr++) {

			uninlined_col_index = new_schema->GetUninlinedColumnIndex(col_itr);
			std::cout << "----------------------------------" << std::endl;
			std::cout << "index of next uninlined column: " << uninlined_col_index << std::endl;
			std::cout << "----------------------------------" << std::endl  << std::endl;

			for(int tup_itr=0; tup_itr<new_tile_active_tuple_count; tup_itr++) {

				std::cout << "\t -----------------------------------------------------" + tup_itr << std::endl;
				std::cout << "\t Before associating new pool to new data for tuple: " + tup_itr << std::endl;
				std::cout << "\t -----------------------------------------------------" + tup_itr << std::endl;

				new_tuple = new_tile->GetTuple(tup_itr);
				std::cout << "\t address of new_tuple: " << static_cast<void *>(new_tuple) << std::endl << std::endl;

				uninlined_col_value = new_tile->GetValue(tup_itr,uninlined_col_index);
				std::cout << "\t uninlined column value: " << uninlined_col_value << std::endl;

				size_t uninlined_col_object_len = ValuePeeker::PeekObjectLength(uninlined_col_value);
				std::cout << "\t size of uninlined column object: " << uninlined_col_object_len << std::endl;

				unsigned char* uninlined_col_object_ptr = static_cast<unsigned char *>(ValuePeeker::PeekObjectValue(uninlined_col_value));
				std::cout << "\t uninlined column object pointer: " << static_cast<void *>(uninlined_col_object_ptr) << std::endl;

				std::string uninlined_varchar_str(reinterpret_cast<char const*>(uninlined_col_object_ptr), uninlined_col_object_len);
				std::cout << "\t the data stored uninlined is: " << uninlined_varchar_str << std::endl << std::endl;

				//char *xyz = new_tuple->GetData();
				//tile->GetValue(tup_itr,next_uninlined_col_index);

				// std::string copy_uninlined_varchar_str;
				// Create a new Value object with the uninlined string w.r.t. the new pool
				Value new_val = ValueFactory::GetStringValue(uninlined_varchar_str, new_pool);
				// Set the newly created value object to the tuple
				// new_tuple->SetValue(uninlined_col_index, uninlined_col_value);
				new_tile->SetValue(new_val, tup_itr, uninlined_col_index);


				std::cout << "\t ---------------------------------------------" + tup_itr << std::endl;
				std::cout << "\t Associated new pool to new data for tuple: " + tup_itr << std::endl;
				std::cout << "\t ---------------------------------------------" + tup_itr << std::endl;

				new_uninlined_col_value = new_tile->GetValue(tup_itr,uninlined_col_index);
				std::cout << "\t new uninlined column value: " << new_uninlined_col_value << std::endl;

				size_t new_uninlined_col_object_len = ValuePeeker::PeekObjectLength(new_uninlined_col_value);
				std::cout << "\t size of uninlined column object: " << new_uninlined_col_object_len << std::endl;

				unsigned char* new_uninlined_col_object_ptr = static_cast<unsigned char *>(ValuePeeker::PeekObjectValue(new_uninlined_col_value));
				std::cout << "\t new uninlined column object pointer: " << static_cast<void *>(new_uninlined_col_object_ptr) << std::endl;

				std::string new_uninlined_varchar_str( reinterpret_cast<char const*>(new_uninlined_col_object_ptr), uninlined_col_object_len);
				std::cout << "\t the new data stored uninlined is: " << new_uninlined_varchar_str << std::endl << std::endl;
			}
		}

		delete new_tuple;
	}

	if(tile==new_tile)
			std::cout << "old and new tiles are the same ..." << std::endl;

	std::string str1 = "hello world";
	std::string str2 = str1;

	std::cout << "add of str1: " << &str1 << std::endl;
	std::cout << "add of str2: " << &str2 << std::endl;




	delete tuple1;
	delete tuple2;
	delete tuple3;
	delete tile;
	delete header;
	delete schema;
	delete backend;
}

} // End test namespace
} // End peloton namespace


