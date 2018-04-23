#include <memory>
#include <math.h>
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"
#include "storage/tile_group_iterator.h"
#include "storage/tile_group.h"
#include "storage/dictionary_encoding_tile.h"

namespace peloton {
namespace test {
class CompressionSelectTest : public PelotonTest {};
TEST_F(CompressionSelectTest, BasicTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a t
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, name varchar(32));");
//  int i;
  std::string testTableName = "foo";
//  std::string s = "'", t = "'";
//  for (i = 0; i < 32; i++) {
//    s += "a";
//    t = s + "'";
//    std::string os =
//        "insert into foo values(" + std::to_string(i) + ", " + t + ");";
//    TestingSQLUtil::ExecuteSQLQuery(os);
//  }
//  EXPECT_EQ(i, 32);

	TestingSQLUtil::ExecuteSQLQuery("insert into foo values(1, 'taodai')");
	TestingSQLUtil::ExecuteSQLQuery("insert into foo values(2, 'bohan')");
	TestingSQLUtil::ExecuteSQLQuery("insert into foo values(3, 'siyuan')");
	txn_manager.CommitTransaction(txn);
	LOG_INFO("insert finish");
	txn = txn_manager.BeginTransaction();
//  auto database_object = catalog::DatabaseCatalog::GetInstance()->GetDatabaseObject(DEFAULT_DB_NAME, txn);
	auto dataTable_object = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, testTableName, txn);
	auto TGiterator = new storage::TileGroupIterator(dataTable_object);
	std::shared_ptr<storage::TileGroup> tg;
	while (TGiterator->HasNext()) {
		TGiterator->Next(tg);
		oid_t num_tile = tg->NumTiles();
		for (oid_t to = 0; to < num_tile; to++) {
			auto curr_tile = tg->GetTileReference(to);
			LOG_DEBUG("before curr_tile id: %d, isEncoded: %d, ptr: %p", curr_tile->GetTileId(), curr_tile->IsDictEncoded(), (void *) curr_tile.get());
			std::shared_ptr<storage::Tile> encoded_tile =
				std::make_shared<storage::DictEncodedTile>(BackendType::MM, nullptr, *(curr_tile->GetSchema()), nullptr,
				curr_tile->GetAllocatedTupleCount());
			if (!curr_tile->IsDictEncoded()) {
				encoded_tile->DictEncode(curr_tile.get());
				LOG_DEBUG("encoded_tile isEncoded: %d, ptr: %p", encoded_tile->IsDictEncoded(), (void*) encoded_tile.get());
				tg->SetTile(to, encoded_tile);
			}
			curr_tile = tg->GetTileReference(to);
			LOG_DEBUG("after curr_tile id: %d, isEncoded: %d, ptr: %p", curr_tile->GetTileId(), curr_tile->IsDictEncoded(), (void *) curr_tile.get());
		}
	}
	delete TGiterator;

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;

//  int rows_affected;
  std::ostringstream os;
  txn_manager.CommitTransaction(txn);

  os << "select * from foo;";
  std::vector<std::string> expected = {"1|taodai", "2|bohan", "3|siyuan"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(os.str(), expected, false);

  txn = txn_manager.BeginTransaction();

  dataTable_object = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, testTableName, txn);
  TGiterator = new storage::TileGroupIterator(dataTable_object);
  while (TGiterator->HasNext()) {
		TGiterator->Next(tg);
		oid_t num_tile = tg->NumTiles();
		for (oid_t to = 0; to < num_tile; to++) {
			auto curr_tile = tg->GetTileReference(to);
			LOG_DEBUG("curr_tile id: %d, isEncoded: %d", curr_tile->GetTileId(), curr_tile->IsDictEncoded());
		}
	}

  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
