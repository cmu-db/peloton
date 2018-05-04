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
  std::string testTableName = "foo";

	TestingSQLUtil::ExecuteSQLQuery("insert into foo values(1, 'taodai')");
	TestingSQLUtil::ExecuteSQLQuery("insert into foo values(2, 'bohan')");
	TestingSQLUtil::ExecuteSQLQuery("insert into foo values(3, 'siyuan')");
	txn_manager.CommitTransaction(txn);
	LOG_INFO("insert finish");
	txn = txn_manager.BeginTransaction();
	auto dataTable_object = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, testTableName, txn);
	auto TGiterator = new storage::TileGroupIterator(dataTable_object);
	std::shared_ptr<storage::TileGroup> tg;

	while (TGiterator->HasNext()) {
		TGiterator->Next(tg);
		if (!tg->IsDictEncoded()) {
			LOG_INFO("tile group not encoded. Encode now...");
			tg->DictEncode();
		}
	}
	delete TGiterator;

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;

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
