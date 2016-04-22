//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.cpp
//
// Identification: tests/concurrency/transaction_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//



namespace peloton {
  namespace test {
  storage::DataTable *TransactionTestsUtil::CreateTable(int num_key,
                                                      std::string table_name,
                                                      oid_t database_id,
                                                      oid_t relation_id,
                                                      oid_t index_oid,
                                                      bool need_primary_index) {
  auto id_column = catalog::Column(VALUE_TYPE_INTEGER,
                                   GetTypeSize(VALUE_TYPE_INTEGER), "id", true);
  auto value_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value", true);

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});

  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      database_id, relation_id, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create index on the id column
  std::vector<oid_t> key_attrs = {0};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_metadata = new index::IndexMetadata(
      "primary_btree_index", index_oid, INDEX_TYPE_BTREE,
      need_primary_index ? INDEX_CONSTRAINT_TYPE_PRIMARY_KEY : INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(pkey_index);

  // add this table to current database
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_id);
  if (db != nullptr) {
    db->AddTable(table);
  }

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < num_key; i++) {
    ExecuteInsert(txn, table, i, 0);
  }
  txn_manager.CommitTransaction();

  return table;
}


  }
}