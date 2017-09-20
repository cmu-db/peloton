//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_database.h
//
// Identification: src/include/benchmark/tpch/tpch_database.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <unordered_map>

#include "benchmark/tpch/tpch_configuration.h"

namespace peloton {

namespace storage {
class Database;
class DataTable;
}  // namespace storage

namespace benchmark {
namespace tpch {

//===----------------------------------------------------------------------===//
// The TPCH Database. This class is responsible for access to all table in the
// DB. Tables are created on instantiation. Individual tables can be loaded
// from files based on the benchmark configuration parameters. Loading supports
// dictionary encoding string values.
//===----------------------------------------------------------------------===//
class TPCHDatabase {
 public:
  typedef std::unordered_map<std::string, uint32_t> Dictionary;

  TPCHDatabase(const Configuration &c);

  ~TPCHDatabase();

  storage::Database &GetDatabase() const;

  // Table accessors
  storage::DataTable &GetTable(TableId table_id) const;

  // Create all tables
  void CreateTables() const;

  void LoadTable(TableId table_id);

  // Load individual tables
  void LoadCustomerTable();
  void LoadLineitemTable();
  void LoadNationTable();
  void LoadOrdersTable();
  void LoadPartTable();
  void LoadPartSupplierTable();
  void LoadRegionTable();
  void LoadSupplierTable();

  uint32_t CodeForMktSegment(const std::string mktsegment) const;

 private:
  uint32_t DictionaryEncode(Dictionary &dict, const std::string &val);

  // Table creators
  void CreateCustomerTable() const;
  void CreateLineitemTable() const;
  void CreateNationTable() const;
  void CreateOrdersTable() const;
  void CreatePartTable() const;
  void CreatePartSupplierTable() const;
  void CreateRegionTable() const;
  void CreateSupplierTable() const;

  // Has the given table been loaded already?
  bool TableIsLoaded(TableId table_id) const {
    return loaded_tables_[static_cast<uint32_t>(table_id) -
                          static_cast<uint32_t>(TableId::Part)];
  }

  void SetTableIsLoaded(TableId table_id) {
    loaded_tables_[static_cast<uint32_t>(table_id) -
                   static_cast<uint32_t>(TableId::Part)] = true;
  }

 private:
  // The configuration
  const Configuration &config_;

  // Track which tables have been loaded
  bool loaded_tables_[8];

  // Dictionary codes
  Dictionary l_shipinstruct_dict_;
  Dictionary l_shipmode_dict_;
  Dictionary p_brand_dict_;
  Dictionary p_container_dict_;
  Dictionary c_mktsegment_dict_;
};

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton
