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

namespace peloton {

namespace storage {
class Database;
class DataTable;
}  // namespace storage

namespace benchmark {
namespace tpch {

class Configuration;

class TPCHDatabase {
 public:
  typedef std::unordered_map<std::string, uint32_t> Dictionary;

  TPCHDatabase(const Configuration &c);

  ~TPCHDatabase();

  enum class TableId : uint32_t {
    Part = 44,
    Supplier = 45,
    PartSupp = 46,
    Customer = 47,
    Nation = 48,
    Lineitem = 49,
    Region = 50,
    Orders = 51,
  };

  storage::Database &GetDatabase() const;

  // Table accessors
  storage::DataTable &GetTable(TableId table_id) const;

  // Dictionary accessors
  const Dictionary &GetShipinstructDict() const { return l_shipinstruct_dict_; }
  const Dictionary &GetShipmodeDict() const { return l_shipmode_dict_; }

  // Create all tables
  void CreateTables() const;

  // Load individual tables
  void LoadCustomerTable();
  void LoadLineitemTable();
  void LoadNationTable();
  void LoadOrdersTable();
  void LoadPartTable();
  void LoadPartSupplierTable();
  void LoadRegionTable();
  void LoadSupplierTable();

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
};

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton