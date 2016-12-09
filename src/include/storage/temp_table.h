
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// temp_table.h
//
// Identification: /peloton/src/include/storage/temp_table.h
//
// Copyright (c) 2015, 2016 Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {

//===--------------------------------------------------------------------===//
// TempTable
//===--------------------------------------------------------------------===//

/**
 * A TempTable is a non-thread-safe place to store tuples that don't
 * need to be durable, don't need indexes, don't need constraints.
 * This is designed to be faster than DataTable.
 */
class TempTable : public AbstractTable {
 public:
  virtual ~TempTable();

 protected:
  // Table constructor
  TempTable(const oid_t table_oid, catalog::Schema *schema,
            const bool own_schema);

  std::string GetName() const {
    std::ostringstream os;
    os << "TEMP_TABLE[" << table_oid << "]";
    return (os.str());
  }

  // Get a string representation for debugging
  inline const std::string GetInfo() const { this->GetInfo(); }

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  inline bool HasPrimaryKey() const {return (false)};

  inline bool HasUniqueConstraints() const {return (false)};

  inline bool HasForeignKeys() const {return (false)};

 private:
  // This is where we're actually going to store the data for this
  storage::TileGroup *data_;
};

}  // End storage namespace
}  // End peloton namespace
