
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
 *
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

  virtual bool HasPrimaryKey() const = 0;

  virtual bool HasUniqueConstraints() const = 0;

  virtual bool HasForeignKeys() const = 0;
};

}  // End storage namespace
}  // End peloton namespace
