//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema.h
//
// Identification: src/include/catalog/schema.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "catalog/column.h"
#include "catalog/constraint.h"
#include "common/printable.h"
#include "type/type.h"
#include "boost/algorithm/string.hpp"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//

class Schema : public Printable {
 public:
  //===--------------------------------------------------------------------===//
  // Static factory methods to construct schema objects
  //===--------------------------------------------------------------------===//

  // Construct schema
  void CreateTupleSchema(const std::vector<type::TypeId> &column_types,
                         const std::vector<oid_t> &column_lengths,
                         const std::vector<std::string> &column_names,
                         const std::vector<bool> &is_inlined);

  // Construct schema from vector of Column
  Schema(const std::vector<Column> &columns);

  // Copy schema
  static std::shared_ptr<const Schema> CopySchema(
      const std::shared_ptr<const Schema> &schema);

  // Copy subset of columns in the given schema
  static std::shared_ptr<const Schema> CopySchema(
      const std::shared_ptr<const Schema> &schema,
      const std::vector<oid_t> &set);

  // Backward compatible for raw pointers
  // Copy schema
  static Schema *CopySchema(const Schema *schema);

  // Copy subset of columns in the given schema
  static Schema *CopySchema(const Schema *schema,
                            const std::vector<oid_t> &index_list);

  static Schema *FilterSchema(const Schema *schema,
                              const std::vector<oid_t> &set);

  // Append two schema objects
  static Schema *AppendSchema(const Schema *first, const Schema *second);

  // Append subset of columns in the two given schemas
  static Schema *AppendSchema(const Schema *first,
                              std::vector<oid_t> &first_set,
                              const Schema *second,
                              std::vector<oid_t> &second_set);

  // Append given schemas.
  static Schema *AppendSchemaList(std::vector<Schema> &schema_list);

  // Append given schemas.
  static Schema *AppendSchemaPtrList(
      const std::vector<const Schema *> &schema_list);

  // Append subsets of columns in the given schemas.
  static Schema *AppendSchemaPtrList(
      const std::vector<const Schema *> &schema_list,
      const std::vector<std::vector<oid_t>> &subsets);

  // Compare two schemas
  hash_t Hash() const;
  bool operator==(const Schema &other) const;
  bool operator!=(const Schema &other) const;

  //===--------------------------------------------------------------------===//
  // Schema accessors
  //===--------------------------------------------------------------------===//

  inline size_t GetOffset(const oid_t column_id) const {
    return columns_[column_id].GetOffset();
  }

  inline type::TypeId GetType(const oid_t column_id) const {
    return columns_[column_id].GetType();
  }

  // Return appropriate length based on whether column is inlined
  inline size_t GetAppropriateLength(const oid_t column_id) const {
    auto is_inlined = columns_[column_id].IsInlined();
    size_t column_length;

    if (is_inlined) {
      column_length = GetLength(column_id);
    } else {
      column_length = GetVariableLength(column_id);
    }

    return column_length;
  }

  // Returns fixed length
  inline size_t GetLength(const oid_t column_id) const {
    return columns_[column_id].GetLength();
  }

  inline size_t GetVariableLength(const oid_t column_id) const {
    return columns_[column_id].GetVariableLength();
  }

  inline bool IsInlined(const oid_t column_id) const {
    return columns_[column_id].IsInlined();
  }

  inline const Column &GetColumn(const oid_t column_id) const {
    return columns_[column_id];
  }

  /**
   * For the given column name, return its offset in this table.
   * If the column does not exist, it will return INVALID_OID
   * @param col_name
   * @return
   */
  inline oid_t GetColumnID(std::string col_name) const {
    oid_t index = INVALID_OID;
    for (oid_t i = 0, cnt = columns_.size(); i < cnt; ++i) {
      if (columns_[i].GetName() == col_name) {
        index = i;
        break;
      }
    }
    return index;
  }

  inline oid_t GetUninlinedColumn(const oid_t column_id) const {
    return uninlined_columns_[column_id];
  }

  inline const std::vector<Column> &GetColumns() const { return columns_; }

  // Return the number of columns in the schema for the tuple.
  inline size_t GetColumnCount() const { return column_count_; }

  inline oid_t GetUninlinedColumnCount() const {
    return uninlined_column_count_;
  }

  // Return the number of bytes used by one tuple.
  inline oid_t GetLength() const { return length_; }

  // Returns a flag indicating whether all columns are inlined
  inline bool IsInlined() const { return tuple_is_inlined_; }

  inline void SetIndexedColumns(const std::vector<oid_t> &indexed_columns) {
    indexed_columns_ = indexed_columns;
  }

  inline const std::vector<oid_t> GetIndexedColumns() const {
    return indexed_columns_;
  }

  //===--------------------------------------------------------------------===//
  // Single column constraint accessors
  //===--------------------------------------------------------------------===//

  // Get the nullability of the column at a given index.
  inline bool AllowNull(const oid_t column_id) const {
    if (columns_[column_id].IsNotNull()) {
      return false;
    }
    return true;
  }

  // Set the not null for the column
  inline void SetNotNull(const oid_t column_id) {
    columns_[column_id].SetNotNull();
    not_null_columns_.push_back(column_id);
  }

  // Drop the not null for the column
  inline void DropNotNull(const oid_t column_id) {
    columns_[column_id].ClearNotNull();
    for (auto itr = not_null_columns_.begin(); itr < not_null_columns_.end(); itr++) {
      if (*itr == column_id) {
        not_null_columns_.erase(itr);
        break;
      }
    }
  }

  // Get not null column list
  inline std::vector<oid_t> GetNotNullColumns() const {
    return not_null_columns_;
  }

  // For single column default
  inline bool AllowDefault(const oid_t column_id) const {
    return columns_[column_id].HasDefault();
  }
  // Get the default value for the column
  inline type::Value* GetDefaultValue(const oid_t column_id) const {
    if (columns_[column_id].HasDefault()) {
      return columns_[column_id].GetDefaultValue().get();
    }
    return nullptr;
  }

  // Set the default value for the column
  inline void SetDefaultValue(const oid_t column_id,
                              const type::Value &default_value) {
    if (columns_[column_id].HasDefault()) {
      columns_[column_id].ClearDefaultValue();
    }
    columns_[column_id].SetDefaultValue(default_value);
  }

  // Drop the default value for the column
  inline void DropDefaultValue(const oid_t column_id) {
    if (columns_[column_id].HasDefault()) {
      columns_[column_id].ClearDefaultValue();
    }
  }

  //===--------------------------------------------------------------------===//
  // Multi-column constraint accessors
  //===--------------------------------------------------------------------===//

  // Add a constraint for the table
  inline void AddConstraint(const std::shared_ptr<Constraint> constraint) {
    constraints[constraint->GetConstraintOid()] = constraint;

    if (constraint->GetType() == ConstraintType::PRIMARY) {
      has_primary_key_ = true;
    } else if (constraint->GetType() == ConstraintType::UNIQUE) {
      unique_constraint_count_++;
    } else if (constraint->GetType() == ConstraintType::FOREIGN) {
      fk_constraints_.push_back(constraint->GetConstraintOid());
    }
  }

  // Delete a constraint by id from the table
  inline void DropConstraint(oid_t constraint_oid) {
    if (constraints[constraint_oid]->GetType() == ConstraintType::PRIMARY) {
      has_primary_key_ = false;
    } else if (constraints[constraint_oid]->GetType() == ConstraintType::UNIQUE) {
      unique_constraint_count_--;
    } else if (constraints[constraint_oid]->GetType() == ConstraintType::FOREIGN) {
      for (auto itr = fk_constraints_.begin(); itr < fk_constraints_.end(); itr++) {
        if (*itr == constraint_oid) {
          fk_constraints_.erase(itr);
          break;
        }
      }
    }

    constraints.erase(constraint_oid);
  }

  inline std::unordered_map<oid_t, std::shared_ptr<Constraint>> GetConstraints() const {
    return constraints;
  }

  inline std::shared_ptr<Constraint> GetConstraint(oid_t constraint_oid) const {
    return constraints.at(constraint_oid);
  }

  // For primary key constraints
  inline bool HasPrimary() { return has_primary_key_; }

  // For unique constraints
  inline bool HasUniqueConstraints() const { return (unique_constraint_count_ > 0); }

  // For foreign key constraints
  inline std::vector<std::shared_ptr<Constraint>> GetForeignKeyConstraints() {
    std::vector<std::shared_ptr<Constraint>> fks;
    for (auto oid : fk_constraints_) {
      PELOTON_ASSERT(constraints[oid]->GetType() == ConstraintType::FOREIGN);
      fks.push_back(constraints[oid]);
    }
    return fks;
  }

  inline bool HasForeignKeys() const { return (fk_constraints_.size() > 0); }

  inline void RegisterForeignKeySource(const std::shared_ptr<Constraint> constraint) {
    fk_sources_.push_back(constraint);
  }

  inline void DeleteForeignKeySource(const oid_t constraint_oid) {
    for (auto itr = fk_sources_.begin(); itr < fk_sources_.end(); itr++) {
      if ((*itr)->GetConstraintOid() == constraint_oid) {
        fk_sources_.erase(itr);
        break;
      }
    }
  }

  inline bool HasForeignKeySources() const { return (fk_sources_.size() > 0); }

  inline std::vector<std::shared_ptr<Constraint>> GetForeignKeySources() {
    return fk_sources_;
  }

  // Get a string representation for debugging
  const std::string GetInfo() const;

 private:
  // size of fixed length columns
  size_t length_;

  // all inlined and uninlined columns in the tuple
  std::vector<Column> columns_;

  // keep these in sync with the vectors above
  oid_t column_count_ = INVALID_OID;

  // keeps track of unlined columns
  std::vector<oid_t> uninlined_columns_;

  oid_t uninlined_column_count_ = INVALID_OID;

  // are all columns inlined
  bool tuple_is_inlined_;

  // keeps track of indexed columns in original table
  std::vector<oid_t> indexed_columns_;

  // Constraint Information
  // keeps constraints
  std::unordered_map<oid_t, std::shared_ptr<Constraint>> constraints;

  // not null column list for fast constraint checking
  std::vector<oid_t> not_null_columns_;

  // has a primary key ?
  bool has_primary_key_ = false;

  // # of unique constraints
  oid_t unique_constraint_count_ = START_OID;

  // list of foreign key constraints
  std::vector<oid_t> fk_constraints_;

  // fk constraints for which this table is the sink
  // The complete information is stored so no need to lookup the table
  // everytime there is a constraint check
  std::vector<std::shared_ptr<Constraint>> fk_sources_;
};

}  // namespace catalog
}  // namespace peloton
