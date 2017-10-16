//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema.cpp
//
// Identification: src/catalog/schema.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "catalog/schema.h"

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/macros.h"

namespace peloton {
namespace catalog {

// Helper function for creating TupleSchema
void Schema::CreateTupleSchema(
    const std::vector<type::TypeId> &column_types,
    const std::vector<oid_t> &column_lengths,
    const std::vector<std::string> &column_names,
    const std::vector<bool> &is_inlined) {
  bool tup_is_inlined = true;
  oid_t num_columns = column_types.size();
  oid_t column_offset = 0;

  for (oid_t column_itr = 0; column_itr < num_columns; column_itr++) {
    Column column(column_types[column_itr], column_lengths[column_itr],
                  column_names[column_itr], is_inlined[column_itr],
                  column_offset);

    column_offset += column.GetFixedLength();

    columns.push_back(std::move(column));

    if (is_inlined[column_itr] == false) {
      tup_is_inlined = false;
      uninlined_columns.push_back(column_itr);
    }
  }

  length = column_offset;
  tuple_is_inlined = tup_is_inlined;

  column_count = columns.size();
  uninlined_column_count = uninlined_columns.size();
}

// Construct schema from vector of Column
Schema::Schema(const std::vector<Column> &columns)
    : length(0), tuple_is_inlined(false) {
  oid_t column_count = columns.size();

  std::vector<type::TypeId> column_types;
  std::vector<oid_t> column_lengths;
  std::vector<std::string> column_names;
  std::vector<bool> is_inlined;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    column_types.push_back(columns[column_itr].GetType());

    if (columns[column_itr].IsInlined())
      column_lengths.push_back(columns[column_itr].GetFixedLength());
    else
      column_lengths.push_back(columns[column_itr].GetVariableLength());

    column_names.push_back(columns[column_itr].GetName());
    is_inlined.push_back(columns[column_itr].IsInlined());
  }

  CreateTupleSchema(column_types, column_lengths, column_names, is_inlined);

  // Add constraints
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    for (auto constraint : columns[column_itr].GetConstraints())
      AddConstraint(column_itr, constraint);
  }
}

// Copy schema
std::shared_ptr<const Schema> Schema::CopySchema(
    const std::shared_ptr<const Schema> &schema) {
  oid_t column_count = schema->GetColumnCount();
  std::vector<oid_t> set;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++)
    set.push_back(column_itr);

  return CopySchema(schema, set);
}

// Copy subset of columns in the given schema
std::shared_ptr<const Schema> Schema::CopySchema(
    const std::shared_ptr<const Schema> &schema,
    const std::vector<oid_t> &set) {
  oid_t column_count = schema->GetColumnCount();
  std::vector<Column> columns;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // If column exists in set
    if (std::find(set.begin(), set.end(), column_itr) != set.end()) {
      columns.push_back(schema->columns[column_itr]);
    }
  }

  return std::shared_ptr<Schema>(new Schema(columns));
}

// Backward compatible for raw pointers
// Copy schema
Schema *Schema::CopySchema(const Schema *schema) {
  oid_t column_count = schema->GetColumnCount();
  std::vector<oid_t> set;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++)
    set.push_back(column_itr);

  return CopySchema(schema, set);
}

/*
 * CopySchema() - Copies the schema into a new schema object with index_list
 *                as indices to copy
 *
 * This function essentially does a "Gathering" operation, in a sense that it
 * collects columns indexed by elements inside index_list in the given order
 * and store them inside a newly created schema object
 *
 * If there are duplicates in index_list then the columns will be duplicated
 * (i.e. no dup checking will be done)
 *
 * If the indices inside index_list >= the size of the column list then behavior
 * is undefined (and is likely to crash)
 *
 * The returned schema is created by new operator, and the caller is responsible
 * for destroying it.
 */
Schema *Schema::CopySchema(const Schema *schema,
                           const std::vector<oid_t> &index_list) {
  std::vector<Column> column_list{};

  // Reserve some space to avoid multiple ma110c() calls
  // But for future push_back() this is not optimized since the
  // memory chunk may not be properly sized and aligned
  column_list.reserve(index_list.size());

  // For each column index, push the column
  for (oid_t column_index : index_list) {
    // Make sure the index does not refer to invalid element
    PL_ASSERT(column_index < schema->columns.size());

    column_list.push_back(schema->columns[column_index]);
  }

  Schema *ret_schema = new Schema(column_list);

  return ret_schema;
}

/*
 * FilterSchema() - Returns a filtered schema using the given base schema
 *                  and index array in the argument
 *
 * This function performs a "Filtering" operation on the schema given in the
 * argument using the index list into a newly created schema object. The
 * returned schema remains the order inside the base schema, no matter how
 * indices are arranged in the index list.
 *
 * If there are duplicated indices in the set, it is guaranteed that only
 * one column will be copied into the returned schema. This is achieved by
 * only traversing the underlying schema once and searches for the column
 * index inside the index list
 *
 * Please note that the new schame is created on the heap, and the caller
 * is responsible for destroying it.
 */
Schema *Schema::FilterSchema(const Schema *schema,
                             const std::vector<oid_t> &set) {
  oid_t column_count = schema->GetColumnCount();
  std::vector<Column> columns;

  // It could only be smaller but not larger, here we use
  // the size of the set (might have duplication) as an estimation
  columns.reserve(set.size());

  // For each column in the base schema, if the column id
  // appears inside the set then push it into the column list
  // for later construction of the new schema
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // If column exists in set
    if (std::find(set.begin(), set.end(), column_itr) != set.end()) {
      columns.push_back(schema->columns[column_itr]);
    }
  }

  Schema *ret_schema = new Schema(columns);

  return ret_schema;
}

// Append two schema objects
Schema *Schema::AppendSchema(Schema *first, Schema *second) {
  return AppendSchemaPtrList({first, second});
}

// Append subset of columns in the two given schemas
Schema *Schema::AppendSchema(Schema *first, std::vector<oid_t> &first_set,
                             Schema *second, std::vector<oid_t> &second_set) {
  const std::vector<Schema *> schema_list({first, second});
  const std::vector<std::vector<oid_t>> subsets({first_set, second_set});
  return AppendSchemaPtrList(schema_list, subsets);
}

// Append given schemas.
Schema *Schema::AppendSchemaList(std::vector<Schema> &schema_list) {
  // All we do here is convert vector<Schema> to vector<Schema *>.
  // This is a convenience function.
  std::vector<Schema *> schema_ptr_list;
  for (unsigned int i = 0; i < schema_list.size(); i++) {
    schema_ptr_list.push_back(&schema_list[i]);
  }
  return AppendSchemaPtrList(schema_ptr_list);
}

// Append given schemas.
Schema *Schema::AppendSchemaPtrList(const std::vector<Schema *> &schema_list) {
  std::vector<std::vector<oid_t>> subsets;

  for (unsigned int i = 0; i < schema_list.size(); i++) {
    unsigned int column_count = schema_list[i]->GetColumnCount();
    std::vector<oid_t> subset;
    for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
      subset.push_back(column_itr);
    }
    subsets.push_back(subset);
  }

  return AppendSchemaPtrList(schema_list, subsets);
}

// Append subsets of columns in the given schemas.
Schema *Schema::AppendSchemaPtrList(
    const std::vector<Schema *> &schema_list,
    const std::vector<std::vector<oid_t>> &subsets) {
  PL_ASSERT(schema_list.size() == subsets.size());

  std::vector<Column> columns;
  for (unsigned int i = 0; i < schema_list.size(); i++) {
    Schema *schema = schema_list[i];
    const std::vector<oid_t> &subset = subsets[i];
    unsigned int column_count = schema->GetColumnCount();

    for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
      // If column exists in set.
      if (std::find(subset.begin(), subset.end(), column_itr) != subset.end()) {
        columns.push_back(schema->columns[column_itr]);
      }
    }
  }

  Schema *ret_schema = new Schema(columns);

  return ret_schema;
}

const std::string Schema::GetInfo() const {
  std::ostringstream os;

  os << "Schema["
     << "NumColumns:" << column_count << ", "
     << "IsInlined:" << tuple_is_inlined << ", "
     << "Length:" << length << ", "
     << "UninlinedCount:" << uninlined_column_count << "]";

  bool first = true;
  os << " :: (";
  for (oid_t i = 0; i < column_count; i++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    os << columns[i].GetInfo();
  }
  os << ")";

  return os.str();
}

hash_t Schema::Hash() const {
  auto column_count = GetColumnCount();
  hash_t hash = HashUtil::Hash(&column_count);

  auto uninlined_column_count = GetUninlinedColumnCount();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&uninlined_column_count));

  auto is_inlined = IsInlined();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_inlined));

  for (const auto &column : columns) {
    hash = HashUtil::CombineHashes(hash, column.Hash());
  }
  return hash;
}

// Compare two schemas
bool Schema::operator==(const Schema &other) const {
  if (other.GetColumnCount() != GetColumnCount() ||
      other.GetUninlinedColumnCount() != GetUninlinedColumnCount() ||
      other.IsInlined() != IsInlined()) {
    return false;
  }

  for (oid_t column_itr = 0; column_itr < other.GetColumnCount();
       column_itr++) {
    const Column &column_info = other.GetColumn(column_itr);
    const Column &other_column_info = GetColumn(column_itr);

    if (column_info != other_column_info) {
      return false;
    }
  }

  return true;
}

bool Schema::operator!=(const Schema &other) const { return !(*this == other); }

}  // namespace catalog
}  // namespace peloton
