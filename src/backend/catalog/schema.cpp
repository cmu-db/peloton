//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema.cpp
//
// Identification: src/backend/catalog/schema.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "backend/catalog/schema.h"
#include "backend/common/macros.h"

namespace peloton {
namespace catalog {

// Helper function for creating TupleSchema
void Schema::CreateTupleSchema(const std::vector<ValueType> &column_types,
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

    column_offset += column.fixed_length;

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

  std::vector<ValueType> column_types;
  std::vector<oid_t> column_lengths;
  std::vector<std::string> column_names;
  std::vector<bool> is_inlined;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    column_types.push_back(columns[column_itr].column_type);

    if (columns[column_itr].is_inlined)
      column_lengths.push_back(columns[column_itr].fixed_length);
    else
      column_lengths.push_back(columns[column_itr].variable_length);

    column_names.push_back(columns[column_itr].column_name);
    is_inlined.push_back(columns[column_itr].is_inlined);
  }

  CreateTupleSchema(column_types, column_lengths, column_names, is_inlined);

  // Add constraints
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    for (auto constraint : columns[column_itr].constraints)
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

// Copy subset of columns in the given schema
Schema *Schema::CopySchema(const Schema *schema,
                           const std::vector<oid_t> &set) {
  oid_t column_count = schema->GetColumnCount();
  std::vector<Column> columns;

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
  ALWAYS_ASSERT(schema_list.size() == subsets.size());

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

  os << "\tSchema :: "
     << " column_count = " << column_count
     << " is_inlined = " << tuple_is_inlined << ","
     << " length = " << length << ","
     << " uninlined_column_count = " << uninlined_column_count << std::endl;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    os << "\t Column " << column_itr << " :: " << columns[column_itr];
  }

  return os.str();
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

}  // End catalog namespace
}  // End peloton namespace
