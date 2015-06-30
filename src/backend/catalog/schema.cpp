/*-------------------------------------------------------------------------
 *
 * tuple_schema.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/tuple_schema.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/schema.h"

#include <cassert>

#include <algorithm>
#include <sstream>

namespace peloton {
namespace catalog {

/// Helper function for creating TupleSchema
void Schema::CreateTupleSchema(const std::vector<ValueType> column_types,
                               const std::vector<oid_t> column_lengths,
                               const std::vector<std::string> column_names,
                               const std::vector<bool> allow_null,
                               const std::vector<bool> is_inlined,
                               const std::vector<std::vector<Constraint>> constraint_vector_of_vectors) {

  bool tup_is_inlined = true;
  oid_t num_columns = column_types.size();
  oid_t column_offset = 0;

  for(oid_t column_itr = 0 ; column_itr < num_columns ; column_itr++)	{

    ColumnInfo column_info(column_types[column_itr],
                           column_offset,
                           column_lengths[column_itr],
                           column_names[column_itr],
                           allow_null[column_itr],
                           is_inlined[column_itr],
                           constraint_vector_of_vectors[column_itr]);

    column_offset += column_info.fixed_length;

    columns.push_back(column_info);

    if(is_inlined[column_itr] == false){
      tup_is_inlined = false;
      uninlined_columns.push_back(column_itr);
    }
  }

  length = column_offset;
  tuple_is_inlined = tup_is_inlined;

  column_count = columns.size();
  uninlined_column_count = uninlined_columns.size();
}

/// Construct schema from vector of ColumnInfo
Schema::Schema(const std::vector<ColumnInfo> columns)
: length(0),
  tuple_is_inlined(false) {
  oid_t column_count = columns.size();

  std::vector<ValueType> column_types;
  std::vector<oid_t> column_lengths;
  std::vector<std::string> column_names;
  std::vector<bool> allow_null;
  std::vector<bool> is_inlined;
  std::vector<std::vector<Constraint>> constraint_vector_of_vectors;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    column_types.push_back(columns[column_itr].type);

    if(columns[column_itr].is_inlined)
      column_lengths.push_back(columns[column_itr].fixed_length);
    else
      column_lengths.push_back(columns[column_itr].variable_length);

    column_names.push_back(columns[column_itr].name);
    allow_null.push_back(columns[column_itr].allow_null);
    is_inlined.push_back(columns[column_itr].is_inlined);
    constraint_vector_of_vectors.push_back(columns[column_itr].constraint_vector);
  }

  CreateTupleSchema(column_types, column_lengths, column_names, allow_null, is_inlined, constraint_vector_of_vectors);
}

/// Copy schema
Schema* Schema::CopySchema(const Schema	*schema) {
  oid_t column_count = schema->GetColumnCount();
  std::vector<oid_t> set;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++)
    set.push_back(column_itr);

  return CopySchema(schema, set);
}

/// Copy subset of columns in the given schema
Schema* Schema::CopySchema(const Schema *schema,
                           const std::vector<oid_t>& set){
  oid_t column_count = schema->GetColumnCount();
  std::vector<ColumnInfo> columns;

  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // If column exists in set
    if(std::find(set.begin(), set.end(), column_itr) != set.end()) {
      columns.push_back(schema->columns[column_itr]);
    }
  }

  Schema *ret_schema = new Schema(columns);
  return ret_schema;
}

/// Append two schema objects
Schema* Schema::AppendSchema(Schema *first, Schema *second){

  return AppendSchemaPtrList({first, second});
}

/// Append subset of columns in the two given schemas
Schema* Schema::AppendSchema(
    Schema *first,
    std::vector<oid_t>& first_set,
    Schema *second,
    std::vector<oid_t>& second_set) {
  const std::vector<Schema *> schema_list({first, second});
  const std::vector<std::vector<oid_t> > subsets({first_set, second_set});
  return AppendSchemaPtrList(schema_list, subsets);
}

/// Append given schemas.
Schema *Schema::AppendSchemaList(std::vector<Schema> &schema_list) {
  // All we do here is convert vector<Schema> to vector<Schema *>.
  // This is a convenience function.
  std::vector<Schema *> schema_ptr_list;
  for (unsigned int i = 0; i < schema_list.size(); i++) {
    schema_ptr_list.push_back(&schema_list[i]);
  }
  return AppendSchemaPtrList(schema_ptr_list);
}

/// Append given schemas.
Schema *Schema::AppendSchemaPtrList(const std::vector<Schema *> &schema_list) {
  std::vector<std::vector<oid_t> > subsets;

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

/// Append subsets of columns in the given schemas.
Schema *Schema::AppendSchemaPtrList(
    const std::vector<Schema *> &schema_list,
    const std::vector<std::vector<oid_t> > &subsets) {
  assert(schema_list.size() == subsets.size());

  std::vector<ColumnInfo> columns;
  for (unsigned int i = 0; i < schema_list.size(); i++) {
    Schema *schema = schema_list[i];
    const std::vector<oid_t> &subset = subsets[i];
    unsigned int column_count = schema->GetColumnCount();

    for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
      // If column exists in set.
      if(std::find(subset.begin(), subset.end(), column_itr) != subset.end()) {
        columns.push_back(schema->columns[column_itr]);
      }
    }
  }

  Schema *ret_schema = new Schema(columns);

  return ret_schema;
}

/// Get a string representation
std::ostream& operator<< (std::ostream& os, const ColumnInfo& column_info){
  os << " type = " << GetTypeName(column_info.type) << "," <<
      " offset = " << column_info.offset << "," <<
      " fixed length = " << column_info.fixed_length << "," <<
      " variable length = " << column_info.variable_length << "," <<
      " nullable = " << column_info.allow_null <<
      " inlined = " << column_info.is_inlined << std::endl;

  
  for( oid_t constraint_itr = 0; constraint_itr < column_info.constraint_vector.size(); constraint_itr++)
  { 
    Constraint constraint = column_info.constraint_vector[constraint_itr] ;
    os << "\tConstraint :: " << constraint_itr+1 << " " << constraint.GetName() << " type : " << ConstraintTypeToString(constraint.GetType()) << std::endl;
  }

  return os;
}

/// Get a string representation of this schema for debugging
std::ostream& operator<< (std::ostream& os, const Schema& schema){
  os << "\tSchema :: " <<
      " column_count = " << schema.column_count <<
      " is_inlined = " << schema.tuple_is_inlined << "," <<
      " length = " << schema.length << "," <<
      " uninlined_column_count = " << schema.uninlined_column_count << std::endl;

  for (oid_t column_itr = 0; column_itr < schema.column_count; column_itr++) {
    os << "\t Column " << column_itr << " :: " << schema.columns[column_itr];
  }

  return os;
}

/// Compare two schemas
bool Schema::operator== (const Schema &other) const {

  if (other.GetColumnCount() != GetColumnCount() ||
      other.GetUninlinedColumnCount() != GetUninlinedColumnCount() ||
      other.IsInlined() != IsInlined()) {
    return false;
  }

  for (oid_t column_itr = 0; column_itr < other.GetColumnCount(); column_itr++) {
    const ColumnInfo& column_info = other.GetColumnInfo(column_itr);
    const ColumnInfo& other_column_info = GetColumnInfo(column_itr);

    if (column_info != other_column_info) {
      return false;
    }
  }

  return true;
}

bool Schema::operator!= (const Schema &other) const {
  return !(*this == other);
}

} // End catalog namespace
} // End peloton namespace
