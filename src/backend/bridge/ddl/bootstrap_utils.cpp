/*-------------------------------------------------------------------------
 *
 * bootstrap_utils.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/bootstrap_utils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <cassert>

#include "backend/bridge/ddl/bootstrap_utils.h"
#include "backend/common/logger.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "common/fe_memutils.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

namespace peloton {
namespace bridge {

/**
 * @brief copy raw tables from vector to raw_database->raw_tables
 * @param raw database
 * @param raw table vector
 */
void BootstrapUtils::CopyRawTables(raw_database_info* raw_database,
                              std::vector<raw_table_info*> raw_tables){
  raw_database->raw_tables = (raw_table_info**)palloc(sizeof(raw_table_info*)*raw_tables.size());
  oid_t table_itr=0;
  for( auto raw_table : raw_tables){
    raw_database->raw_tables[table_itr++] = raw_table;
  }
  raw_database->table_count = table_itr;
}

/**
 * @brief copy raw indexes from vector to raw_database->raw_indexes
 * @param raw database
 * @param raw index vector
 */
void BootstrapUtils::CopyRawIndexes(raw_database_info* raw_database,
                                    std::vector<raw_index_info*> raw_indexes){
  raw_database->raw_indexes = (raw_index_info**)palloc(sizeof(raw_index_info*)*raw_indexes.size());
  oid_t index_itr=0;
  for( auto raw_index : raw_indexes){
    raw_database->raw_indexes[index_itr++] = raw_index;
  }
  raw_database->index_count = index_itr;
}

/**
 * @brief copy given string
 * @param string
 * @return string allocated using palloc
 */
char* BootstrapUtils::CopyString(const char* string){
  size_t len = strlen(string);
  size_t size = (len+1)*sizeof(char);
  char* string_desc = (char*)palloc(size);
  strncpy(string_desc, string, len);
  string_desc[len] = '\0';
  return string_desc;
}

/**
 * @brief copy given string vector
 * @param string vector
 * @return string allocated using palloc
 */
char** BootstrapUtils::CopyStrings(std::vector<std::string> strings){
  char** string_dest = (char**)palloc(sizeof(char*)*(strings.size()));

  oid_t string_itr=0;
  for(auto string : strings){
    string_dest[string_itr++] = CopyString(string.c_str());
  }
  return string_dest;
}

/**
 * @brief print raw database for debugging
 */
void BootstrapUtils::PrintRawDatabase(raw_database_info* raw_database){
  printf("\n\nPrint Dataase %s(%u)\n\n", raw_database->database_name, raw_database->database_oid);
  PrintRawTables(raw_database->raw_tables, raw_database->table_count);
  PrintRawIndexes(raw_database->raw_indexes, raw_database->index_count);
  printf("\n\n");
}

void BootstrapUtils::PrintRawTables(raw_table_info** raw_tables,
                                    oid_t table_count){
  for(int table_itr=0; table_itr<table_count; table_itr++){
    printf("  Print Table #%u\n", table_itr);
    PrintRawTable(raw_tables[table_itr]);
    printf("\n");
  }
}

void BootstrapUtils::PrintRawIndexes(raw_index_info** raw_indexes,
                                     oid_t index_count){
  for(int index_itr=0; index_itr<index_count; index_itr++){
    printf("  Print Index #%u\n", index_itr);
    PrintRawIndex(raw_indexes[index_itr]);
    printf("\n");
  }
}

void BootstrapUtils::PrintRawTable(raw_table_info* raw_table){
  printf("  table name %s \n", raw_table->table_name);
  printf("  table oid %u \n", raw_table->table_oid);
  PrintRawColumns(raw_table->raw_columns, raw_table->column_count);
}

void BootstrapUtils::PrintRawIndex(raw_index_info* raw_index){
  printf("  index name %s %d \n", raw_index->index_name, (int)strlen(raw_index->index_name));
  printf("  index oid %u \n", raw_index->index_oid);
  printf("  table name %s \n", raw_index->table_name);
  printf("  method type %d \n", (int)raw_index->method_type);
  printf("  constraint type %d \n", (int)raw_index->constraint_type);
  printf("  unique keys %d \n", (int)raw_index->unique_keys);
  PrintKeyColumnNames(raw_index->key_column_names, raw_index->key_column_count);
}

void BootstrapUtils::PrintRawColumn(raw_column_info* raw_column){
  printf("    column name %s \n", raw_column->column_name);
  printf("    column type %s \n", ValueTypeToString(raw_column->column_type).c_str());
  printf("    column length %u \n", raw_column->column_length);
  printf("    column inlined %d \n", (int)raw_column->is_inlined);
  PrintRawConstraints(raw_column->raw_constraints, raw_column->constraint_count);
}

void BootstrapUtils::PrintRawColumns(raw_column_info** raw_columns,
                     oid_t column_count){
  for(int column_itr=0; column_itr<column_count; column_itr++){
    printf("    Print Column #%u\n", column_itr);
    PrintRawColumn(raw_columns[column_itr]);
    printf("\n");
  }
}

void BootstrapUtils::PrintRawConstraints(raw_constraint_info** raw_constraints,
                                         oid_t constraint_count){
  for(int constraint_itr=0; constraint_itr<constraint_count; constraint_itr++){
    printf("      Print Constraint #%u\n", constraint_itr);
    PrintRawConstraint(raw_constraints[constraint_itr]);
    printf("\n");
  }
}

void BootstrapUtils::PrintRawConstraint(raw_constraint_info* raw_constraint){
  printf("      constraint type %s \n", ConstraintTypeToString(raw_constraint->constraint_type).c_str());
  printf("      constraint name %s \n", raw_constraint->constraint_name);
}

void BootstrapUtils::PrintKeyColumnNames(char** key_column_names,
                                         oid_t key_column_count){
  for(int key_column_itr=0; key_column_itr<key_column_count; key_column_itr++){
    printf("    Print KeyColumnName %s\n\n", key_column_names[key_column_itr]);
  }
}

}  // namespace bridge
}  // namespace peloton
