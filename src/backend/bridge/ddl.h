/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

typedef struct 
{
   char name[100];
   int type;
   int size;
   bool is_not_null;
} DDL_Column;

#ifdef __cplusplus

namespace nstore {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Class 
//===--------------------------------------------------------------------===//

class DDL {
public:
  static int CreateTable(char* table_name, DDL_Column* columns, int number_of_columns);

};

extern "C" {
  int DDL_CreateTable(char* table_name, DDL_Column* columns, int number_of_columns);
}

} // namespace bridge
} // namespace nstore

#endif

extern int DDL_CreateTable(char* table_name, DDL_Column* columns, int number_of_columns);

