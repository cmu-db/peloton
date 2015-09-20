/*-------------------------------------------------------------------------
 *
 * format_transformer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl/format_transformer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/ddl/format_transformer.h"
#include "backend/common/logger.h"

namespace peloton {
namespace bridge {

PelotonValueFormat
FormatTransformer::TransformValueFormat(PostgresValueFormat postgresValueFormat){

  oid_t postgres_value_type_id = postgresValueFormat.GetTypeId();
  int postgres_column_length = postgresValueFormat.GetLength();
  int postgres_typemod = postgresValueFormat.GetTypeMod(); 

  PostgresValueType postgresValueType = (PostgresValueType)postgres_value_type_id;
  ValueType peloton_value_type = PostgresValueTypeToPelotonValueType(postgresValueType);

  int peloton_column_length;
  bool peloton_is_inlined = true;

  switch(peloton_value_type){
    case VALUE_TYPE_TINYINT:
      peloton_column_length = 1;
      break;
    case VALUE_TYPE_SMALLINT:
      peloton_column_length = 2;
      break;
    case VALUE_TYPE_INTEGER:
      peloton_column_length = 4;
      break;
    case VALUE_TYPE_VARCHAR:
      peloton_column_length = 65535;
      break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_DOUBLE:
    case VALUE_TYPE_TIMESTAMP:
      peloton_column_length = 8;
      break;
    default:
      peloton_column_length = postgres_column_length;
      break;
  }

  if (peloton_column_length == -1) {
    peloton_column_length = postgres_typemod;
    peloton_is_inlined = false;
  }

  // TODO: Special case for DECIMAL. May move it somewhere else?
  // DECIMAL in PG is variable length but in Peloton is inlined (16 bytes)
  // This code is duplicated in schema_transformer.cpp
  if(VALUE_TYPE_DECIMAL == peloton_value_type){
    LOG_TRACE("Detect a DECIMAL attribute. \n");
    peloton_column_length = 16;
    peloton_is_inlined = true;
  }

  return PelotonValueFormat( peloton_value_type, 
                             peloton_column_length, 
                             peloton_is_inlined);

}

}  // End bridge namespace
}  // End peloton namespace
