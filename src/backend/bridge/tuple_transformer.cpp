/*-------------------------------------------------------------------------
 *
 * tuple_transformer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/bridge/tuple_transformer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <iostream>

#include "backend/bridge/tuple_transformer.h"
#include "backend/common/value_peeker.h"
#include "backend/storage/tuple.h"
#include "backend/common/types.h"
#include "backend/bridge/ddl.h"

#include "fmgr.h"
#include "utils/lsyscache.h"

namespace peloton {
namespace bridge {

/**
 * @brief Convert from Datum to Value.
 * @return converted Value.
 */
Value DatumGetValue(Datum datum, Oid atttypid) {
  Value value;

  switch (atttypid) {
    case 21:
    {
      int16_t smallint = DatumGetInt16(datum);
      printf("%d\n", smallint);
      value = ValueFactory::GetSmallIntValue(smallint);
    }
    break;

    case 23:
    {
      int32_t integer = DatumGetInt32(datum);
      printf("%d\n", integer);
      value = ValueFactory::GetIntegerValue(integer);
    }
    break;

    case 20:
    {
      int64_t bigint = DatumGetInt64(datum);
      printf("%ld\n", bigint);
      value = ValueFactory::GetBigIntValue(bigint);
    }
    break;

    case 1042:
    {
      char *character = DatumGetCString(datum);
      Pool *data_pool = nullptr;
      printf("%s\n", character);
      value = ValueFactory::GetStringValue(character, data_pool);
    }
    break;

    case 1043:
    {
      char * varlen_character = DatumGetCString(datum);
      Pool *data_pool = nullptr;
      printf("%s\n", varlen_character);
      value = ValueFactory::GetStringValue(varlen_character,
                                                    data_pool);
    }
    break;

    case 1114:
    {
      long int timestamp = DatumGetInt64(datum);
      char *timestamp_cstring = DatumGetCString(datum);
      printf("%s\n", timestamp_cstring);
      value = ValueFactory::GetTimestampValue(timestamp);
    }
    break;

    default:
      break;
  }

  return value;
}

/**
 * @brief Convert from Value to Datum.
 * @return converted Datum.
 */
Datum ValueGetDatum(Value value) {
  ValueType value_type;
  Datum datum;

  value_type = value.GetValueType();
  switch (value_type) {

    case 4:
    {
      int16_t smallint = ValuePeeker::PeekSmallInt(value);
      printf("%d\n", smallint);
      datum = Int16GetDatum(smallint);
    }
    break;

    case 5:
    {
      int32_t integer = ValuePeeker::PeekInteger(value);
      printf("%d\n", integer);
      datum = Int32GetDatum(integer);
    }
    break;

    case 6:
    {
      int64_t bigint = ValuePeeker::PeekBigInt(value);
      printf("%ld\n", bigint);
      datum = Int64GetDatum(bigint);
    }
    break;

    case 8:
    {
      double double_precision = ValuePeeker::PeekDouble(value);
      printf("%f\n", double_precision);
      datum = Float8GetDatum(double_precision);
    }
    break;

    case 9:
    {
      char *variable_character = (char *) ValuePeeker::PeekObjectValue(value);
      printf("%s\n", variable_character);
      datum = CStringGetDatum(variable_character);
    }
    break;

    case 11:
    {
      long int timestamp = ValuePeeker::PeekTimestamp(value);
      datum = Int64GetDatum(timestamp);
      printf("%s\n",DatumGetCString(timestamp));
    }
    break;

    default:
      break;
  }

  return datum;
}

/**
 * @brief Convert a Postgres tuple into Peloton tuple
 * @param slot Postgres tuple
 * @param schema Peloton scheme of the table to which the tuple belongs
 * @return a Peloton tuple
 */
storage::Tuple *TupleTransformer(TupleTableSlot *slot,
                                 const catalog::Schema *schema) {

  TupleDesc typeinfo = slot->tts_tupleDescriptor;
  int natts = typeinfo->natts;
  Datum attr;
  char *value;
  bool isnull;
  Oid typoutput;
  bool typisvarlena;
  unsigned attributeId;
  Form_pg_attribute attributeP;
  char *name;

  Datum p_datum;
  Oid p_oid;
  std::vector<Value> vals;
  std::vector<oid_t> oid_t_vec;
  oid_t num_columns = schema->GetColumnCount();

  // Go over each attribute
  for (oid_t att_itr = 0; att_itr < natts; ++att_itr) {

    attr = slot_getattr(slot, att_itr + 1, &isnull);
    if (isnull)
      continue;

    Assert(typeinfo != NULL);
    Assert(typeinfo->attrs[i] != NULL);
    getTypeOutputInfo(typeinfo->attrs[att_itr]->atttypid, &typoutput, &typisvarlena);

    value = OidOutputFunctionCall(typoutput, attr);

    // Print the attribute
    attributeId = att_itr + 1;
    attributeP = typeinfo->attrs[att_itr];

    name = NameStr(attributeP->attname);
    for (oid_t column_itr = 0; column_itr < num_columns; column_itr++) {
      // GetColumns and iterate it and compare with .. this.. one ..
      if (!strcmp(name, (schema->GetColumnInfo(column_itr).name).c_str()))
        oid_t_vec.push_back(column_itr);
    }

    p_oid = attributeP->atttypid;

    switch (p_oid) {
      // short int
      case 21:
        p_datum = Int16GetDatum((short )(atoi(value)));
        break;
        // integer
      case 23:
        p_datum = Int32GetDatum(atoi(value));
        break;
        // big int
      case 20:
        p_datum = Int64GetDatum(strtol(value,NULL,10));
        break;
        // real
      case 700:
        p_datum = Float4GetDatum(strtof(value, NULL));
        break;
        // double
      case 701:
        p_datum = Float8GetDatum(strtod(value, NULL));
        break;
        // char
      case 1042:
        p_datum = CStringGetDatum(value);
        break;
        // varchar
      case 1043:
        p_datum = CStringGetDatum(value);
        break;
      default:
        p_datum = CStringGetDatum(value);
        break;
    }

    vals.push_back(DatumGetValue(p_datum, p_oid));
  }

  // the num of values should be the same as schema found
  assert(vals.size() == oid_t_vec.size());

  catalog::Schema * tuple_schema = catalog::Schema::CopySchema(schema, oid_t_vec);

  // Allocate space for a new tuple with given schema
  storage::Tuple *tuple = new storage::Tuple(tuple_schema, true);

  oid_t att_itr = 0;
  for (auto val : vals) {
    tuple->SetValue(att_itr++, val);
  }

  return tuple;
}

} // namespace bridge
} // namespace peloton
