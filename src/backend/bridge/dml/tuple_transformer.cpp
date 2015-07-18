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

#include "backend/bridge/dml/tuple_transformer.h"
#include "backend/common/logger.h"
#include "backend/common/value_peeker.h"
#include "backend/storage/tuple.h"
#include "backend/common/types.h"
#include "backend/bridge/ddl/ddl.h"

#include "access/htup_details.h"
#include "nodes/print.h"

namespace peloton {
namespace bridge {

/**
 * @brief Convert from Datum to Value.
 * @return converted Value.
 */
Value TupleTransformer::GetValue(Datum datum, Oid atttypid) {
  Value value;

  switch (atttypid) {
    case 21:
    {
      int16_t smallint = DatumGetInt16(datum);
      LOG_INFO("%d\n", smallint);
      value = ValueFactory::GetSmallIntValue(smallint);
    }
    break;

    case 23:
    {
      int32_t integer = DatumGetInt32(datum);
      LOG_INFO("%d\n", integer);
      value = ValueFactory::GetIntegerValue(integer);
    }
    break;

    case 20:
    {
      int64_t bigint = DatumGetInt64(datum);
      LOG_INFO("%ld\n", bigint);
      value = ValueFactory::GetBigIntValue(bigint);
    }
    break;

    case 1042:
    {
      char *character = DatumGetCString(datum);
      Pool *data_pool = nullptr;
      LOG_INFO("%s\n", character);
      value = ValueFactory::GetStringValue(character, data_pool);
    }
    break;

    case 1043:
    {
      char * varlen_character = DatumGetCString(datum);
      Pool *data_pool = nullptr;
      LOG_INFO("%s\n", varlen_character);
      value = ValueFactory::GetStringValue(varlen_character,
                                           data_pool);
    }
    break;

    case 1114:
    {
      long int timestamp = DatumGetInt64(datum);
      char *timestamp_cstring = DatumGetCString(datum);
      LOG_INFO("%s\n", timestamp_cstring);
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
Datum TupleTransformer::GetDatum(Value value) {
  ValueType value_type;
  Datum datum;

  value_type = value.GetValueType();
  switch (value_type) {

    case 4:
    {
      int16_t smallint = ValuePeeker::PeekSmallInt(value);
      LOG_INFO("%d\n", smallint);
      datum = Int16GetDatum(smallint);
    }
    break;

    case 5:
    {
      int32_t integer = ValuePeeker::PeekInteger(value);
      LOG_INFO("%d\n", integer);
      datum = Int32GetDatum(integer);
    }
    break;

    case 6:
    {
      int64_t bigint = ValuePeeker::PeekBigInt(value);
      LOG_INFO("%ld\n", bigint);
      datum = Int64GetDatum(bigint);
    }
    break;

    case 8:
    {
      double double_precision = ValuePeeker::PeekDouble(value);
      LOG_INFO("%f\n", double_precision);
      datum = Float8GetDatum(double_precision);
    }
    break;

    case 9:
    {
      char *variable_character = (char *) ValuePeeker::PeekObjectValue(value);
      LOG_INFO("%s\n", variable_character);
      datum = CStringGetDatum(variable_character);
    }
    break;

    case 11:
    {
      long int timestamp = ValuePeeker::PeekTimestamp(value);
      datum = Int64GetDatum(timestamp);
      LOG_INFO("%s\n",DatumGetCString(timestamp));
    }
    break;

    default:
      datum = PointerGetDatum(nullptr);
      LOG_INFO("Unrecognized value type : %u\n", value_type);
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
storage::Tuple *TupleTransformer::GetPelotonTuple(TupleTableSlot *slot,
                                                 const catalog::Schema *schema) {
  assert(slot);

  TupleDesc tuple_desc = slot->tts_tupleDescriptor;
  int natts = tuple_desc->natts;
  bool isnull;

  // Allocate space for a new tuple with given schema
  storage::Tuple *tuple = new storage::Tuple(schema, true);

  // Go over each attribute and convert Datum to Value
  for (oid_t att_itr = 0; att_itr < natts; ++att_itr) {
    Datum attr = slot_getattr(slot, att_itr + 1, &isnull);
    if (isnull)
      continue;

    Form_pg_attribute attribute_info = tuple_desc->attrs[att_itr];
    Oid attribute_type_id = attribute_info->atttypid;

    Value value = GetValue(attr, attribute_type_id);
    tuple->SetValue(att_itr++, value);
  }

  return tuple;
}

/**
 * @brief Convert a Peloton tuple into Postgres tuple slot
 * @param tuple Peloton tuple
 * @return a Postgres tuple
 */
TupleTableSlot *TupleTransformer::GetPostgresTuple(storage::Tuple *tuple,
                                                   TupleDesc tuple_desc) {
  assert(tuple);

  TupleTableSlot *slot = NULL;
  HeapTuple heap_tuple;
  int natts = tuple_desc->natts;
  Datum *datums;
  bool *nulls;

  assert(tuple->GetColumnCount() == natts);

  // Allocate space for datums
  datums = (Datum *) palloc(natts * sizeof(Datum));
  nulls = (bool *) palloc(natts * sizeof(bool));

  // Go over each attribute and convert Value to Datum
  for (oid_t att_itr = 0; att_itr < natts; ++att_itr) {
    Value value = tuple->GetValue(att_itr);
    Datum datum = GetDatum(value);

    datums[att_itr] = datum;
    nulls[att_itr] = tuple->IsNull(att_itr) ? true : false;
  }

  // Construct tuple
  heap_tuple = heap_form_tuple(tuple_desc, datums, nulls);

  // Construct slot
  slot = MakeSingleTupleTableSlot(tuple_desc);

  // Store tuple in slot
  ExecStoreTuple(heap_tuple, slot, InvalidBuffer, true);

  // Clean up
  pfree(datums);
  pfree(nulls);

  return slot;
}


} // namespace bridge
} // namespace peloton
