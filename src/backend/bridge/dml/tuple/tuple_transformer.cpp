//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tuple_transformer.cpp
//
// Identification: src/backend/bridge/dml/tuple/tuple_transformer.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/common/logger.h"
#include "backend/common/value_peeker.h"
#include "backend/storage/tuple.h"
#include "backend/common/types.h"
#include "backend/bridge/ddl/ddl.h"

#include "access/htup_details.h"
#include "nodes/print.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "postgres_ext.h"


namespace peloton {
namespace bridge {

/**
 * @brief Convert from Datum to Value.
 * @return converted Value.
 */
Value TupleTransformer::GetValue(Datum datum, Oid atttypid) {
  Value value;

  switch (atttypid) {
    case POSTGRES_VALUE_TYPE_SMALLINT: {
      int16_t smallint = DatumGetInt16(datum);
      LOG_TRACE("%d\n", smallint);
      value = ValueFactory::GetSmallIntValue(smallint);
    } break;

    case POSTGRES_VALUE_TYPE_INTEGER: {
      int32_t integer = DatumGetInt32(datum);
      LOG_TRACE("%d\n", integer);
      value = ValueFactory::GetIntegerValue(integer);
    } break;

    case POSTGRES_VALUE_TYPE_BIGINT: {
      int64_t bigint = DatumGetInt64(datum);
      LOG_TRACE("%ld\n", bigint);
      value = ValueFactory::GetBigIntValue(bigint);
    } break;

    case POSTGRES_VALUE_TYPE_DOUBLE: {
      double fpnum = DatumGetFloat8(datum);
      LOG_TRACE("%f\n", fpnum);
      value = ValueFactory::GetDoubleValue(fpnum);
    } break;

    /*
     * In PG, BPCHAR and VARCHAR and TEXT are represented using
     * 'struct varlena',
     * which is a 4-byte header followed by the meat.
     * However, the 4-byte header should not be accessed directly.
     * It should be used in MACROS:
     * VARSIZE(ptr), VARDATA(ptr) and VARHDRSZ.
     * NB1: VARSIZE(ptr) is the size of the meat PLUS the header.
     * NB2: DON'T assume strings have terminating-null's.
     */
    case POSTGRES_VALUE_TYPE_BPCHAR: {
      struct varlena *bpcharptr = reinterpret_cast<struct varlena *>(datum);
      int len = VARSIZE(bpcharptr) - VARHDRSZ;
      char *varchar = static_cast<char *>(VARDATA(bpcharptr));
      VarlenPool *data_pool = nullptr;
      std::string str(varchar, len);
      LOG_TRACE("len = %d , bpchar = \"%s\"", len, str.c_str());
      value = ValueFactory::GetStringValue(str, data_pool);

    } break;

    case POSTGRES_VALUE_TYPE_VARCHAR2: {
      struct varlena *varlenptr = reinterpret_cast<struct varlena *>(datum);
      int len = VARSIZE(varlenptr) - VARHDRSZ;
      char *varchar = static_cast<char *>(VARDATA(varlenptr));
      VarlenPool *data_pool = nullptr;
      std::string str(varchar, len);
      LOG_TRACE("len = %d , varchar = \"%s\"", len, str.c_str());
      value = ValueFactory::GetStringValue(str, data_pool);
    } break;

    case POSTGRES_VALUE_TYPE_TEXT: {
      struct varlena *textptr = reinterpret_cast<struct varlena *>(datum);
      int len = VARSIZE(textptr) - VARHDRSZ;
      char *varchar = static_cast<char *>(VARDATA(textptr));
      VarlenPool *data_pool = nullptr;
      std::string str(varchar, len);
      LOG_TRACE("len = %d , text = \"%s\"", len, str.c_str());
      value = ValueFactory::GetStringValue(str, data_pool);
    } break;

    case POSTGRES_VALUE_TYPE_TIMESTAMPS: {
      long int timestamp = DatumGetInt64(datum);
      value = ValueFactory::GetTimestampValue(timestamp);
    } break;

    case POSTGRES_VALUE_TYPE_DECIMAL:{
      /*
       * WARNING:
       * Peloton has smaller allowed precision/scale than PG.
       * If the passed in datum is longer than that,
       * Peloton will fail to convert it.
       */

      // 1. Get string representation of the PG numeric (this is tricky)
      char* cstr = DatumGetCString(DirectFunctionCall1(numeric_out, datum));

      LOG_INFO("PG decimal = %s \n", cstr);

      // 2. Construct Peloton Decimal from the string
      value = ValueFactory::GetDecimalValueFromString(std::string(cstr));

      pfree(cstr);
    } break;

    default:
      LOG_ERROR("Unknown atttypeid : %u ", atttypid);
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
    case VALUE_TYPE_SMALLINT: {
      int16_t smallint = ValuePeeker::PeekSmallInt(value);
      LOG_TRACE("%d\n", smallint);
      datum = Int16GetDatum(smallint);
    } break;

    case VALUE_TYPE_INTEGER: {
      int32_t integer = ValuePeeker::PeekInteger(value);
      LOG_TRACE("%d\n", integer);
      datum = Int32GetDatum(integer);
    } break;

    case VALUE_TYPE_BIGINT: {
      int64_t bigint = ValuePeeker::PeekBigInt(value);
      LOG_TRACE("%ld\n", bigint);
      datum = Int64GetDatum(bigint);
    } break;

    case VALUE_TYPE_DOUBLE: {
      double double_precision = ValuePeeker::PeekDouble(value);
      LOG_TRACE("%f\n", double_precision);
      datum = Float8GetDatum(double_precision);
    } break;

    case VALUE_TYPE_VARCHAR: {
      char *data_ptr = static_cast<char *>(ValuePeeker::PeekObjectValue(value));
      auto data_len = ValuePeeker::PeekObjectLengthWithoutNull(value);
      // NB: Peloton object don't have terminating-null's, so
      // we should use PG functions that take explicit length.
      datum = PointerGetDatum(cstring_to_text_with_len(data_ptr, data_len));
    } break;

    case VALUE_TYPE_TIMESTAMP: {
      long int timestamp = ValuePeeker::PeekTimestamp(value);
      datum = Int64GetDatum(timestamp);
      LOG_TRACE("%s\n", DatumGetCString(timestamp));
    } break;

    case VALUE_TYPE_DECIMAL: {

      auto precision = Value::kMaxDecPrec;
      auto scale = Value::kMaxDecScale;

      std::string str = ValuePeeker::PeekDecimalString(value);

      datum = DirectFunctionCall3(
          numeric_in,
          CStringGetDatum(str.c_str()),
          ObjectIdGetDatum(InvalidOid),
          Int32GetDatum(((precision << 16) | scale) + VARHDRSZ));


    } break;

    case VALUE_TYPE_NULL:
      datum = PointerGetDatum(nullptr);
      break;

    default:
      datum = PointerGetDatum(nullptr);
      LOG_ERROR("Unrecognized value type : %u\n", value_type);
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
storage::Tuple *TupleTransformer::GetPelotonTuple(
    TupleTableSlot *slot, const catalog::Schema *schema) {
  assert(slot);

  TupleDesc tuple_desc = slot->tts_tupleDescriptor;
  unsigned int natts = tuple_desc->natts;
  bool isnull;

  // Allocate space for a new tuple with given schema
  storage::Tuple *tuple = new storage::Tuple(schema, true);

  // Go over each attribute and convert Datum to Value
  for (oid_t att_itr = 0; att_itr < natts; ++att_itr) {
    Datum attr = slot_getattr(slot, att_itr + 1, &isnull);
    if (isnull) continue;

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
TupleTableSlot *TupleTransformer::GetPostgresTuple(AbstractTuple *tuple,
                                                   TupleDesc tuple_desc) {
  assert(tuple);
  assert(tuple_desc);

  TupleTableSlot *slot = NULL;
  HeapTuple heap_tuple;
  const unsigned int natts = tuple_desc->natts;
  Datum *datums;
  bool *nulls;

  // Allocate space for datums
  datums = (Datum *)palloc0(natts * sizeof(Datum));
  nulls = (bool *)palloc0(natts * sizeof(bool));

  // Go over each attribute and convert Value to Datum
  for (oid_t att_itr = 0; att_itr < natts; ++att_itr) {
    Value value = tuple->GetValue(att_itr);
    Datum datum = GetDatum(value);

    assert(tuple_desc->attrs[att_itr]->attbyval == true
           || value.GetValueType() == VALUE_TYPE_VARCHAR
           || value.GetValueType() == VALUE_TYPE_VARBINARY
           || value.GetValueType() == VALUE_TYPE_DECIMAL
           || value.GetValueType() == VALUE_TYPE_NULL);

    datums[att_itr] = datum;
    nulls[att_itr] = value.IsNull() ? true : false;
  }

  // Construct tuple
  // PG does a deep copy in heap_form_tuple()
  heap_tuple = heap_form_tuple(tuple_desc, datums, nulls);

  // Construct slot
  slot = MakeSingleTupleTableSlot(tuple_desc);

  // Store tuple in slot
  // This function just sets a point in slot to the heap_tuple.
  ExecStoreTuple(heap_tuple, slot, InvalidBuffer, true);

  // Clean up
  // (A) Clean up any possible varlena's
  //assert(natts == tuple_desc->natts);
  for (oid_t att_itr = 0; att_itr < natts; ++att_itr) {
    if (tuple_desc->attrs[att_itr]->attlen < 0) {  // should be a varlena
      assert(tuple_desc->attrs[att_itr]->attbyval == false);
      if (nulls[att_itr]) continue;

      pfree((void *)(datums[att_itr]));
    }
  }

  // (B) Free the datum array itself
  pfree(datums);

  pfree(nulls);

  return slot;
}


}  // namespace bridge
}  // namespace peloton
