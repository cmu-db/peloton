/*
 * tupleTransformer.cpp
 *
 *  Created on: Jun 22, 2015
 *      Author: vivek
 */

#include <iostream>

#include "backend/bridge/tuple_transformer.h"
#include "backend/common/value_peeker.h"

void TestTupleTransformer(Datum datum, Oid atttypid) {

  nstore::Value p_value;
  Datum p_datum;
  printf("Call to DatumGetValue in Peloton\n");
  printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
  p_value = DatumGetValue(datum, atttypid);
  printf("\n");
  printf("--------------------------------\n");
  printf("\n");
  printf("Call to ValueGetDatum in Peloton\n");
  printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
  p_datum = ValueGetDatum(p_value);
  printf("\n");
  printf("--------------------------------\n");
  printf("\n");
}



nstore::Value
DatumGetValue(Datum datum, Oid atttypid) {
  nstore::Value value;
  nstore::Pool *data_pool = nullptr;
  int16_t smallint;
  int32_t integer;
  int64_t bigint;
  char *character;
  char *variable_character;
  double timestamp;

  switch (atttypid) {
    case 21:
      smallint = DatumGetInt16(datum);
      printf("%d\n", smallint);
      value = nstore::ValueFactory::GetSmallIntValue(smallint);
      break;

    case 23:
      integer = DatumGetInt32(datum);
      printf("%d\n", integer);
      value = nstore::ValueFactory::GetIntegerValue(integer);
      break;

    case 20:
      bigint = DatumGetInt64(datum);
      printf("%ld\n", bigint);
      value = nstore::ValueFactory::GetBigIntValue(bigint);
      break;

    case 1042:
      character = DatumGetCString(datum);
      printf("%s\n", character);
      value = nstore::ValueFactory::GetStringValue(character, data_pool);
      break;

    case 1043:
      variable_character = DatumGetCString(datum);
      printf("%s\n", variable_character);
      value = nstore::ValueFactory::GetStringValue(variable_character,
                                                   data_pool);
      break;

    case 1114:
      timestamp = DatumGetFloat8(datum);
      printf("%f\n", timestamp);
      value = nstore::ValueFactory::GetDoubleValue(timestamp);
      break;

    default:
      break;

  }
  return value;
}

Datum
ValueGetDatum(nstore::Value value) {
  nstore::ValueType value_type;
  nstore::ValuePeeker value_peeker;
  Datum datum;
  int16_t smallint;
  int32_t integer;
  int64_t bigint;
  char *character;
  char *variable_character;
  double timestamp;

  value_type = value.GetValueType();

  switch (value_type) {
    case 4:
      smallint = value_peeker.PeekSmallInt(value);
      printf("%d\n", smallint);
      datum = Int16GetDatum(smallint);
      break;

    case 5:
      integer = value_peeker.PeekInteger(value);
      printf("%d\n", integer);
      datum = Int32GetDatum(integer);
      break;

    case 6:
      bigint = value_peeker.PeekBigInt(value);
      printf("%ld\n", bigint);
      datum = Int64GetDatum(bigint);
      break;

      //case 9:		variable_character = CastAsString(value);
      //		datum = CStringGetDatum(variable_character);
      //	break;

    case 11:
      timestamp = value_peeker.PeekTimestamp(value);
      printf("%f\n", (float) timestamp);
      datum = Float8GetDatum((float) timestamp);
      break;

    default:
      break;

      return datum;
  }
}

