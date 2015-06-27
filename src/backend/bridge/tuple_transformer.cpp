/*
 * tupleTransformer.cpp
 *
 *  Created on: Jun 22, 2015
 *      Author: vivek
 */

#include <iostream>
//#include <cstring>

#include "backend/bridge/tuple_transformer.h"
#include "backend/common/value_peeker.h"
#include "backend/storage/tuple.h"
#include "backend/common/types.h"
#include "backend/bridge/ddl.h"

extern "C" {
#include "fmgr.h"
#include "utils/lsyscache.h"
}
void TestTupleTransformer(Datum datum, Oid atttypid) {

  peloton::Value p_value;
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

peloton::Value DatumGetValue(Datum datum, Oid atttypid) {
  peloton::Value value;
  peloton::Pool *data_pool = nullptr;
  int16_t smallint;
  int32_t integer;
  int64_t bigint;
  char *character;
  char *variable_character;
  long int timestamp;
  char *timestamp_charpointer;

  switch (atttypid) {
    case 21:
      smallint = DatumGetInt16(datum);
      printf("%d\n", smallint);
      value = peloton::ValueFactory::GetSmallIntValue(smallint);
      break;

    case 23:
      integer = DatumGetInt32(datum);
      printf("%d\n", integer);
      value = peloton::ValueFactory::GetIntegerValue(integer);
      break;

    case 20:
      bigint = DatumGetInt64(datum);
      printf("%ld\n", bigint);
      value = peloton::ValueFactory::GetBigIntValue(bigint);
      break;

    case 1042:
      character = DatumGetCString(datum);
      printf("%s\n", character);
      value = peloton::ValueFactory::GetStringValue(character, data_pool);
      break;

    case 1043:
      variable_character = DatumGetCString(datum);
      printf("%s\n", variable_character);
      value = peloton::ValueFactory::GetStringValue(variable_character,
                                                   data_pool);
      break;

    case 1114:
      timestamp = DatumGetInt64(datum);
      timestamp_charpointer = DatumGetCString(datum);
      printf("%s\n", timestamp_charpointer);
      value = peloton::ValueFactory::GetTimestampValue(timestamp);
      break;

    default:
      break;

  }
  return value;
}

Datum
ValueGetDatum(peloton::Value value) {
	peloton::ValueType value_type;
	peloton::ValuePeeker value_peeker;
	Datum datum;
	int16_t smallint;
	int32_t integer;
	int64_t bigint;
	double double_precision;
	char *character;
	std::string variable_character_string;
	char *variable_character;
	long int timestamp;
	//char *timestamp_charpointer;

	value_type = value.GetValueType();

	switch (value_type) {
		//small int
		case 4:
			smallint = value_peeker.PeekSmallInt(value);
			printf("%d\n", smallint);
			datum = Int16GetDatum(smallint);
			break;
		// integer
		case 5:
			integer = value_peeker.PeekInteger(value);
			printf("%d\n", integer);
			datum = Int32GetDatum(integer);
			break;
		// big int
		case 6:
			bigint = value_peeker.PeekBigInt(value);
			printf("%ld\n", bigint);
			datum = Int64GetDatum(bigint);
			break;

		//double
		case 8:
			double_precision = value_peeker.PeekDouble(value);
			printf("%f\n", double_precision);
			datum = Float8GetDatum(double_precision);
			break;

		// varchar
		case 9:
			variable_character = (char *)value_peeker.PeekObjectValue(value);
			printf("%s\n", variable_character);
			datum = CStringGetDatum(variable_character);
			break;

		// timestamp
		case 11:
			timestamp = value_peeker.PeekTimestamp(value);
			datum = Int64GetDatum(timestamp);
			printf("%s\n",DatumGetCString(timestamp));
			break;

		default:
		  break;

		return datum;
  }
}

namespace peloton {
namespace bridge {
/* @brief convert a Postgres tuple into Peloton tuple
 * @param slot Postgres tuple
 * @param schema Peloton scheme of the table to which the tuple belongs
 * @return a Peloton tuple
 */
storage::Tuple *TupleTransformer(TupleTableSlot * slot,
                                 const catalog::Schema *schema) {
  TupleDesc typeinfo = slot->tts_tupleDescriptor;
  int natts = typeinfo->natts;
  int i;
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

  for (i = 0; i < natts; ++i) {
    attr = slot_getattr(slot, i + 1, &isnull);
    if (isnull)
      continue;
    Assert(typeinfo != NULL);
    Assert(typeinfo->attrs[i] != NULL);
    getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &typoutput, &typisvarlena);

    value = OidOutputFunctionCall(typoutput, attr);

    // Print the attribute.
    attributeId = (unsigned) i + 1;
    attributeP = typeinfo->attrs[i];

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
  assert(vals.size() == oid_t_vec.size()); /* the num of value should be the same as schema found */
  catalog::Schema * tuple_schema = catalog::Schema::CopySchema(schema,
                                                               oid_t_vec);
	storage::Tuple *tuple(new storage::Tuple(tuple_schema, true));
	i = 0;
	for (auto val : vals) {
	  tuple->SetValue(i++, val);
	}
	return tuple;
}
}
}
