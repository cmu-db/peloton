/*-------------------------------------------------------------------------
 *
 * format_transformer.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl/format_transformer.cpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"

namespace peloton {
namespace bridge {


class PostgresValueFormat;
class PelotonValueFormat;

//===--------------------------------------------------------------------===//
// Format Transformer
//===--------------------------------------------------------------------===//

class FormatTransformer {

public:
  FormatTransformer(const FormatTransformer &) = delete;
  FormatTransformer &operator=(const FormatTransformer &) = delete;
  FormatTransformer(FormatTransformer &&) = delete;
  FormatTransformer &operator=(FormatTransformer &&) = delete;

  static PelotonValueFormat TransformValueFormat(PostgresValueFormat postgresValueFormat);
};

class PostgresValueFormat{
  public:
    PostgresValueFormat(oid_t postgres_value_type_id,
                        int postgres_column_length,
                        int postgres_typemod)
                        : postgres_value_type_id(postgres_value_type_id),
                          postgres_column_length(postgres_column_length), 
                          postgres_typemod(postgres_typemod){}

    oid_t GetTypeId() { return postgres_value_type_id; }
    int GetLength() { return postgres_column_length; }
    int GetTypeMod() { return postgres_typemod; }

  private:

    oid_t postgres_value_type_id;
    int postgres_column_length;
    int postgres_typemod;
};

class PelotonValueFormat{
  public:
    PelotonValueFormat(ValueType peloton_value_type,
                        int peloton_column_length,
                        bool peloton_is_inlined)
                        : peloton_value_type(peloton_value_type),
                          peloton_column_length(peloton_column_length), 
                          peloton_is_inlined(peloton_is_inlined) {}

    PelotonValueFormat& operator=(const PelotonValueFormat& rhs){
      peloton_value_type = rhs.peloton_value_type;
      peloton_column_length = rhs.peloton_column_length;
      peloton_is_inlined = rhs.peloton_is_inlined;
      return *this;
    }

    ValueType GetType() { return peloton_value_type; }
    int GetLength() { return peloton_column_length; }
    bool IsInlined() { return peloton_is_inlined; }

  private:
    ValueType peloton_value_type;
    int peloton_column_length;
    bool peloton_is_inlined;
};

}  // End bridge namespace
}  // End peloton namespace

