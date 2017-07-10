//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_type.cpp
//
// Identification: src/codegen/type/sql_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/sql_type.h"

#include "codegen/value.h"
#include "codegen/type/array_type.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/date_type.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/smallint_type.h"
#include "codegen/type/timestamp_type.h"
#include "codegen/type/tinyint_type.h"
#include "codegen/type/varbinary_type.h"
#include "codegen/type/varchar_type.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {
namespace type {

class Invalid : public SqlType, public Singleton<Invalid> {
 public:
  bool IsVariableLength() const override {
    throw Exception{"INVALID type know if it is variable in length"};
  }

  Value GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"INVALID type doesn't have a minimum value"};
  }

  Value GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"INVALID type doesn't have a maximum value"};
  }

  Value GetNullValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"INVALID type doesn't have a NULL value"};
  }

  void GetTypeForMaterialization(
      UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Type *&val_type,
      UNUSED_ATTRIBUTE llvm::Type *&len_type) const override {
    throw Exception{"INVALID type doesn't have a materialization type"};
  }

  llvm::Function *GetOutputFunction(
      UNUSED_ATTRIBUTE CodeGen &codegen,
      UNUSED_ATTRIBUTE const Type &type) const override {
    throw Exception{"INVALID type does not have an output function"};
  }

  const TypeSystem &GetTypeSystem() const override {
    throw Exception{"INVALID type doesn't have a type system"};
  }

 private:
  friend class Singleton<Invalid>;

  Invalid() : SqlType(peloton::type::TypeId::INVALID) {}
};

namespace {

// The order of elements here **must** be the same as type::TypeId
static const SqlType *kTypeTable[] = {
    &Invalid::Instance(),    // The invalid type
    &Invalid::Instance(),    // The parameter offset type ... which isn't a real
                             // SQL type
    &Boolean::Instance(),    // The boolean type
    &TinyInt::Instance(),    // The tinyint type (1 byte)
    &SmallInt::Instance(),   // The smallint type (2 byte)
    &Integer::Instance(),    // The integer type (4 byte)
    &BigInt::Instance(),     // The bigint type (8 byte)
    &Decimal::Instance(),    // The decimal type (8 byte)
    &Timestamp::Instance(),  // The timestamp type (8 byte)
    &Date::Instance(),       // The date type (4 byte)
    &Varchar::Instance(),    // The varchar type
    &Varbinary::Instance(),  // The varbinary type
    &Array::Instance(),      // The array type
    &Invalid::Instance()     // A user-defined type
};

}  // anonymous namespace

const SqlType &SqlType::LookupType(peloton::type::TypeId type_id) {
  return *kTypeTable[static_cast<uint32_t>(type_id)];
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
