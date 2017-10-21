//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_storage.cpp
//
// Identification: src/codegen/table_storage.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/table_storage.h"

#include "catalog/schema.h"
#include "codegen/lang/if.h"
#include "codegen/proxy/tuple_runtime_proxy.h"
#include "codegen/type/sql_type.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

void TableStorage::StoreValues(CodeGen &codegen, llvm::Value *tuple_ptr,
    const std::vector<codegen::Value> &values, llvm::Value *pool) const {
  for (oid_t i = 0; i < schema_.GetColumnCount(); i++) {
    auto offset = schema_.GetOffset(i);
    auto *ptr = codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(),
                                                    tuple_ptr, offset);
    auto &value = values[i];
    auto &sql_type = value.GetType().GetSqlType();
    llvm::Type *val_type, *len_type;
    sql_type.GetTypeForMaterialization(codegen, val_type, len_type);

    if (sql_type.IsVariableLength()) {
      PL_ASSERT(value.GetLength() != nullptr);
      auto val_ptr = codegen->CreateBitCast(ptr, val_type);
      lang::If value_is_null{codegen, value.IsNull(codegen)};
      {
        auto null_val = sql_type.GetNullValue(codegen);
        codegen.Call(TupleRuntimeProxy::CreateVarlen,
            {null_val.GetValue(), null_val.GetLength(), val_ptr, pool});
      }
      value_is_null.ElseBlock();
      {
        codegen.Call(TupleRuntimeProxy::CreateVarlen,
                     {value.GetValue(), value.GetLength(), val_ptr, pool});
      }
      value_is_null.EndIf();
    } else {
      auto val_ptr = codegen->CreateBitCast(ptr, val_type->getPointerTo());
      lang::If value_is_null{codegen, value.IsNull(codegen)};
      {
        auto null_val = sql_type.GetNullValue(codegen);
        codegen->CreateStore(null_val.GetValue(), val_ptr);
      }
      value_is_null.ElseBlock();
      {
        codegen->CreateStore(value.GetValue(), val_ptr);
      }
      value_is_null.EndIf();
    }
  }
}

}  // namespace codegen
}  // namespace peloton
