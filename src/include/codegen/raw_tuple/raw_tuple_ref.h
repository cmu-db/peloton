//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// raw_tuple_ref.h
//
// Identification: src/include/codegen/raw_tuple/raw_tuple_ref.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/schema.h"
#include "planner/attribute_info.h"
#include "planner/binding_context.h"
#include "codegen/row_batch.h"
#include "codegen/type.h"

namespace peloton {
namespace codegen {

/**
 * @brief This class is used to materialize a @c RowBatch::Row into a @c storage::Tuple.
 */
class RawTupleRef {
 public:
  RawTupleRef(CodeGen &codegen, RowBatch::Row &row,
              planner::BindingContext &binding_context) {
  }

  void Materialize(oid_t column_id) {
    size_t offset = this->schema_->GetOffset(column_id);

    const planner::AttributeInfo *attrib_info = this->binding_context_.Find(column_id);

    codegen::Value v = this->row_.DeriveValue(codegen, attrib_info);

    llvm::Value *ptr = this->codegen_->CreateConstInBoundsGEP1_32(
        this->codegen_.ByteType(),
        this->data_,
        offset
    );

    llvm::Type *val_type;
    llvm::Type *len_type;
    Type::GetTypeForMaterialization(this->codegen_, v.GetType(), val_type, len_type);

    ptr = this->codegen_->CreateBitCast(ptr, val_type->getPointerTo());

    switch (v.GetType()) {
      case type::Type::TypeId::TINYINT: {
        this->codegen_->CreateStore(v.GetValue(), ptr);
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        this->codegen_->CreateStore(v.GetValue(), ptr);
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        break;
      }
      case type::Type::TypeId::TIMESTAMP: {
        break;
      }
      case type::Type::TypeId::BIGINT: {
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        break;
      }
      case type::Type::TypeId::VARBINARY: {
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        break;
      }
      default: {
        std::string msg =
            StringUtil::Format("Can't serialize value type '%s' at position %u",
                               TypeIdToString(val.GetType()).c_str(), i);
        throw Exception{msg};
      }
    }
  }

  // MaterializeTinyInt(char *ptr, int8_t val)
  void MaterializeTinyInt(llvm::Value *ptr, llvm::Value *val) {

  }

  // MaterializeSmallInt(char *ptr, int16_t val)
  void MaterializeSmallInt(llvm::Value *ptr, llvm::Value *val) {

  }

  // MaterializeInteger(char *ptr, int32_t val)
  void MaterializeInteger(llvm::Value *ptr, llvm::Value *val) {
  }

  // MaterializeBigInt(char *ptr, int64_t val)
  void MaterializeBigInt(llvm::Value *ptr, llvm::Value *val) {

  }

  // MaterializeTimestamp(char *ptr, int64_t val)
  void MaterializeTimestamp(llvm::Value *ptr, llvm::Value *val) {

  }

  // MaterializeDecimal(char *ptr, double val)
  void MaterializeDecimal(llvm::Value *ptr, llvm::Value *val) {

  }

  // MaterializeVarchar(char *ptr, char *str, uint32_t len)
  void MaterializeVarchar(llvm::Value *ptr, llvm::Value *str, llvm::Value *len) {

  }

  // MaterializeVarbinary(char *ptr, char *str, uint32_t len)
  void MaterializeVarbinary(llvm::Value *ptr, llvm::Value *str, llvm::Value *len) {

  }

 private:
  CodeGen &codegen_;
  RowBatch::Row &row_;
  planner::BindingContext &binding_context_;
  llvm::Value *data_;
};

}  // namespace codegen
}  // namespace peloton
