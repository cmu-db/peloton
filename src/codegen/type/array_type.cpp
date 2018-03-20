//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_type.cpp
//
// Identification: src/codegen/type/array_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/array_type.h"

#include "codegen/value.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

// TODO: Fill this out

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// The list of types a SQL array type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {};

// Explicit casting rules
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {};

// Comparison operations
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {};

// Unary operators
static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

// Nary operations
static std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// No arg operations
static std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// ARRAY type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Array::Array()
    : SqlType(peloton::type::TypeId::ARRAY),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable, kNaryOperatorTable,
                   kNoArgOperatorTable) {}

Value Array::GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const {
  throw Exception{"Arrays don't have minimum values ...."};
}

Value Array::GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const {
  throw Exception{"Arrays don't have maximum values ...."};
}

Value Array::GetNullValue(CodeGen &codegen) const {
  return Value{*this, codegen.NullPtr(codegen.CharPtrType()),
               codegen.Const32(0), codegen.ConstBool(true)};
}

void Array::GetTypeForMaterialization(
    UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Type *&val_type,
    UNUSED_ATTRIBUTE llvm::Type *&len_type) const {
  // TODO
  throw NotImplementedException{
      "Arrays currently do not have a materialization format. Fix me."};
}

llvm::Function *Array::GetOutputFunction(
    UNUSED_ATTRIBUTE CodeGen &codegen,
    UNUSED_ATTRIBUTE const Type &type) const {
  throw NotImplementedException{"Array's can't be output ... for now ..."};
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton