//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_type.cpp
//
// Identification: src/codegen/type/array_type.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/array_type.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

// TODO: Fill this out

//===----------------------------------------------------------------------===//
// TYPE SYSTEM CONSTRUCTION
//===----------------------------------------------------------------------===//

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

}  // anonymous namespace

// Initialize the BIGINT SQL type with the configured type system
Array::Array()
    : SqlType(peloton::type::TypeId::ARRAY),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable) {}

}  // namespace type
}  // namespace codegen
}  // namespace peloton