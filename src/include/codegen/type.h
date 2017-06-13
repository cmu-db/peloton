//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.h
//
// Identification: src/include/codegen/type.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/value.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// This class contains all the type-system functionality for the codegen
// component. In reality, there is tremendous overlap with the Peloton type
// system. In the future, this class should replace that one ...
//
// This class implements an operator table very similar to Postgres. The main
// difference is that we break up operators into four categories: casting,
// comparison, unary, and binary operators.
//===----------------------------------------------------------------------===//
class Type {
 public:
  //===--------------------------------------------------------------------===//
  // Casting operator
  //===--------------------------------------------------------------------===//
  struct Cast {
    // Virtual destructor
    virtual ~Cast() {}

    // Does this cast support casting from the given type to the given type?
    virtual bool SupportsTypes(type::TypeId from_type,
                               type::TypeId to_type) const = 0;

    // Perform the cast on the given value to the provided type
    virtual Value DoCast(CodeGen &codegen, const Value &value,
                         type::TypeId to_type) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // The generic comparison interface for all comparisons between all types
  //===--------------------------------------------------------------------===//
  struct Comparison {
    // Virtual destructor
    virtual ~Comparison() {}

    // Does this instance support comparison of the values of the given left and
    // right SQL types?
    virtual bool SupportsTypes(type::TypeId left_type,
                               type::TypeId right_type) const = 0;

    // Main comparison operators
    virtual Value DoCompareLt(CodeGen &codegen, const Value &left,
                              const Value &right) const = 0;

    virtual Value DoCompareLte(CodeGen &codegen, const Value &left,
                               const Value &right) const = 0;

    virtual Value DoCompareEq(CodeGen &codegen, const Value &left,
                              const Value &right) const = 0;

    virtual Value DoCompareNe(CodeGen &codegen, const Value &left,
                              const Value &right) const = 0;

    virtual Value DoCompareGt(CodeGen &codegen, const Value &left,
                              const Value &right) const = 0;

    virtual Value DoCompareGte(CodeGen &codegen, const Value &left,
                               const Value &right) const = 0;

    // Perform a comparison used for sorting. We need a stable and transitive
    // sorting comparison operator here. The operator returns:
    //  < 0 - if the left value comes before the right value when sorted
    //  = 0 - if the left value is equivalent to the right element
    //  > 0 - if the left value comes after the right value when sorted
    virtual Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                                      const Value &right) const = 0;
  };

  // All builtin operators we currently support
  enum class OperatorId : uint32_t {
    Negation = 0,
    Abs,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
  };
  static const std::string kOpNames[];

  //===--------------------------------------------------------------------===//
  // A unary operator (i.e., an operator that accepts a single argument)
  //===--------------------------------------------------------------------===//
  struct UnaryOperator {
    virtual ~UnaryOperator() {}

    // Does this unary operator support values of the given type?
    virtual bool SupportsType(type::TypeId type_id) const = 0;

    // What is the SQL type of the result of applying the unary operator on a
    // value of the provided type?
    virtual type::TypeId ResultType(
        type::TypeId val_type) const = 0;

    // Apply the operator on the given value
    virtual Value DoWork(CodeGen &codegen, const Value &val) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // A binary operator (i.e., an operator that accepts two arguments)
  //===--------------------------------------------------------------------===//
  struct BinaryOperator {
    virtual ~BinaryOperator() {}
    // Does this binary operator support the two provided input types?
    virtual bool SupportsTypes(type::TypeId left_type,
                               type::TypeId right_type) const = 0;

    // What is the SQL type of the result of applying the binary operator on the
    // provided left and right value types?
    virtual type::TypeId ResultType(
        type::TypeId left_type, type::TypeId right_type) const = 0;

    // Execute the actual operator
    virtual Value DoWork(CodeGen &codegen, const Value &left,
                         const Value &right, Value::OnError on_error) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Get the storage size in bytes of the given type
  static uint32_t GetFixedSizeForType(type::TypeId type_id);

  // Is the given type variable length?
  static bool IsVariableLength(type::TypeId type_id) {
    return type_id == type::TypeId::VARCHAR ||
           type_id == type::TypeId::VARBINARY;
  }

  // Is the given type an integral type (i.e., tinyint to bigint)
  static bool IsIntegral(type::TypeId type_id);

  // Is the given type a numeric (real, decimal, numeric etc.)
  static bool IsNumeric(type::TypeId type_id) {
    return type_id == type::TypeId::DECIMAL;
  }

  // Can values of the provided type be implicitly casted into a value of the
  // other provided type?
  static bool CanImplicitlyCastTo(type::TypeId from_type,
                                  type::TypeId to_type);

  // Get the min, max, null, and default value for the given type
  static Value GetMinValue(CodeGen &codegen, type::TypeId type_id);
  static Value GetMaxValue(CodeGen &codegen, type::TypeId type_id);
  static Value GetNullValue(CodeGen &codegen, type::TypeId type_id);
  static Value GetDefaultValue(CodeGen &codegen, type::TypeId type_id);

  // Get the LLVM types used to materialize a SQL value of the given type
  static void GetTypeForMaterialization(CodeGen &codegen,
                                        type::TypeId type_id,
                                        llvm::Type *&val_type,
                                        llvm::Type *&len_type);

  // Lookup comparison handler for the given type
  static const Cast *GetCast(type::TypeId from_type,
                             type::TypeId to_type);

  // Lookup comparison handler for the given type
  static const Comparison *GetComparison(type::TypeId left_type,
                                         type::TypeId &left_casted_type,
                                         type::TypeId right_type,
                                         type::TypeId &right_casted_type);

  // Lookup the given binary operator that works on the left and right types
  static const BinaryOperator *GetBinaryOperator(
      OperatorId op_id, type::TypeId left_type,
      type::TypeId &left_casted_type, type::TypeId right_type,
      type::TypeId &right_casted_type);

 private:
  struct OperatorIdHasher {
    size_t operator()(const OperatorId &func_id) const {
      return std::hash<uint32_t>{}(static_cast<uint32_t>(func_id));
    }
  };

  typedef std::unordered_map<type::TypeId,
                             std::vector<type::TypeId>,
                             type::Type::TypeIdHasher> ImplicitCastTable;

  typedef std::unordered_map<type::TypeId, std::vector<const Cast *>,
                             type::Type::TypeIdHasher> CastingTable;

  typedef std::unordered_map<type::TypeId,
                             std::vector<const Comparison *>,
                             type::Type::TypeIdHasher> ComparisonTable;

  typedef std::unordered_map<OperatorId, std::vector<const UnaryOperator *>,
                             OperatorIdHasher> UnaryOperatorTable;

  typedef std::unordered_map<OperatorId, std::vector<const BinaryOperator *>,
                             OperatorIdHasher> BinaryOperatorTable;

 private:
  static ImplicitCastTable kImplicitCastsTable;

  // The table of casting functions
  static CastingTable kCastingTable;

  // The comparison table
  static ComparisonTable kComparisonTable;

  // The table of builtin unary operators
  static UnaryOperatorTable kBuiltinUnaryOperatorsTable;

  // The table of builtin binary operators
  static BinaryOperatorTable kBuiltinBinaryOperatorsTable;
};

}  // namespace codegen
}  // namespace peloton
