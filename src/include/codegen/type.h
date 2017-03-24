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
// difference is that we break up operators into three categories: comparison
// operators, unary operators and binary operators.
//===----------------------------------------------------------------------===//
class Type {
 public:
  //===--------------------------------------------------------------------===//
  // The generic comparison interface for all comparisons between all types
  //===--------------------------------------------------------------------===//
  struct Comparison {
    // Virtual destructor
    virtual ~Comparison() {}

    // Main comparison operators
    virtual Value DoCompareLt(CodeGen &codegen, const Value &left,
                              const Value &right) const;

    virtual Value DoCompareLte(CodeGen &codegen, const Value &left,
                               const Value &right) const;

    virtual Value DoCompareEq(CodeGen &codegen, const Value &left,
                              const Value &right) const;

    virtual Value DoCompareNe(CodeGen &codegen, const Value &left,
                              const Value &right) const;

    virtual Value DoCompareGt(CodeGen &codegen, const Value &left,
                              const Value &right) const;

    virtual Value DoCompareGte(CodeGen &codegen, const Value &left,
                               const Value &right) const;

    // Perform a comparison used for sorting. We need a stable and transitive
    // sorting comparison operator here. The operator returns:
    //  < 0 - if the left value comes before the right value when sorted
    //  = 0 - if the left value is equivalent to the right element
    //  > 0 - if the left value comes after the right value when sorted
    virtual Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                                      const Value &right) const;
  };

  // All unary operators we currently support
  enum class UnaryOperatorId : uint32_t {
    Negation = 0,
    Abs,
  };
  static const std::string kUnaryOpNames[];

  // All binary operators we currently support
  enum class BinaryOperatorId : uint32_t {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
  };
  static const std::string kBinaryOpNames[];

  //===--------------------------------------------------------------------===//
  // A unary operator (i.e., an operator that accepts a single argument)
  //===--------------------------------------------------------------------===//
  struct UnaryOperator {
    virtual Value DoWork(CodeGen &codegen, const Value &val) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // A binary operator (i.e., an operator that accepts two arguments)
  //===--------------------------------------------------------------------===//
  struct BinaryOperator {
    // Does this binary operator support the two provided input types?
    virtual bool SupportsTypes(type::Type::TypeId left_type,
                               type::Type::TypeId right_type) const {
      return left_type == right_type;
    }

    // Execute the actual operator
    virtual Value DoWork(CodeGen &codegen, const Value &left,
                         const Value &right) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Is the given type variable length?
  static bool HasVariableLength(type::Type::TypeId type_id);

  // Is the given type an integral type (i.e., tinyint to bigint)
  static bool IsIntegral(type::Type::TypeId type_id);

  // Lookup comparison handler for the given type
  static const Comparison *GetComparison(type::Type::TypeId type_id);

  // Lookup the given binary operator that works on the left and right types
  static const BinaryOperator *GetBinaryOperator(BinaryOperatorId op_id,
                                                 type::Type::TypeId left_type,
                                                 type::Type::TypeId right_type);

 private:
  // The comparison table
  static std::vector<const Comparison *> kComparisonTable;

  struct OperatorIdHasher {
    template <typename T>
    size_t operator()(const T &func_id) const {
      return std::hash<uint32_t>{}(static_cast<uint32_t>(func_id));
    }
  };

  typedef std::unordered_map<UnaryOperatorId,
                             std::vector<const UnaryOperator *>,
                             OperatorIdHasher> UnaryOperatorTable;

  typedef std::unordered_map<BinaryOperatorId,
                             std::vector<const BinaryOperator *>,
                             OperatorIdHasher> BinaryOperatorTable;

  // The table of builtin unary operators
  static BinaryOperatorTable kBuiltinUnaryOperatorsTable;

  // The table of builtin binary operators
  static BinaryOperatorTable kBuiltinBinaryOperatorsTable;
};

}  // namespace codegen
}  // namespace peloton