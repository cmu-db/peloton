//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_system.h
//
// Identification: src/include/codegen/type/type_system.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "common/macros.h"
#include "common/internal_types.h"

namespace llvm {
class Value;
}  // namespace llvm

namespace peloton {
namespace codegen {

class CodeGen;
class RuntimeState;
class Value;

namespace type {

class Type;

//===----------------------------------------------------------------------===//
// This class contains all the type-system functionality for the codegen
// component. In reality, there is tremendous overlap with the Peloton type
// system. In the future, this class should replace that one ...
//
// This class implements an operator table similar in spirit to Postgres. In
// Peloton, operators are categorized as casting, comparison, unary or binary
// operators. Every SQL type configures a TypeSystem with supported operators.
// As in Postgres, each operator can be configured to be overriden by a user-
// provided implementation.
//===----------------------------------------------------------------------===//
class TypeSystem {
 public:
  //===--------------------------------------------------------------------===//
  //
  // Casting operation
  //
  //===--------------------------------------------------------------------===//
  class Cast {
   public:
    virtual ~Cast() = default;

    // Does this cast support casting from the given type to the given type?
    virtual bool SupportsTypes(const Type &from_type,
                               const Type &to_type) const = 0;

    // Cast the given value to the provided type
    virtual Value Eval(CodeGen &codegen, const Value &value,
                       const Type &to_type) const = 0;
  };

  //===--------------------------------------------------------------------===//
  //
  // CastHandleNull
  //
  // An abstract base class for cast operations. This class performs common,
  // generic NULL checking logic for most cast operations. Derived classes
  // implement Impl() and assume non-null inputs.
  //
  // If the input is not NULL-able, no NULL check is performs, and the derived
  // classes implementation is invoked.
  //
  // If the input is NULLable, an if-then-else construct is generated to perform
  // the NULL check. Derived classes are invoked in the non-null branch.
  //
  //===--------------------------------------------------------------------===//
  class CastHandleNull : public TypeSystem::Cast {
   public:
    Value Eval(CodeGen &codegen, const Value &value,
               const Type &to_type) const override;

   protected:
    // Perform the cast assuming the input is not NULLable
    virtual Value Impl(CodeGen &codegen, const Value &value,
                       const Type &to_type) const = 0;
  };

  struct CastInfo {
    peloton::type::TypeId from_type;
    peloton::type::TypeId to_type;
    const Cast &cast_operation;
  };

  //===--------------------------------------------------------------------===//
  //
  // The generic comparison interface for all comparisons between all types
  //
  //===--------------------------------------------------------------------===//
  class Comparison {
   public:
    // Virtual destructor
    virtual ~Comparison() = default;

    // Does this instance support comparison of the values of the given left and
    // right SQL types?
    virtual bool SupportsTypes(const Type &left_type,
                               const Type &right_type) const = 0;

    // Main comparison operators
    virtual Value EvalCompareLt(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value EvalCompareLte(CodeGen &codegen, const Value &left,
                                 const Value &right) const = 0;
    virtual Value EvalCompareEq(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value EvalCompareNe(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value EvalCompareGt(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value EvalCompareGte(CodeGen &codegen, const Value &left,
                                 const Value &right) const = 0;

    // Perform a comparison used for sorting. We need a stable and transitive
    // sorting comparison operator here. The operator returns:
    //  < 0 - if the left value comes before the right value when sorted
    //  = 0 - if the left value is equivalent to the right element
    //  > 0 - if the left value comes after the right value when sorted
    virtual Value EvalCompareForSort(CodeGen &codegen, const Value &left,
                                     const Value &right) const = 0;
  };

  //===--------------------------------------------------------------------===//
  //
  // SimpleComparisonHandleNull
  //
  // An abstract base class for comparison operations. This class computes the
  // NULL bit based on arguments and **always** invokes the derived
  // implementation to perform the comparison. This means that derived classes
  // may operate on SQL NULL values. For simple comparisons, i.e., numeric
  // comparisons, this is safe since the values will always be valid, though
  // potentially garbage). The resulting boolean value is still marked with the
  // correct NULL bit, thus hiding the (potentially) garbage comparison.
  //
  // This class isn't safe for string comparisons since strings  For string
  // comparisons, this is not safe because the string can take on C/C++ NULL,
  // and SEGFAULT.
  //
  //===--------------------------------------------------------------------===//
  class SimpleComparisonHandleNull : public Comparison {
   public:
    Value EvalCompareLt(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareLte(CodeGen &codegen, const Value &left,
                         const Value &right) const override;
    Value EvalCompareEq(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareNe(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareGt(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareGte(CodeGen &codegen, const Value &left,
                         const Value &right) const override;
    Value EvalCompareForSort(CodeGen &codegen, const Value &left,
                             const Value &right) const override;

   protected:
    // The non-null comparison implementations
    virtual Value CompareLtImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareLteImpl(CodeGen &codegen, const Value &left,
                                 const Value &right) const = 0;
    virtual Value CompareEqImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareNeImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareGtImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareGteImpl(CodeGen &codegen, const Value &left,
                                 const Value &right) const = 0;
    virtual Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                                     const Value &right) const = 0;
  };

  //===--------------------------------------------------------------------===//
  //
  // ExpensiveComparisonHandleNull
  //
  // An abstract base class for comparison operations. This class correctly
  // computes the NULL bit, but generates a full-blown if-then-else clause
  // (hence the "expensive" part) to perform the NULL check. Thus, derived
  // comparison implementations are assured that arguments are not NULL-able.
  // If the arguments are non-NULL-able, then the NULL check is elided.
  //
  //===--------------------------------------------------------------------===//
  class ExpensiveComparisonHandleNull : public Comparison {
   public:
    Value EvalCompareLt(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareLte(CodeGen &codegen, const Value &left,
                         const Value &right) const override;
    Value EvalCompareEq(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareNe(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareGt(CodeGen &codegen, const Value &left,
                        const Value &right) const override;
    Value EvalCompareGte(CodeGen &codegen, const Value &left,
                         const Value &right) const override;
    Value EvalCompareForSort(CodeGen &codegen, const Value &left,
                             const Value &right) const override;

   protected:
    // The non-null comparison implementations
    virtual Value CompareLtImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareLteImpl(CodeGen &codegen, const Value &left,
                                 const Value &right) const = 0;
    virtual Value CompareEqImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareNeImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareGtImpl(CodeGen &codegen, const Value &left,
                                const Value &right) const = 0;
    virtual Value CompareGteImpl(CodeGen &codegen, const Value &left,
                                 const Value &right) const = 0;
    virtual Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                                     const Value &right) const = 0;
  };

  struct ComparisonInfo {
    // The operation
    const Comparison &comparison;
  };

  // Every operator invocation carries this context to provide additional
  // contextual information to the function.
  struct InvocationContext {
    // TODO(pmenon): Fill me
    // What to do if an error occurs during execution?
    OnError on_error;
    // The execution context
    llvm::Value *executor_context;
  };

  //===--------------------------------------------------------------------===//
  // An operator with no argument
  //===--------------------------------------------------------------------===//
  struct NoArgOperator {
    virtual ~NoArgOperator() = default;

    // Does this unary operator support values of the given type?
    bool SupportsType(const UNUSED_ATTRIBUTE Type &type) const { return false; }

    // What is the SQL type of the result of applying the unary operator on a
    // value of the provided type?
    virtual Type ResultType(const Type &val_type) const = 0;

    // Apply the operator on the given value
    virtual Value Eval(CodeGen &codegen,
                       const InvocationContext &ctx) const = 0;
  };

  struct NoArgOpInfo {
    // The ID of the operation
    OperatorId op_id;
    // The operation
    const NoArgOperator &no_arg_operation;
  };

  //===--------------------------------------------------------------------===//
  //
  // A unary operator (i.e., an operator that accepts a single argument)
  //
  //===--------------------------------------------------------------------===//
  class UnaryOperator {
   public:
    virtual ~UnaryOperator() = default;

    // Is this input type supported by this operator?
    virtual bool SupportsType(const Type &type) const = 0;

    // The result type of the operator
    virtual Type ResultType(const Type &val_type) const = 0;

    // Apply the operator on the given value
    virtual Value Eval(CodeGen &codegen, const Value &val,
                       const InvocationContext &ctx) const = 0;
  };

  //===--------------------------------------------------------------------===//
  //
  // UnaryOperatorHandleNull
  //
  // An abstract base class for unary operators that returns NULL if the input
  // is NULL. If the input is not NULL, derived implementations are invoked to
  // execute the operator logic.
  //
  // If the input is not NULL-able, no NULL check is performed.
  //
  // If the input is NULL-able, an if-then-else clause is created, and derived
  // implementations are called in the non-NULL branch.
  //
  //===--------------------------------------------------------------------===//
  class UnaryOperatorHandleNull : public UnaryOperator {
   public:
    Value Eval(CodeGen &codegen, const Value &val,
               const InvocationContext &ctx) const override;

   protected:
    // The actual implementation assuming non-NULL input
    virtual Value Impl(CodeGen &codegen, const Value &val,
                       const InvocationContext &ctx) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // Metadata structure capturing an operator ID and an instance
  //===--------------------------------------------------------------------===//
  struct UnaryOpInfo {
    // The ID of the operation
    OperatorId op_id;
    // The operation
    const UnaryOperator &unary_operation;
  };

  //===--------------------------------------------------------------------===//
  //
  // A binary operator (i.e., an operator that accepts two arguments)
  //
  //===--------------------------------------------------------------------===//
  class BinaryOperator {
   public:
    virtual ~BinaryOperator() = default;

    // Are these input types supported by this operator?
    virtual bool SupportsTypes(const Type &left_type,
                               const Type &right_type) const = 0;

    // The SQL type of the result of this operator
    virtual Type ResultType(const Type &left_type,
                            const Type &right_type) const = 0;

    // Execute the actual operator
    virtual Value Eval(CodeGen &codegen, const Value &left, const Value &right,
                       const InvocationContext &ctx) const = 0;
  };

  //===--------------------------------------------------------------------===//
  //
  // An abstract base class for binary operators that returns NULL if either
  // input argument is NULL. If neither input is NULL, derived implementations
  // are called (through Impl()) to execute operator logic.
  //
  // If the input is not NULL-able, no NULL check is performed.
  //
  // If the input is NULL-able, an if-then-else clause is created, and derived
  // implementations are called in the non-NULL branch.
  //
  //===--------------------------------------------------------------------===//
  struct BinaryOperatorHandleNull : public BinaryOperator {
   public:
    Value Eval(CodeGen &codegen, const Value &left, const Value &right,
               const InvocationContext &ctx) const override;

   protected:
    // The implementation assuming non-nullable types
    virtual Value Impl(CodeGen &codegen, const Value &left, const Value &right,
                       const InvocationContext &ctx) const = 0;
  };

  struct BinaryOpInfo {
    // The ID of the operation
    OperatorId op_id;

    // The operation
    const BinaryOperator &binary_operation;
  };

  // An n-ary function
  class NaryOperator {
   public:
    virtual ~NaryOperator() = default;

    // Does this operator support the provided input argument types?
    virtual bool SupportsTypes(const std::vector<Type> &arg_types) const = 0;

    // What is type of the result produced by this operator?
    virtual Type ResultType(const std::vector<Type> &arg_types) const = 0;

    // Execute the actual operator
    virtual Value Eval(CodeGen &codegen, const std::vector<Value> &input_args,
                       const InvocationContext &ctx) const = 0;
  };

  struct NaryOpInfo {
    // The ID of the operation
    OperatorId op_id;

    // The operation
    const NaryOperator &nary_operation;
  };

 public:
  TypeSystem(const std::vector<peloton::type::TypeId> &implicit_cast_table,
             const std::vector<CastInfo> &explicit_cast_table,
             const std::vector<ComparisonInfo> &comparison_table,
             const std::vector<UnaryOpInfo> &unary_op_table,
             const std::vector<BinaryOpInfo> &binary_op_table,
             const std::vector<NaryOpInfo> &nary_op_table,
             const std::vector<NoArgOpInfo> &no_arg_op_table);

  // Can values of the provided type be implicitly casted into a value of the
  // other provided type?
  static bool CanImplicitlyCastTo(const Type &from_type, const Type &to_type);

  // Lookup comparison handler for the given type
  static const Cast *GetCast(const Type &from_type, const Type &to_type);

  // Lookup comparison handler for the given type
  static const Comparison *GetComparison(const Type &left_type,
                                         Type &left_casted_type,
                                         const Type &right_type,
                                         Type &right_casted_type);

  // Lookup the given binary operator that works on the left and right types
  static const UnaryOperator *GetUnaryOperator(OperatorId op_id,
                                               const Type &input_type);

  // Lookup the given binary operator that works on the left and right types
  static const BinaryOperator *GetBinaryOperator(OperatorId op_id,
                                                 const Type &left_type,
                                                 Type &left_casted_type,
                                                 const Type &right_type,
                                                 Type &right_casted_type);

  // Lookup the given nary operator that operators on the provided types
  static const NaryOperator *GetNaryOperator(
      OperatorId op_id, const std::vector<Type> &arg_types);

 private:
  // The list of types a given type can be implicitly casted to
  const std::vector<peloton::type::TypeId> &implicit_cast_table_;

  // The table of explicit casting functions
  const std::vector<CastInfo> &explicit_cast_table_;

  // The comparison table
  const std::vector<ComparisonInfo> &comparison_table_;

  // The table of builtin unary operators
  const std::vector<UnaryOpInfo> &unary_op_table_;

  // The table of builtin binary operators
  const std::vector<BinaryOpInfo> &binary_op_table_;

  // The table of builtin nary operators
  const std::vector<NaryOpInfo> &nary_op_table_;

  // The table of builtin no-arg operators
  const std::vector<NoArgOpInfo> &no_arg_op_table_;

 private:
  DISALLOW_COPY_AND_MOVE(TypeSystem);
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton