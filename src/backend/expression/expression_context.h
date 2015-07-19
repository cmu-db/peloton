/*
 * expression_context.h
 *
 */

#include "backend/common/value_vector.h"

namespace peloton {
namespace expression {

/**
 * @brief Stores per-execution context for expression evaluation.
 */
class ExpressionContext {
 public:
  ExpressionContext(const ExpressionContext &) = delete;
  ExpressionContext& operator=(const ExpressionContext&) = delete;
  ExpressionContext(const ExpressionContext &&) = delete;
  ExpressionContext& operator=(const ExpressionContext&&) = delete;

  ExpressionContext(ValueArray &params)
    : params_(params){

  }


  const ValueArray& GetParams() const {
    return params_;
  }

 private:
  ValueArray params_;
};


} // namespace expression
} // namespace peloton
