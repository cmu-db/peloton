/*-------------------------------------------------------------------------
 *
 * abstract_expression.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/expression/abstract_expression.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>

#include "common/types.h"
#include "common/value.h"
#include "common/value_vector.h"
#include <json_spirit.h>

namespace nstore {
namespace expression {

class SerializeInput;
class SerializeOutput;

/**
 * Predicate objects for filtering tuples during query execution.
 * These objects are stored in query plans and passed to Storage Access Manager.
 */

// ------------------------------------------------------------------
// AbstractExpression
// Base class for all expression nodes
// ------------------------------------------------------------------

class AbstractExpression {
  public:
    /** destroy this node and all children */
    virtual ~AbstractExpression();

    virtual Value eval(const storage::Tuple *tuple1, const storage::Tuple *tuple2) const = 0;

    /** set parameter values for this node and its descendents */
    virtual void substitute(const ValueArray &params);

    /** return true if self or descendent should be substitute()'d */
    virtual bool hasParameter() const;

    /* debugging methods - some various ways to create a sring
       describing the expression tree */
    std::string debug() const;
    std::string debug(bool traverse) const;
    std::string debug(const std::string &spacer) const;
    virtual std::string debugInfo(const std::string &spacer) const = 0;

    /* serialization methods. expressions are serialized in java and
       deserialized in the execution engine during startup. */

    /** create an expression tree. call this once with the input
        stream positioned at the root expression node */
    static AbstractExpression* buildExpressionTree(json_spirit::Object &obj);

    /** accessors */
    ExpressionType getExpressionType() const {
        return m_type;
    }

    const AbstractExpression *getLeft() const {
        return m_left;
    }

    const AbstractExpression *getRight() const {
        return m_right;
    }

  protected:
    AbstractExpression();
    AbstractExpression(ExpressionType type);
    AbstractExpression(ExpressionType type,
                       AbstractExpression *left,
                       AbstractExpression *right);

  private:
    static AbstractExpression* buildExpressionTree_recurse(json_spirit::Object &obj);
    bool initParamShortCircuits();

  protected:
    AbstractExpression *m_left, *m_right;

    ExpressionType m_type;

    bool m_hasParameter;
};

} // End expression namespace
} // End nstore namespace
