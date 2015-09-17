//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// function_expression.cpp
//
// Identification: src/backend/expression/function_expression.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/function_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {

/** implement a forced SQL ERROR function (for test and example purposes) for either integer or string types **/
template<> inline Value Value::CallUnary<FUNC_VOLT_SQL_ERROR>() const {
    //const char* sqlstatecode;
    const char* msgtext;
    const ValueType type = GetValueType();
    char msg_format_buffer[1024];
    char state_format_buffer[6];
    if (type == VALUE_TYPE_VARCHAR) {
        if (IsNull()) {
             throw Exception("Must not ask  for object length on sql null object.");
        }
        const int32_t valueLength = GetObjectLengthWithoutNull();
        const char *valueChars = reinterpret_cast<char*>(GetObjectValueWithoutNull());
        std::string valueStr(valueChars, valueLength);
        snprintf(msg_format_buffer, sizeof(msg_format_buffer), "%s", valueStr.c_str());
        //sqlstatecode = SQLException::nonspecific_error_code_for_error_forced_by_user;
        msgtext = msg_format_buffer;
    } else {
        int64_t intValue = CastAsBigIntAndGetValue(); // let cast throw if invalid
        if (intValue == 0) {
            return *this;
        }
        snprintf(state_format_buffer, sizeof(state_format_buffer), "%05ld", (long) intValue);
        //sqlstatecode = state_format_buffer;
        msgtext = state_format_buffer;
    }
    throw Exception(msgtext);
}

/** implement the 2-argument forced SQL ERROR function (for test and example purposes) */
template<> inline Value Value::Call<FUNC_VOLT_SQL_ERROR>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const char* sqlstatecode;
    char msg_format_buffer[1024];
    char state_format_buffer[6];

    const Value& codeArg = arguments[0];
    if (codeArg.IsNull()) {
        //sqlstatecode = SQLException::nonspecific_error_code_for_error_forced_by_user;
    } else {
        int64_t intValue = codeArg.CastAsBigIntAndGetValue(); // let cast throw if invalid
        if (intValue == 0) {
            return codeArg;
        }
        snprintf(state_format_buffer, sizeof(state_format_buffer), "%05ld", (long) intValue);
        sqlstatecode = state_format_buffer;
    }

    const Value& strValue = arguments[1];
    if (strValue.IsNull()) {
        msg_format_buffer[0] = '\0';
    } else {
        if (strValue.GetValueType() != VALUE_TYPE_VARCHAR) {
            ThrowCastSQLException (strValue.GetValueType(), VALUE_TYPE_VARCHAR);
        }
        const int32_t valueLength = strValue.GetObjectLengthWithoutNull();
        char *valueChars = reinterpret_cast<char*>(strValue.GetObjectValueWithoutNull());
        std::string valueStr(valueChars, valueLength);
        snprintf(msg_format_buffer, sizeof(msg_format_buffer), "%s", valueStr.c_str());
    }
    throw Exception(std::string(msg_format_buffer) + std::string(sqlstatecode));
}

/*
 * Constant (no parameter) function. (now, random)
 */
template <int F>
class ConstantFunctionExpression : public expression::AbstractExpression {
public:
    ConstantFunctionExpression()
        : expression::AbstractExpression(EXPRESSION_TYPE_FUNCTION) {
    };

    Value Evaluate(const AbstractTuple *, const AbstractTuple *,
                   executor::ExecutorContext *) const {
        return Value::CallConstant<F>();
    }

    std::string DebugInfo(const std::string &spacer) const {
        std::stringstream buffer;
        buffer << spacer << "ConstantFunctionExpression " << F << std::endl;
        return (buffer.str());
    }
};

/*
 * Unary functions. (abs, upper, lower)
 */

template <int F>
class UnaryFunctionExpression : public expression::AbstractExpression {
  expression::AbstractExpression * const m_child;
public:
    UnaryFunctionExpression(AbstractExpression *child)
        : AbstractExpression(EXPRESSION_TYPE_FUNCTION)
        , m_child(child) {
    }

    virtual ~UnaryFunctionExpression() {
        delete m_child;
    }

    virtual bool hasParameter() const {
        return m_child->HasParameter();
    }

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const {
        assert (m_child);
        return (m_child->Evaluate(tuple1, tuple2, context)).CallUnary<F>();
    }

    std::string DebugInfo(const std::string &spacer) const {
        std::stringstream buffer;
        buffer << spacer << "UnaryFunctionExpression " << F << std::endl;
        return (buffer.str());
    }
};

/*
 * N-ary functions.
 */
template <int F>
class GeneralFunctionExpression : public expression::AbstractExpression {
public:
    GeneralFunctionExpression(const std::vector<expression::AbstractExpression *>& args)
        : AbstractExpression(EXPRESSION_TYPE_FUNCTION), m_args(args) {}

    virtual ~GeneralFunctionExpression() {
        size_t i = m_args.size();
        while (i--) {
            delete m_args[i];
        }
        delete &m_args;
    }

    virtual bool hasParameter() const {
        for (size_t i = 0; i < m_args.size(); i++) {
            assert(m_args[i]);
            if (m_args[i]->HasParameter()) {
                return true;
            }
        }
        return false;
    }

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const {
        //TODO: Could make this vector a member, if the memory management implications
        // (of the Value internal state) were clear --.Is there a penalty for longer-lived
        // Values that outweighs the current per-Evaluate allocation penalty?
        std::vector<Value> nValue(m_args.size());
        for (size_t i = 0; i < m_args.size(); ++i) {
            nValue[i] = m_args[i]->Evaluate(tuple1, tuple2, context);
        }
        return Value::Call<F>(nValue);
    }

    std::string DebugInfo(const std::string &spacer) const {
        std::stringstream buffer;
        buffer << spacer << "GeneralFunctionExpression " << F << std::endl;
        return (buffer.str());
    }

private:
    const std::vector<AbstractExpression *>& m_args;
};

expression::AbstractExpression*
expression::FunctionFactory(int functionId, const std::vector<AbstractExpression*>* arguments) {
    AbstractExpression* ret = 0;
    assert(arguments);
    size_t nArgs = arguments->size();
    if (nArgs == 0) {
        switch(functionId) {
        case FUNC_CURRENT_TIMESTAMP:
            ret = new ConstantFunctionExpression<FUNC_CURRENT_TIMESTAMP>();
            break;
        default:
            return NULL;
        }
        delete arguments;
    }
    else if (nArgs == 1) {
        switch(functionId) {
        case FUNC_ABS:
            ret = new UnaryFunctionExpression<FUNC_ABS>((*arguments)[0]);
            break;
        case FUNC_CEILING:
            ret = new UnaryFunctionExpression<FUNC_CEILING>((*arguments)[0]);
            break;
        case FUNC_CHAR:
            ret = new UnaryFunctionExpression<FUNC_CHAR>((*arguments)[0]);
            break;
        case FUNC_CHAR_LENGTH:
            ret = new UnaryFunctionExpression<FUNC_CHAR_LENGTH>((*arguments)[0]);
            break;
        case FUNC_EXP:
            ret = new UnaryFunctionExpression<FUNC_EXP>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_DAY:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_DAY_OF_WEEK:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_WEEK>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_WEEKDAY:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_WEEKDAY>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_DAY_OF_YEAR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_YEAR>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_HOUR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_HOUR>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_MINUTE:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MINUTE>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_MONTH:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MONTH>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_QUARTER:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_QUARTER>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_SECOND:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_SECOND>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_WEEK_OF_YEAR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_WEEK_OF_YEAR>((*arguments)[0]);
            break;
        case FUNC_EXTRACT_YEAR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_YEAR>((*arguments)[0]);
            break;
        case FUNC_SINCE_EPOCH_SECOND:
            ret = new UnaryFunctionExpression<FUNC_SINCE_EPOCH_SECOND>((*arguments)[0]);
            break;
        case FUNC_SINCE_EPOCH_MILLISECOND:
            ret = new UnaryFunctionExpression<FUNC_SINCE_EPOCH_MILLISECOND>((*arguments)[0]);
            break;
        case FUNC_SINCE_EPOCH_MICROSECOND:
            ret = new UnaryFunctionExpression<FUNC_SINCE_EPOCH_MICROSECOND>((*arguments)[0]);
            break;
        case FUNC_TO_TIMESTAMP_SECOND:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_SECOND>((*arguments)[0]);
            break;
        case FUNC_TO_TIMESTAMP_MILLISECOND:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_MILLISECOND>((*arguments)[0]);
            break;
        case FUNC_TO_TIMESTAMP_MICROSECOND:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_MICROSECOND>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_YEAR:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_YEAR>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_QUARTER:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_QUARTER>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_MONTH:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MONTH>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_DAY:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_DAY>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_HOUR:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_HOUR>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_MINUTE:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MINUTE>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_SECOND:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_SECOND>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_MILLISECOND:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MILLISECOND>((*arguments)[0]);
            break;
        case FUNC_TRUNCATE_MICROSECOND:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MICROSECOND>((*arguments)[0]);
            break;
        // Alias for function FUNC_TO_TIMESTAMP_SECOND
        case FUNC_VOLT_FROM_UNIXTIME:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_SECOND>((*arguments)[0]);
            break;
        case FUNC_FLOOR:
            ret = new UnaryFunctionExpression<FUNC_FLOOR>((*arguments)[0]);
            break;
        case FUNC_OCTET_LENGTH:
            ret = new UnaryFunctionExpression<FUNC_OCTET_LENGTH>((*arguments)[0]);
            break;
        case FUNC_SPACE:
            ret = new UnaryFunctionExpression<FUNC_SPACE>((*arguments)[0]);
            break;
        case FUNC_FOLD_LOWER:
            ret = new UnaryFunctionExpression<FUNC_FOLD_LOWER>((*arguments)[0]);
            break;
        case FUNC_FOLD_UPPER:
            ret = new UnaryFunctionExpression<FUNC_FOLD_UPPER>((*arguments)[0]);
            break;
        case FUNC_SQRT:
            ret = new UnaryFunctionExpression<FUNC_SQRT>((*arguments)[0]);
            break;
        case FUNC_VOLT_ARRAY_LENGTH:
            ret = new UnaryFunctionExpression<FUNC_VOLT_ARRAY_LENGTH>((*arguments)[0]);
            break;
        case FUNC_VOLT_BITNOT:
            ret = new UnaryFunctionExpression<FUNC_VOLT_BITNOT>((*arguments)[0]);
            break;
        case FUNC_VOLT_HEX:
            ret = new UnaryFunctionExpression<FUNC_VOLT_HEX>((*arguments)[0]);
            break;
        case FUNC_VOLT_BIN:
            ret = new UnaryFunctionExpression<FUNC_VOLT_BIN>((*arguments)[0]);
            break;
        case FUNC_VOLT_SQL_ERROR:
            ret = new UnaryFunctionExpression<FUNC_VOLT_SQL_ERROR>((*arguments)[0]);
            break;
        case FUNC_LN:
            ret = new UnaryFunctionExpression<FUNC_LN>((*arguments)[0]);
            break;
        default:
            return NULL;
        }
        delete arguments;
    } else {
        // GeneralFunctions defer deleting the arguments container until through with it.
        switch(functionId) {
        case FUNC_BITAND:
            ret = new GeneralFunctionExpression<FUNC_BITAND>(*arguments);
            break;
        case FUNC_BITOR:
            ret = new GeneralFunctionExpression<FUNC_BITOR>(*arguments);
            break;
        case FUNC_BITXOR:
            ret = new GeneralFunctionExpression<FUNC_BITXOR>(*arguments);
            break;
        case FUNC_CONCAT:
            ret = new GeneralFunctionExpression<FUNC_CONCAT>(*arguments);
            break;
        case FUNC_DECODE:
            ret = new GeneralFunctionExpression<FUNC_DECODE>(*arguments);
            break;
        case FUNC_LEFT:
            ret = new GeneralFunctionExpression<FUNC_LEFT>(*arguments);
            break;
        case FUNC_MOD:
            ret = new GeneralFunctionExpression<FUNC_MOD>(*arguments);
            break;
        case FUNC_OVERLAY_CHAR:
            ret = new GeneralFunctionExpression<FUNC_OVERLAY_CHAR>(*arguments);
            break;
        case FUNC_POSITION_CHAR:
            ret = new GeneralFunctionExpression<FUNC_POSITION_CHAR>(*arguments);
            break;
        case FUNC_POWER:
            ret = new GeneralFunctionExpression<FUNC_POWER>(*arguments);
            break;
        case FUNC_REPEAT:
            ret = new GeneralFunctionExpression<FUNC_REPEAT>(*arguments);
            break;
        case FUNC_REPLACE:
            ret = new GeneralFunctionExpression<FUNC_REPLACE>(*arguments);
            break;
        case FUNC_RIGHT:
            ret = new GeneralFunctionExpression<FUNC_RIGHT>(*arguments);
            break;
        case FUNC_SUBSTRING_CHAR:
            ret = new GeneralFunctionExpression<FUNC_SUBSTRING_CHAR>(*arguments);
            break;
        case FUNC_TRIM_BOTH_CHAR:
            ret = new GeneralFunctionExpression<FUNC_TRIM_BOTH_CHAR>(*arguments);
            break;
        case FUNC_TRIM_LEADING_CHAR:
            ret = new GeneralFunctionExpression<FUNC_TRIM_LEADING_CHAR>(*arguments);
            break;
        case FUNC_TRIM_TRAILING_CHAR:
            ret = new GeneralFunctionExpression<FUNC_TRIM_TRAILING_CHAR>(*arguments);
            break;
        case FUNC_VOLT_ARRAY_ELEMENT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_ARRAY_ELEMENT>(*arguments);
            break;
        case FUNC_VOLT_BIT_SHIFT_LEFT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_BIT_SHIFT_LEFT>(*arguments);
            break;
        case FUNC_VOLT_BIT_SHIFT_RIGHT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_BIT_SHIFT_RIGHT>(*arguments);
            break;
        case FUNC_VOLT_FIELD:
            ret = new GeneralFunctionExpression<FUNC_VOLT_FIELD>(*arguments);
            break;
        case FUNC_VOLT_FORMAT_CURRENCY:
            ret = new GeneralFunctionExpression<FUNC_VOLT_FORMAT_CURRENCY>(*arguments);
            break;
        case FUNC_VOLT_SET_FIELD:
            ret = new GeneralFunctionExpression<FUNC_VOLT_SET_FIELD>(*arguments);
            break;
        case FUNC_VOLT_SQL_ERROR:
            ret = new GeneralFunctionExpression<FUNC_VOLT_SQL_ERROR>(*arguments);
            break;
        case FUNC_VOLT_SUBSTRING_CHAR_FROM:
            ret = new GeneralFunctionExpression<FUNC_VOLT_SUBSTRING_CHAR_FROM>(*arguments);
            break;
        default:
            return NULL;
        }
    }
    // T.Is function may have explicitly returned null, earlier, leaving it to the Caller
    // (with more context?) to generate an exception.
    // But having fallen through to this point indicates that
    // a FunctionExpression was constructed.
    assert(ret);
    return ret;
}

}  // End peloton namespace

