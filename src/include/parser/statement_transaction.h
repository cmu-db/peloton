#pragma once

#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

/**
 * @struct TransactionStatement
 * @brief Represents "BEGIN or COMMIT or ROLLBACK [TRANSACTION]"
 */
struct TransactionStatement : SQLStatement {
  enum CommandType {
    kBegin,
    kCommit,
    kRollback,
  };

  TransactionStatement(CommandType type) :
    SQLStatement(STATEMENT_TYPE_TRANSACTION),
    type(type){
  }

  CommandType type;
};

} // End parser namespace
} // End peloton namespace
