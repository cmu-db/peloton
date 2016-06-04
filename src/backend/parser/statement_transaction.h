#pragma once

#include "backend/parser/sql_statement.h"

namespace nstore {
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
} // End nstore namespace
