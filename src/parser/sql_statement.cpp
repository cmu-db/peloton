//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_statement.cpp
//
// Identification: src/parser/sql_statement.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*-------------------------------------------------------------------------
 *
 * sql_statement.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/parser/sql_statement.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "parser/sql_statement.h"

#include <sstream>
#include <map>

#include "parser/parser_utils.h"

namespace peloton {

namespace parser {

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

const std::string SQLStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[" << StatementTypeToString(stmt_type) << "]\n";

  int indent = 1;
  switch (stmt_type) {
    case StatementType::SELECT:
      os << ParserUtils::GetSelectStatementInfo((SelectStatement*)this, indent);
      break;
    case StatementType::INSERT:
      os << ParserUtils::GetInsertStatementInfo((InsertStatement*)this, indent);
      break;
    case StatementType::CREATE:
      os << ParserUtils::GetCreateStatementInfo((CreateStatement*)this, indent);
      break;
    case StatementType::DELETE:
      os << ParserUtils::GetDeleteStatementInfo((DeleteStatement*)this, indent);
      break;
    case StatementType::COPY:
      os << ParserUtils::GetCopyStatementInfo((CopyStatement*)this, indent);
      break;
    case StatementType::UPDATE:
      os << ParserUtils::GetUpdateStatementInfo((UpdateStatement*)this, indent);
      break;
    default:
      break;
  }
  return os.str();
}

const std::string SQLStatementList::GetInfo() const {
  std::ostringstream os;

  if (is_valid) {
    for (auto stmt : statements) {
      os << stmt->GetInfo();
    }
  } else {
    os << "Invalid statement list \n";
  }

  return os.str();
}

QueryType StatementTypeToQueryType(StatementType stmt_type, std::shared_ptr<parser::SQLStatement> sql_stmt) {
  LOG_INFO("%s", StatementTypeToString(stmt_type).c_str());
  QueryType query_type = QueryType::QUERY_OTHER;
  std::map<StatementType, QueryType> type_map {
      {StatementType::EXECUTE, QueryType::QUERY_EXECUTE},
      {StatementType::PREPARE, QueryType::QUERY_PREPARE},
      {StatementType::INSERT, QueryType::QUERY_INSERT},
      {StatementType::UPDATE, QueryType::QUERY_UPDATE},
      {StatementType::DELETE, QueryType::QUERY_DELETE},
      {StatementType::COPY, QueryType::QUERY_COPY},
      {StatementType::ANALYZE, QueryType::QUERY_ANALYZE},
      {StatementType::ALTER, QueryType::QUERY_ALTER},
      {StatementType::DROP, QueryType::QUERY_DROP},
      {StatementType::SELECT, QueryType::QUERY_SELECT},
      {StatementType::VARIABLE_SET, QueryType::QUERY_SET},
  };
  std::map<StatementType, QueryType>::iterator it  = type_map.find(stmt_type);
  if (it != type_map.end()) {
    query_type = it -> second;
  } else {
    switch(stmt_type) {
      case StatementType::TRANSACTION:
        switch(std::dynamic_pointer_cast<parser::TransactionStatement>(sql_stmt)->type) {
          case parser::TransactionStatement::CommandType::kBegin:
            query_type = QueryType::QUERY_BEGIN;
            break;
          case parser::TransactionStatement::CommandType::kCommit:
            query_type = QueryType::QUERY_COMMIT;
            break;
          case parser::TransactionStatement::CommandType::kRollback:
            query_type = QueryType::QUERY_ROLLBACK;
            break;
        }
        break;
      case StatementType::CREATE:
        switch (std::dynamic_pointer_cast<parser::CreateStatement>(sql_stmt) -> type) {
          case parser::CreateStatement::CreateType::kDatabase:
            query_type = QueryType::QUERY_CREATE_DB;
            break;
          case parser::CreateStatement::CreateType::kIndex:
            query_type = QueryType::QUERY_CREATE_INDEX;
            break;
          case parser::CreateStatement::CreateType::kTable:
            query_type = QueryType::QUERY_CREATE_TABLE;
            break;
          case parser::CreateStatement::CreateType::kTrigger:
            query_type = QueryType::QUERY_CREATE_TRIGGER;
            break;
        }
        break;
      default:
        query_type = QueryType::QUERY_OTHER;
    }
  }
  return query_type;
}
}  // namespace parser
}  // namespace peloton
