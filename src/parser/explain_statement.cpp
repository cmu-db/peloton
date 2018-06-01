//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_statement.cpp
//
// Identification: src/parser/explain_statement.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/explain_statement.h"
#include "parser/select_statement.h"

namespace peloton {
namespace parser {

const std::string ExplainStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "ExplainStatement:\n";
  os << real_sql_stmt->GetInfo(num_indent + 1);
  return os.str();
}

const std::string ExplainStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[EXPLAIN]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
