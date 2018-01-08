//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.cpp
//
// Identification: src/parser/transaction_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include "parser/transaction_statement.h"
#include "util/string_util.h"

namespace peloton {
namespace parser {

const std::string TransactionStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "TransactionStatement\n";
  os << StringUtil::Indent(num_indent + 1) << "Type: ";

  switch (type) {
    case kBegin:
      os << "Begin\n";
      break;
    case kCommit:
      os << "Commit\n";
      break;
    case kRollback:
      os << "Rollback\n";
      break;
  }

  return os.str();
}

const std::string TransactionStatement::GetInfo() const {
  std::ostringstream os;
  os << "SQLStatement[TRANSACTION]\n";
  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
