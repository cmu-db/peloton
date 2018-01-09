//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_statement.cpp
//
// Identification: src/parser/transaction_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/transaction_statement.h"
#include "util/string_util.h"
#include <sstream>

namespace peloton {
namespace parser {

const std::string TransactionStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "TransactionStatement\n";
  os << StringUtil::Indent(num_indent + 1) << "Type: ";

  switch (type) {
    case kBegin:
      os << "Begin";
      break;
    case kCommit:
      os << "Commit";
      break;
    case kRollback:
      os << "Rollback";
      break;
  }
  os << std::endl;

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
