//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_statement.cpp
//
// Identification: src/parser/drop_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/drop_statement.h"
#include <iostream>
#include <sstream>
#include "util/string_util.h"

namespace peloton {
namespace parser {

const std::string DropStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "DropStatement\n";
  os << StringUtil::Indent(num_indent + 1);
  switch (type_) {
    case kDatabase: {
      os << "DropType: Database\n";
      os << StringUtil::Indent(num_indent + 1)
         << "Database name: " << GetDatabaseName();
      break;
    }
    case kTable: {
      os << "DropType: Table\n";
      os << StringUtil::Indent(num_indent + 1)
         << "Table name: " << GetTableName();
      break;
    }
    case kSchema: {
      os << "DropType: Schema\n";
      os << StringUtil::Indent(num_indent + 1)
         << "Schema name: " << GetSchemaName();
      break;
    }
    case kIndex: {
      os << "DropType: Index\n";
      os << StringUtil::Indent(num_indent + 1) << "Index name: " << index_name_;
      break;
    }
    case kView: {
      os << "DropType: View\n";
      os << StringUtil::Indent(num_indent + 1)
         << "Drop view is not implemented yet...";
      break;
    }
    case kPreparedStatement: {
      os << "DropType: PreparedStatement\n";
      os << StringUtil::Indent(num_indent + 1)
         << "Prepared statement: " << prep_stmt_;
      break;
    }
    case kTrigger: {
      os << "DropType: Trigger\n";
      os << StringUtil::Indent(num_indent + 1)
         << "Trigger table name: " << GetTableName() << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << "Trigger name: " << trigger_name_;
      break;
    }
  }
  os << std::endl;
  os << StringUtil::Indent(num_indent + 1)
     << "IF EXISTS: " << ((missing_) ? "True" : "False") << std::endl;
  os << StringUtil::Indent(num_indent + 1)
     << "CASCADE: " << ((cascade_) ? "True" : "False") << std::endl;
  os << StringUtil::Indent(num_indent + 1)
     << "RESTRICT: " << ((cascade_) ? "False" : "True") << std::endl;
  return os.str();
}

const std::string DropStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[DROP]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
