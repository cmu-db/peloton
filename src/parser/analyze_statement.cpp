//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_statement.cpp
//
// Identification: src/parser/analyze_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/analyze_statement.h"

namespace peloton {
namespace parser {

const std::string AnalyzeStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "AnalyzeStatement\n";
  os << analyze_table.get()->GetInfo(num_indent + 1);
  if (analyze_columns.size() > 0) {
    os << StringUtil::Indent(num_indent + 1) << "Columns: \n";
  }
  for (const auto &col : analyze_columns) {
    os << StringUtil::Indent(num_indent + 2) << col << "\n";
  }
  return os.str();
}

const std::string AnalyzeStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[ANALYZE]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
