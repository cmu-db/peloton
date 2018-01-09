//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_statement.cpp
//
// Identification: src/parser/create_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/create_statement.h"
#include <iostream>

namespace peloton {
namespace parser {

const std::string CreateStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "CreateStatement\n";
  os << StringUtil::Indent(num_indent + 1);

  switch (type) {
    case CreateStatement::CreateType::kTable: {
      os << "Create type: Table" << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("IF NOT EXISTS: %s",
                               (if_not_exists) ? "True" : "False") << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("Table name: %s", GetTableName().c_str());;
      break;
    }
    case CreateStatement::CreateType::kDatabase: {
      os << "Create type: Database" << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("Database name: %s", GetDatabaseName().c_str());
      break;
    }
    case CreateStatement::CreateType::kIndex: {
      os << "Create type: Index" << std::endl;
      os << StringUtil::Indent(num_indent + 1) << index_name << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << "INDEX : table : " << GetTableName() << " unique : " << unique
         << " attrs : ";
      for (auto &key : index_attrs) os << key << " ";
      os << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << "Type : " << IndexTypeToString(index_type);
      break;
    }
    case CreateStatement::CreateType::kTrigger: {
      os << "Create type: Trigger" << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("Trigger table name: %s", trigger_name.c_str())
         << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("Trigger name: %s", GetTableName().c_str());
      break;
    }
    case CreateStatement::CreateType::kSchema: {
      os << "Create type: Schema" << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("Schema name: %s", schema_name.c_str());
      break;
    }
    case CreateStatement::CreateType::kView: {
      os << "Create type: View" << std::endl;
      os << StringUtil::Indent(num_indent + 1)
         << StringUtil::Format("View name: %s", view_name.c_str());
      break;
    }
  }
  os << std::endl;

  if (!columns.empty()) {
    for (auto &col : columns) {
      if (col->name.empty()) {
        continue;
      }
      if (col->type == ColumnDefinition::DataType::PRIMARY) {
        os << StringUtil::Indent(num_indent + 1) << "-> PRIMARY KEY : ";
        for (auto &key : col->primary_key) os << key << " ";
      } else if (col->type == ColumnDefinition::DataType::FOREIGN) {
        os << StringUtil::Indent(num_indent + 1)
           << "-> FOREIGN KEY : References " << col->name << " Source : ";
        for (auto &key : col->foreign_key_source) {
          os << key << " ";
        }
        os << "Sink : ";
        for (auto &key : col->foreign_key_sink) {
          os << key << " ";
        }
      } else {
        os << StringUtil::Indent(num_indent + 1)
           << "-> COLUMN REF : " << col->name << " "
           // << col->type << " not null : "
           << col->not_null << " primary : " << col->primary << " unique "
           << col->unique << " varlen " << col->varlen;
      }
      os << std::endl;  
    }
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

const std::string CreateStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[CREATE]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
