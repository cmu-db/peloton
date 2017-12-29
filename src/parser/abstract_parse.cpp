//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_parse.cpp
//
// Identification: src/parser/peloton/abstract_parse.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/abstract_parse.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <util/string_util.h>

#include "common/internal_types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

AbstractParse::AbstractParse() {}

AbstractParse::~AbstractParse() {}

void AbstractParse::AddChild(std::unique_ptr<AbstractParse> &&child) {
  children_.emplace_back(std::move(child));
}

const std::vector<std::unique_ptr<AbstractParse>> &AbstractParse::GetChildren()
    const {
  return children_;
}

const AbstractParse *AbstractParse::GetParent() { return parent_; }

// Get a string representation of this plan
std::ostream &operator<<(std::ostream &os, const AbstractParse &parse) {
  os << ParseNodeTypeToString(parse.GetParseNodeType());
  return os;
}

const std::string AbstractParse::GetInfo() const {
  std::ostringstream os;

  os << GetInfo();

  // Traverse the tree
  for (int ctr = 0, cnt = static_cast<int>(children_.size()); ctr < cnt;
       ctr++) {
    os << peloton::GETINFO_SPACER << children_[ctr].get()->GetParseNodeType() << "\n";
    os << children_[ctr].get()->GetInfo() << "\n";
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

std::string AbstractParse::GetTableName() { return "NoTable"; }

}  // namespace parser
}  // namespace peloton
