//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_parse.h
//
// Identification: src/include/parser/peloton/abstract_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <cstdint>
#include <vector>
#include <vector>

#include "common/printable.h"
#include "common/types.h"
#include "type/serializer.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Abstract Parse
//===--------------------------------------------------------------------===//

class AbstractParse : public Printable {
 public:
  AbstractParse(const AbstractParse &) = delete;
  AbstractParse &operator=(const AbstractParse &) = delete;
  AbstractParse(AbstractParse &&) = delete;
  AbstractParse &operator=(AbstractParse &&) = delete;

  AbstractParse();

  virtual ~AbstractParse();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(std::unique_ptr<AbstractParse> &&child);

  const std::vector<std::unique_ptr<AbstractParse>> &GetChildren() const;

  const AbstractParse *GetParent();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual ParseNodeType GetParseNodeType() const = 0;

  virtual std::string GetTableName();
  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

 protected:
  // only used by its derived classes (when deserialization)
  AbstractParse *Parent() { return parent_; }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractParse>> children_;

  AbstractParse *parent_ = nullptr;
};

}  // namespace parser
}  // namespace peloton
