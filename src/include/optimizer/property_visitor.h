//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_visitor.h
//
// Identification: src/include/optimizer/property_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace optimizer {

// Forward Declaration
class PropertyColumns;
class PropertyProjection;
class PropertySort;
class PropertyDistinct;
class PropertyLimit;

//===--------------------------------------------------------------------===//
// Property Visitor
//===--------------------------------------------------------------------===//

// Visit physical properties
class PropertyVisitor {
 public:
  virtual ~PropertyVisitor(){};

  virtual void Visit(const PropertyColumns *) = 0;
  virtual void Visit(const PropertySort *) = 0;
  virtual void Visit(const PropertyDistinct *) = 0;
  virtual void Visit(const PropertyLimit *) = 0;
};

} // namespace optimizer
} // namespace peloton
