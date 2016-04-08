//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.h
//
// Identification: src/backend/planner/abstract_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <memory>
#include <cstdint>
#include <vector>
#include <map>
#include <vector>

#include "backend/common/assert.h"
#include "backend/common/printable.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"

#include "nodes/nodes.h"

namespace peloton {

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan
//===--------------------------------------------------------------------===//

class AbstractPlan : public Printable {
 public:
  AbstractPlan(const AbstractPlan &) = delete;
  AbstractPlan &operator=(const AbstractPlan &) = delete;
  AbstractPlan(AbstractPlan &&) = delete;
  AbstractPlan &operator=(AbstractPlan &&) = delete;

  AbstractPlan();

  virtual ~AbstractPlan();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(const AbstractPlan *child);

  const std::vector<const AbstractPlan *> &GetChildren() const;

  const AbstractPlan *GetParent() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

  virtual std::unique_ptr<AbstractPlan> Copy() const = 0;

  // A plan will be sent to anther node
  // So serialization is should be implemented by the derived classes

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement these functions
  // After the implementation for each sub-class, we should set these to pure virtual
  //===--------------------------------------------------------------------===//
  virtual bool SerializeTo(SerializeOutput &output) const {
      ASSERT(&output != nullptr);
      return false;
  }
  virtual bool DeserializeFrom(SerializeInputBE &input) {
      ASSERT(&input != nullptr);
      return false;
  }
  virtual int SerializeSize() {
      return 0;
  }

 protected:
  // only used by its derived classes (when deserialization)
  AbstractPlan *Parent() {return parent_;}

 private:
  // A plan node can have multiple children
  std::vector<const AbstractPlan *> children_;

  AbstractPlan *parent_ = nullptr;
};

}  // namespace planner
}  // namespace peloton
