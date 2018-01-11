//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.h
//
// Identification: src/include/planner/abstract_plan.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "codegen/query_parameters_map.h"
#include "common/printable.h"
#include "planner/binding_context.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "util/hash_util.h"

namespace peloton {

namespace catalog {
class Schema;
}  // namespace catalog

namespace executor {
class AbstractExecutor;
class LogicalTile;
}  // namespace executor

namespace expression {
class AbstractExpression;
class Parameter;
}  // namespace expression

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan
//===--------------------------------------------------------------------===//

class AbstractPlan : public Printable {
 public:
  AbstractPlan();

  virtual ~AbstractPlan();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(std::unique_ptr<AbstractPlan> &&child);

  const std::vector<std::unique_ptr<AbstractPlan>> &GetChildren() const;

  size_t GetChildrenSize() const { return children_.size(); }

  const AbstractPlan *GetChild(uint32_t child_index) const;

  const AbstractPlan *GetParent() const;
  
  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  // Setting values of the parameters in the prepare statement
  virtual void SetParameterValues(std::vector<type::Value> *values);
  
  // Get the estimated cardinality of this plan
  int GetCardinality() const { return estimated_cardinality_; }
  
  // TODO: This is only for testing now. When the optimizer is ready, we should
  // delete this function and pass this information to constructor
  void SetCardinality(int cardinality) { estimated_cardinality_ = cardinality; }

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Binding allows a plan to track the source of an attribute/column regardless
  // of its position in a tuple.  This binding allows a plan to know the types
  // of all the attributes it uses *before* execution. This is primarily used
  // by the codegen component since attributes are not positional.
  virtual void PerformBinding(BindingContext &binding_context) {
    for (auto &child : GetChildren()) {
      child->PerformBinding(binding_context);
    }
  }

  virtual void GetOutputColumns(std::vector<oid_t> &columns UNUSED_ATTRIBUTE)
      const { }

  // Get a string representation for debugging
  const std::string GetInfo() const override;

  virtual std::unique_ptr<AbstractPlan> Copy() const = 0;

  // A plan will be sent to anther node via serialization
  // So serialization should be implemented by the derived classes

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement these functions
  // After the implementation for each sub-class, we should set these to pure
  // virtual
  //===--------------------------------------------------------------------===//
  virtual bool SerializeTo(SerializeOutput &output UNUSED_ATTRIBUTE) const {
    return false;
  }
  virtual bool DeserializeFrom(SerializeInput &input UNUSED_ATTRIBUTE) {
    return false;
  }
  virtual int SerializeSize() const { return 0; }

  virtual hash_t Hash() const;

  virtual bool operator==(const AbstractPlan &rhs) const;
  virtual bool operator!=(const AbstractPlan &rhs) const {
    return !(*this == rhs);
  }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) {
    for (auto &child : GetChildren()) {
      child->VisitParameters(map, values, values_from_user);
    }
  }

 protected:
  // only used by its derived classes (when deserialization)
  AbstractPlan *Parent() const { return parent_; }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractPlan>> children_;

  AbstractPlan *parent_ = nullptr;
  
  // TODO: This field is harded coded now. This needs to be changed when
  // optimizer has the cost model and cardinality estimation
  int estimated_cardinality_ = 500000;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractPlan);
};

class Equal {
 public:
  bool operator()(const std::shared_ptr<planner::AbstractPlan> &a,
                  const std::shared_ptr<planner::AbstractPlan> &b) const {
    return *a.get() == *b.get();
  }
};

class Hash {
 public:
  size_t operator()(const std::shared_ptr<planner::AbstractPlan> &plan) const {
    return static_cast<size_t>(plan->Hash());
  }
};

}  // namespace planner
}  // namespace peloton
