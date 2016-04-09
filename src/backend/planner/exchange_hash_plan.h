//
// Created by Rui Wang on 16-4-6.
//

#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/hash_plan.h"

namespace peloton {
namespace planner {

class ExchangeHashPlan : public AbstractPlan {
public:
  ExchangeHashPlan() = delete;
  ExchangeHashPlan(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan &operator=(const ExchangeHashPlan &) = delete;
  ExchangeHashPlan(ExchangeHashPlan &&) = delete;
  ExchangeHashPlan &operator=(ExchangeHashPlan &&) = delete;

  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

  ExchangeHashPlan(std::vector<HashKeyPtrType> &hashkeys)
    : hash_keys_(std::move(hashkeys)) {}

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_HASH; }

  const std::string GetInfo() const { return "ExchangeHash"; }

  inline const std::vector<HashKeyPtrType> &GetHashKeys() const {
    return hash_keys_;
  }

  std::unique_ptr<peloton::planner::AbstractPlan> Copy() const {
    std::vector<HashKeyPtrType> copied_hash_keys;
    for (const auto &key : hash_keys_) {
      copied_hash_keys.push_back(std::unique_ptr<HashKeyType>(key->Copy()));
    }
    return std::unique_ptr<peloton::planner::AbstractPlan>(new ExchangeHashPlan(copied_hash_keys));
  }

private:
  std::vector<HashKeyPtrType> hash_keys_;
};

}  // namespace planner
}  // namespace peloton
