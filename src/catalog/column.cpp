/*-------------------------------------------------------------------------
 *
 * column.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/column.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/column.h"

namespace nstore {
namespace catalog {

bool Column::AddConstraint(Constraint* constraint) {
  if(std::find(constraints.begin(), constraints.end(), constraint) != constraints.end())
    return false;
  constraints.push_back(constraint);
  return true;
}

Constraint* Column::GetConstraint(const std::string &constraint_name) const {
  for(auto constraint : constraints)
    if(constraint->GetName() == constraint_name)
      return constraint;
  return nullptr;
}

bool Column::RemoveConstraint(const std::string &constraint_name) {
  for(auto itr = constraints.begin(); itr != constraints.end() ; itr++)
    if((*itr)->GetName() == constraint_name) {
      constraints.erase(itr);
      return true;
    }
  return false;
}

} // End catalog namespace
} // End nstore namespace


