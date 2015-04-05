#include "plan_fragment.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

PlanFragment::PlanFragment(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_id(0),
  m_has_dependencies(false),
  m_multi_partition(false),
  m_read_only(false),
  m_non_transactional(false),
  m_fast_aggregate(false),
  m_fast_combine(false) {
  CatalogValue value;
  m_fields["id"] = value;
  m_fields["hasdependencies"] = value;
  m_fields["multipartition"] = value;
  m_fields["readonly"] = value;
  m_fields["plannodetree"] = value;
  m_fields["nontransactional"] = value;
  m_fields["fastaggregate"] = value;
  m_fields["fastcombine"] = value;
}

PlanFragment::~PlanFragment() {
}

void PlanFragment::Update() {
  m_id = m_fields["id"].intValue;
  m_has_dependencies = m_fields["hasdependencies"].intValue;
  m_multi_partition = m_fields["multipartition"].intValue;
  m_read_only = m_fields["readonly"].intValue;
  m_plan_node_tree = m_fields["plannodetree"].strValue.c_str();
  m_non_transactional = m_fields["nontransactional"].intValue;
  m_fast_aggregate = m_fields["fastaggregate"].intValue;
  m_fast_combine = m_fields["fastcombine"].intValue;
}

CatalogType * PlanFragment::AddChild(__attribute__((unused)) const std::string &collection_name,
                                     __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * PlanFragment::GetChild(__attribute__((unused)) const std::string &collection_name,
                                     __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool PlanFragment::RemoveChild(const std::string &collection_name,
                               __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

int32_t PlanFragment::GetId() const {
  return m_id;
}

bool PlanFragment::HasDependencies() const {
  return m_has_dependencies;
}

bool PlanFragment::IsMultiPartition() const {
  return m_multi_partition;
}

bool PlanFragment::IsReadOnly() const {
  return m_read_only;
}

const std::string & PlanFragment::GetPlanNodeTree() const {
  return m_plan_node_tree;
}

bool PlanFragment::IsNonTransactional() const {
  return m_non_transactional;
}

bool PlanFragment::FastAggregate() const {
  return m_fast_aggregate;
}

bool PlanFragment::FastCombine() const {
  return m_fast_combine;
}

} // End catalog namespace
} // End nstore namespace
