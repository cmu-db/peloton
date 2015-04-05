#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// PlanFragment
//===--------------------------------------------------------------------===//

/**
 * Instructions to the executor to execute part of an execution plan
 */
class PlanFragment : public CatalogType {
  friend class Catalog;
  friend class CatalogMap<PlanFragment>;

 public:
  ~PlanFragment();

  /** GETTER: Unique Id for this PlanFragment */
  int32_t GetId() const;

  /** GETTER: Dependencies must be received before this plan fragment can execute */
  bool HasDependencies() const;

  /** GETTER: Should this plan fragment be sent to all partitions */
  bool IsMultiPartition() const;

  /** GETTER: Whether this PlanFragment is read only */
  bool IsReadOnly() const;

  /** GETTER: A serialized representation of the plan-graph/plan-pipeline */
  const std::string & GetPlanNodeTree() const;

  /** GETTER: True if this fragment doesn't read from or write to any persistent tables */
  bool IsNonTransactional() const;

  /** GETTER: Whether this PlanFragment is an aggregate that can be executed in Java */
  bool FastAggregate() const;

  /** GETTER: Whether this PlanFragment just combines its input tables and therefore can be executed in Java */
  bool FastCombine() const;

 protected:
  PlanFragment(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  int32_t m_id;

  bool m_has_dependencies;

  bool m_multi_partition;

  bool m_read_only;

  std::string m_plan_node_tree;

  bool m_non_transactional;

  bool m_fast_aggregate;

  bool m_fast_combine;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);


};

} // End catalog namespace
} // End nstore namespace
