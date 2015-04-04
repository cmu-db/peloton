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

protected:
    PlanFragment(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    bool m_hasdependencies;
    bool m_multipartition;
    bool m_readonly;
    std::string m_plannodetree;
    bool m_nontransactional;
    bool m_fastaggregate;
    bool m_fastcombine;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~PlanFragment();

    /** GETTER: Unique Id for this PlanFragment */
    int32_t id() const;
    /** GETTER: Dependencies must be received before this plan fragment can execute */
    bool hasdependencies() const;
    /** GETTER: Should this plan fragment be sent to all partitions */
    bool multipartition() const;
    /** GETTER: Whether this PlanFragment is read only */
    bool readonly() const;
    /** GETTER: A serialized representation of the plan-graph/plan-pipeline */
    const std::string & plannodetree() const;
    /** GETTER: True if this fragment doesn't read from or write to any persistent tables */
    bool nontransactional() const;
    /** GETTER: Whether this PlanFragment is an aggregate that can be executed in Java */
    bool fastaggregate() const;
    /** GETTER: Whether this PlanFragment just combines its input tables and therefore can be executed in Java */
    bool fastcombine() const;
};

} // End catalog namespace
} // End nstore namespace
