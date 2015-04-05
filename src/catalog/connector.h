#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Connector
//===--------------------------------------------------------------------===//

class UserRef;
class GroupRef;
class ConnectorTableInfo;

/**
 * Export connector (ELT)
 */
class Connector : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Connector>;

public:
    ~Connector();

    /** GETTER: The class name of the connector */
    const std::string & GetLoaderClass() const;

    /** GETTER: Is the connector enabled */
    bool IsEnabled() const;

    /** GETTER: Users authorized to invoke this procedure */
    const CatalogMap<UserRef> & GetAuthUsers() const;

    /** GETTER: Groups authorized to invoke this procedure */
    const CatalogMap<GroupRef> & GetAuthGroups() const;

    /** GETTER: Per table configuration */
    const CatalogMap<ConnectorTableInfo> & GetTableInfo() const;

protected:
    Connector(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    std::string m_loader_class;

    bool m_enabled;

    CatalogMap<UserRef> m_auth_users;

    CatalogMap<GroupRef> m_auth_groups;

    CatalogMap<ConnectorTableInfo> m_table_info;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
