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

protected:
    Connector(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    std::string m_loaderclass;
    bool m_enabled;
    CatalogMap<UserRef> m_authUsers;
    CatalogMap<GroupRef> m_authGroups;
    CatalogMap<ConnectorTableInfo> m_tableInfo;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Connector();

    /** GETTER: The class name of the connector */
    const std::string & loaderclass() const;
    /** GETTER: Is the connector enabled */
    bool enabled() const;
    /** GETTER: Users authorized to invoke this procedure */
    const CatalogMap<UserRef> & authUsers() const;
    /** GETTER: Groups authorized to invoke this procedure */
    const CatalogMap<GroupRef> & authGroups() const;
    /** GETTER: Per table configuration */
    const CatalogMap<ConnectorTableInfo> & tableInfo() const;
};

} // End catalog namespace
} // End nstore namespace
