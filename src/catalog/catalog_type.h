#pragma once

#include <map>
#include <string>
#include <stdint.h>

#include "catalog_map.h"

namespace nstore {
namespace catalog {

class Catalog;
class CatalogType;

struct CatalogValue {
    std::string strValue;
    int32_t intValue;
    CatalogType* typeValue;

    CatalogValue() : intValue(0), typeValue(NULL) {}
};

//===--------------------------------------------------------------------===//
// Catalog Type
//===--------------------------------------------------------------------===//

/**
 * The base class for all objects in the Catalog. CatalogType instances all
 * have a name, a guid and a path (from the root). They have fields and children.
 * All fields are simple types. All children are CatalogType instances.
 *
 */
class CatalogType {
    friend class Catalog;

  private:
    void clearUpdateStatus() {
        m_wasAdded = false;
        m_wasUpdated = false;
    }
    void added() {
        m_wasAdded = true;
    }

    void updated() {
        m_wasUpdated = true;
    }

    bool m_wasAdded;     // victim of 'add' command in catalog update
    bool m_wasUpdated;   // target of 'set' command in catalog update

  protected:
    std::map<std::string, CatalogValue> m_fields;
    // the void* is a giant hack, solutions welcome
    std::map<std::string, void*> m_childCollections;

    std::string m_name;
    std::string m_path;
    CatalogType *m_parent;
    Catalog *m_catalog;
    int32_t m_relativeIndex;

    CatalogType(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    virtual ~CatalogType();

    void Set(const std::string &field, const std::string &value);

    virtual void Update() = 0;

    virtual CatalogType * AddChild(const std::string &collectionName, const std::string &name) = 0;

    virtual CatalogType * GetChild(const std::string &collectionName, const std::string &childName) const = 0;

    // returns true if a child was deleted
    virtual bool RemoveChild(const std::string &collectionName, const std::string &childName) = 0;

public:

    /**
     * Get the parent of this CatalogType instance
     * @return The name of this CatalogType instance
     */
    std::string GetName() const;

    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    std::string GetPath() const;

    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    CatalogType * GetParent() const;

    /**
     * Get a pointer to the root Catalog instance for this CatalogType
     * instance
     * @return A pointer to the root Catalog instance
     */
    Catalog * GetCatalog() const;

    /**
     * Get the index of this node within its parent collection
     * @return The index of this node amongst its sibling nodes
     */
    int32_t GetRelativeIndex() const;

    bool WasAdded() const {
        return m_wasAdded;
    }

    bool WasUpdated() const {
        return m_wasUpdated;
    }
};

} // End catalog namespace
} // End nstore namespace
