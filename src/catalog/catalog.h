#pragma once

#include <vector>
#include <map>
#include <string>
#include <list>
#include "boost/unordered_map.hpp"

#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

class Cluster;

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

/**
 * The root class in the Catalog hierarchy, which is essentially a tree of
 * instances of CatalogType objects, accessed by guids globally, paths
 * globally, and child names when given a parent.
 *
 */
class Catalog : public CatalogType {
    friend class CatalogType;

protected:
    struct UnresolvedInfo {
        CatalogType * type;
        std::string field;
    };

    std::map<std::string, std::list<UnresolvedInfo> > m_unresolved;

    // it would be nice if this code was generated
    CatalogMap<Cluster> m_clusters;

    // for memory cleanup and fast-lookup purposes
    boost::unordered_map<std::string, CatalogType*> m_allCatalogObjects;

    //  paths of objects recently deleted from the catalog.
    std::vector<std::string> m_deletions;

    void executeOne(const std::string &stmt);
    CatalogType * itemForRef(const std::string &ref);
    CatalogType * itemForPath(const CatalogType *parent, const std::string &path);
    CatalogType * itemForPathPart(const CatalogType *parent, const std::string &pathPart) const;

    virtual void update();
    virtual CatalogType *addChild(const std::string &collectionName, const std::string &childName);
    virtual CatalogType *getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

    void RegisterGlobally(CatalogType *catObj);
    void UnregisterGlobally(CatalogType *catObj);

    static std::vector<std::string> splitString(const std::string &str, char delimiter);
    static std::vector<std::string> splitToTwoString(const std::string &str, char delimiter);

    void addUnresolvedInfo(std::string path, CatalogType *type, std::string fieldName);
private:
    void resolveUnresolvedInfo(std::string path);
    void cleanupExecutionBookkeeping();

public:
    void purgeDeletions();

    /**
     * Create a new Catalog hierarchy.
     */
    Catalog();
    virtual ~Catalog();

    /**
     * Run one or more single-line catalog commands separated by newlines.
     * See the docs for more info on catalog statements.
     * @param stmts A string containing one or more catalog commands separated by
     * newlines
     */
    void execute(const std::string &stmts);

    /** GETTER: The set of the clusters in this catalog */
    const CatalogMap<Cluster> & clusters() const;

    /** pass in a buffer at least half as long as the string */
    static void hexDecodeString(const std::string &hexString, char *buffer);

    /** pass in a buffer at twice as long as the string */
    static void hexEncodeString(const char *string, char *buffer);

    /** return by out-param a copy of the recently deleted paths. */
    void getDeletedPaths(std::vector<std::string> &deletions);
};

} // End catalog namespace
} // End nstore namespace
