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

 public:

    // Singleton
    static Catalog& GetInstance();

     /**
      * Create a new Catalog hierarchy.
      */
     Catalog();
     virtual ~Catalog();

     void PurgeDeletions();

     /**
      * Run one or more single-line catalog commands separated by newlines.
      * See the docs for more info on catalog statements.
      * @param stmts A string containing one or more catalog commands separated by
      * newlines
      */
     void Execute(const std::string &stmts);

     /** GETTER: The set of the clusters in this catalog */
     const CatalogMap<Cluster> & GetClusters() const;

     /** pass in a buffer at least half as long as the string */
     static void HexDecodeString(const std::string &hexString, char *buffer);

     /** pass in a buffer at twice as long as the string */
     static void HexEncodeString(const char *string, char *buffer);

     /** return by out-param a copy of the recently deleted paths. */
     void GetDeletedPaths(std::vector<std::string> &deletions);

     virtual void Update();
     virtual CatalogType *AddChild(const std::string &collectionName, const std::string &childName);
     virtual CatalogType *GetChild(const std::string &collectionName, const std::string &childName) const;
     virtual bool RemoveChild(const std::string &collectionName, const std::string &childName);

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

    void ExecuteOne(const std::string &stmt);
    CatalogType * ItemForRef(const std::string &ref);
    CatalogType * ItemForPath(const CatalogType *parent, const std::string &path);
    CatalogType * ItemForPathPart(const CatalogType *parent, const std::string &pathPart) const;

    void RegisterGlobally(CatalogType *catObj);
    void UnregisterGlobally(CatalogType *catObj);

    static std::vector<std::string> SplitString(const std::string &str, char delimiter);
    static std::vector<std::string> SplitToTwoString(const std::string &str, char delimiter);

    void AddUnresolvedInfo(std::string path, CatalogType *type, std::string fieldName);

private:

    void ResolveUnresolvedInfo(std::string path);

    void CleanupExecutionBookkeeping();

};

} // End catalog namespace
} // End nstore namespace
