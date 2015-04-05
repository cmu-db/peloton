#include <cstdlib>
#include <cassert>
#include <functional>
#include <iostream>
#include <stdio.h>
#include <string.h>

#include "catalog.h"

#include "catalog_type.h"
#include "cluster.h"
#include "common/exception.h"

namespace nstore {
namespace catalog {

Catalog::Catalog()
: CatalogType(this, NULL, "/", "catalog"), m_clusters(this, this, "/clusters") {
    m_allCatalogObjects["/"] = this;
    m_childCollections["clusters"] = &m_clusters;
    m_relativeIndex = 1;
}

Catalog::~Catalog() {
    std::map<std::string, Cluster*>::const_iterator cluster_iter = m_clusters.begin();
    while (cluster_iter != m_clusters.end()) {
        delete cluster_iter->second;
        cluster_iter++;
    }
    m_clusters.clear();
}

/*
 * Clear the wasAdded/wasUpdated and deletion path lists.
 */
void Catalog::CleanupExecutionBookkeeping() {
    // sad there isn't clean syntax to for_each a map's pair->second
    boost::unordered_map<std::string, CatalogType*>::iterator iter;
    for (iter = m_allCatalogObjects.begin(); iter != m_allCatalogObjects.end(); iter++) {
        CatalogType *ct = iter->second;
        ct->clearUpdateStatus();
    }
    m_deletions.clear();
}

void Catalog::PurgeDeletions() {
    for (std::vector<std::string>::iterator i = m_deletions.begin();
         i != m_deletions.end();
         i++)
    {
        boost::unordered_map<std::string, CatalogType*>::iterator object = m_allCatalogObjects.find(*i);
        if (object == m_allCatalogObjects.end()) {
            std::string errmsg = "Catalog reference for " + (*i) + " not found.";
            throw CatalogException(errmsg);
        }
        delete object->second;
    }
    m_deletions.clear();
}

void Catalog::Execute(const std::string &stmts) {
    CleanupExecutionBookkeeping();

    std::vector<std::string> lines = SplitString(stmts, '\n');
    for (uint32_t i = 0; i < lines.size(); ++i) {
        ExecuteOne(lines[i]);
    }

    if (m_unresolved.size() > 0) {
        throw CatalogException("failed to execute catalog");
    }
}

/*
 * Produce constituent elements of catalog command.
 */
static void parse(const std::string &stmt,
                  std::string &command,
                  std::string &ref,
                  std::string &coll,
                  std::string &child)
{
    // stmt is formatted as one of:
    // add ref collection name
    // set ref fieldname value
    // ref = path | guid
    // parsed into std::strings: command ref coll child

    size_t pos = 0;
    size_t end = stmt.find(' ', pos);
    command = stmt.substr(pos, end - pos);
    pos = end + 1;
    end = stmt.find(' ', pos);
    ref = stmt.substr(pos, end - pos);
    pos = end + 1;
    end = stmt.find(' ', pos);
    coll = stmt.substr(pos, end - pos);
    pos = end + 1;
    end = stmt.length() + 1;
    child = stmt.substr(pos, end - pos);

    // cout << std::endl << "Configuring catalog: " << std::endl;
    // cout << "Command: " << command << endl;
    // cout << "Ref: " << ref << endl;
    // cout << "A: " << coll << endl;
    // cout << "B: " << child << endl;
}

/*
 * Run one catalog command.
 */
void Catalog::ExecuteOne(const std::string &stmt) {
    std::string command, ref, coll, child;
    parse(stmt, command, ref, coll, child);

    CatalogType *item = ItemForRef(ref);
    if (item == NULL) {
        std::string errmsg = "Catalog reference for " + ref + " not found.";
        throw CatalogException(errmsg);
    }

    // execute
    if (command.compare("add") == 0) {
        CatalogType *type = item->AddChild(coll, child);
        if (type == NULL) {
            throw CatalogException("Catalog failed to add child. Stmt :: " + stmt);
        }
        type->added();
        ResolveUnresolvedInfo(type->GetPath());
    }
    else if (command.compare("set") == 0) {
        item->Set(coll, child);
        item->updated();
    }
    else if (command.compare("delete") == 0) {
        // remove from collection and hash path to the deletion tracker
        // throw if nothing was removed.
        if(item->RemoveChild(coll, child)) {
            m_deletions.push_back(ref + "/" + coll + "[" + child + "]");
        }
        else {
            std::string errmsg = "Catalog reference for " + ref + " not found.";
            throw CatalogException(errmsg);
        }
    }
    else {
        throw CatalogException("Invalid catalog command.");
    }
}

const CatalogMap<Cluster> & Catalog::GetClusters() const {
    return m_clusters;
}

CatalogType *Catalog::ItemForRef(const std::string &ref) {
    // if it's a path
    boost::unordered_map<std::string, CatalogType*>::const_iterator iter;
    iter = m_allCatalogObjects.find(ref);
    if (iter != m_allCatalogObjects.end())
        return iter->second;
    else
        return NULL;
}

CatalogType *Catalog::ItemForPath(const CatalogType *parent, const std::string &path) {
    std::string realpath = path;
    if (path.at(0) == '/')
        realpath = realpath.substr(1);

    // root case
    if (realpath.length() == 0)
        return this;

    std::vector<std::string> parts = SplitToTwoString(realpath, '/');

    // child of root
    if (parts.size() <= 1)
        return ItemForPathPart(parent, parts[0]);

    CatalogType *nextParent = ItemForPathPart(parent, parts[0]);
    if (nextParent == NULL)
        return NULL;
    return ItemForPath(nextParent, parts[1]);
}

CatalogType *Catalog::ItemForPathPart(const CatalogType *parent, const std::string &pathPart) const {
    std::vector<std::string> parts = SplitToTwoString(pathPart, '[');
    if (parts.size() <= 1)
        return NULL;
    parts[1] = SplitString(parts[1], ']')[0];
    return parent->GetChild(parts[0], parts[1]);
}

void Catalog::RegisterGlobally(CatalogType *catObj) {
    boost::unordered_map<std::string, CatalogType*>::iterator iter
      = m_allCatalogObjects.find(catObj->GetPath());
    if (iter != m_allCatalogObjects.end() && iter->second != catObj ) {
        // this is a defect if it happens
        printf("Replacing object at path: %s (%p,%p)\n",
               catObj->GetPath().c_str(), iter->second, catObj);
        assert(false);
    }
    m_allCatalogObjects[catObj->GetPath()] = catObj;
}

void Catalog::UnregisterGlobally(CatalogType *catObj) {
    boost::unordered_map<std::string, CatalogType*>::iterator iter
      = m_allCatalogObjects.find(catObj->GetPath());
    if (iter != m_allCatalogObjects.end()) {
        m_allCatalogObjects.erase(iter);
    }
}

void Catalog::Update() {
    // nothing to do
}

std::vector<std::string> Catalog::SplitString(const std::string &str, char delimiter) {
    std::vector<std::string> vec;
    size_t begin = 0;
    while (true) {
        size_t end = str.find(delimiter, begin);
        if (end == std::string::npos) {
            if (begin != str.size()) {
                vec.push_back(str.substr(begin));
            }
            break;
        }
        vec.push_back(str.substr(begin, end - begin));
        begin = end + 1;
    }
    return vec;
}
std::vector<std::string> Catalog::SplitToTwoString(const std::string &str, char delimiter) {
    std::vector<std::string> vec;
    size_t end = str.find(delimiter);
    if (end == std::string::npos) {
        vec.push_back(str);
    } else {
        vec.push_back(str.substr(0, end));
        vec.push_back(str.substr(end + 1));
    }
    return vec;
}

/*
 * Add a path to the unresolved list to be processed when
 * the referenced value appears
 */
void Catalog::AddUnresolvedInfo(std::string path, CatalogType *type, std::string fieldName) {
    assert(type != NULL);

    UnresolvedInfo ui;
    ui.field = fieldName;
    ui.type = type;

    std::list<UnresolvedInfo> lui = m_unresolved[path];
    lui.push_back(ui);
    m_unresolved[path] = lui;
}

/*
 * Clean up any resolved binding for path.
 */
void Catalog::ResolveUnresolvedInfo(std::string path) {
    if (m_unresolved.count(path) != 0) {
        std::list<UnresolvedInfo> lui = m_unresolved[path];
        m_unresolved.erase(path);
        std::list<UnresolvedInfo>::const_iterator iter;
        for (iter = lui.begin(); iter != lui.end(); iter++) {
            UnresolvedInfo ui = *iter;
            std::string path2 = "set " + ui.type->GetPath() + " "
              + ui.field + " " + path;
            ExecuteOne(path2);
        }
    }
}

CatalogType *
Catalog::AddChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("clusters") == 0) {
        CatalogType *exists = m_clusters.get(childName);
        if (exists)
            throw CatalogException("trying to add a duplicate value.");
        return m_clusters.add(childName);
    }

    return NULL;
}

CatalogType *
Catalog::GetChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("clusters") == 0)
        return m_clusters.get(childName);
    return NULL;
}

bool
Catalog::RemoveChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("clusters") == 0) {
        return m_clusters.remove(childName);
    }
    return false;
}

/** takes in 0-F, returns 0-15 */
int32_t hexCharToInt(char c) {
    c = static_cast<char>(toupper(c));
    assert ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'));
    int32_t retval;
    if (c >= 'A')
        retval = c - 'A' + 10;
    else
        retval = c - '0';
    assert(retval >=0 && retval < 16);
    return retval;
}

/** pass in a buffer at least half as long as the std::string */
void Catalog::HexDecodeString(const std::string &hexString, char *buffer) {
    assert (buffer);
    uint32_t i;
    for (i = 0; i < hexString.length() / 2; i++) {
        int32_t high = hexCharToInt(hexString[i * 2]);

        int32_t low = hexCharToInt(hexString[i * 2 + 1]);
        int32_t result = high * 16 + low;
        assert (result >= 0 && result < 256);
        buffer[i] = static_cast<char>(result);
    }
    buffer[i] = '\0';
}

/** pass in a buffer at least twice as long as the std::string */
void Catalog::HexEncodeString(const char *string, char *buffer) {
    assert (buffer);
    uint32_t i = 0;
    for (; i < strlen(string); i++) {
        char ch[2];
        snprintf(ch, 2, "%x", (string[i] >> 4) & 0xF);
        buffer[i * 2] = ch[0];
        snprintf(ch, 2, "%x", string[i] & 0xF);
        buffer[(i * 2) + 1] = ch[0];
    }
    buffer[i*2] = '\0';
}

void Catalog::GetDeletedPaths(std::vector<std::string> &deletions) {
    copy(m_deletions.begin(), m_deletions.end(), back_inserter(deletions));
}

} // End catalog namespace
} // End nstore namespace

