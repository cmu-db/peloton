/*-------------------------------------------------------------------------
 *
 * abstract_catalog_object.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"

#include <string>
#include <mutex>

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Abstract Catalog Object
//===--------------------------------------------------------------------===//

/**
 * Base class for all catalog objecets
 */
class AbstractCatalogObject {
    
protected:

    // Constructor
    // TODO: Need to decide whether we want to include a pointer to
    //       the parent catalog object for this one. We used that a lot in the
    //       analysis code for H-Store but we probably don't need it for this.
    AbstractCatalogObject(oid_t id, std::string name) :
        id_(id),
        name_(name) {
        
        // Nothing else to do!
    }
    
    // Deconstructor
    ~AbstractCatalogObject() {
        // I don't think there is anything that we need to do here
    }

public:
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//
    
    inline oid_t GetId() const {
        return id_;
    }

    inline std::string GetName() const {
        return name_;
    }
    
    inline void Lock() {
        mutex_.lock();
    }

    inline void Unlock() {
        mutex_.unlock();
    }
    
private:

    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//
    
    // The unique identifier for this catalog object
    oid_t id_;
    
    // The name of this catalog object
    // All catalog objects need to have a name.
    std::string name_;
    
    // Lock for this single
    // TODO: Determine whether we really need this per catalog object. We probably don't
    // and we're wasting memory by allocating it.
    std::mutex mutex_;


};

} // End catalog namespace
} // End nstore namespace


