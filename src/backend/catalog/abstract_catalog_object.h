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

#include "backend/common/types.h"

#include <string>
#include <mutex>

namespace peloton {
namespace catalog {

/**
 * Base class for all catalog objects
 * @author pavlo
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
    // TODO: We need to decide whether this id is unique for given type of catalog object
    //       within a database (across all columns) or whether it's globally unique.
    //       If it's going to be globally unique then we will want this to be a 64-bit int.
    //       It may also cause problems if we want to really wide tables, since now we
    //       need to store an entire column catalog object for it.
    //       We should discusss this design goal to determine how feasible it is. For example, we 
    //       could have special column groups that have an auto-generated name that 
    //       we don't have to store a unique names for each time and can use a single type
    oid_t id_;
    
    // The name of this catalog object
    // All catalog objects need to have a name.
    std::string name_;
    
    // Lock for this single
    // TODO: Determine whether we really need this per catalog object. We probably don't
    //       and we're wasting memory by allocating it.
    std::mutex mutex_;

};

} // End catalog namespace
} // End peloton namespace


