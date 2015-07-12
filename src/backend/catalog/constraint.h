/*-------------------------------------------------------------------------
 *
 * constraint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/constraint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/logger.h"

#include "nodes/nodes.h" // TODO :: REMOVE, just for raw expr

#include <string>
#include <vector>
#include <iostream>

namespace peloton {

namespace index {
class Index;
}
namespace storage {
class DataTable;
}

namespace catalog {

class ReferenceTableInfo;

//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class Constraint
{

public:
    // Configure ( type [, name] )
    Constraint( ConstraintType type, std::string name = "" )
                : type(type), name(name) {}

    // Configure ( type, raw expression )
    Constraint( ConstraintType type, Node* _raw_default_expr )
                : type(type) { raw_default_expr = (Node*) copyObject((void*) _raw_default_expr ); }
 
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    ConstraintType GetType() const {
        return type;
    }

    std::string GetName() const {
        return name;
    }

    inline void SetReferenceTablePosition( int position ) {
      if( position < 0 ){
        LOG_ERROR(" ReferenceTable position can be negative !!");
      }
      reference_table_position = position;
    }

    inline void SetUniqueIndexPosition( int position ) {
      if( position < 0 ){
        LOG_ERROR(" Unique Index position can be negative !!");
      }
      unique_index_position = position;
    }

    // Get a string representation of this constraint
    // TODO Print out 
    friend std::ostream& operator<<(std::ostream& os, const Constraint& constraint);

private:
    
    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//
    
    // The type of constraint
    ConstraintType type = CONSTRAINT_TYPE_INVALID;

    // Constraint name ( if needed )
    std::string name = "";

    // FIXME :: Cooked expr
    // Raw_default_expr ( if needed )
    Node* raw_default_expr = nullptr;

    // Unique index and reference table position in Table
    int reference_table_position = -1;
    int unique_index_position = -1;

};


//===--------------------------------------------------------------------===//
// ReferenceTable Class
//===--------------------------------------------------------------------===//

class ReferenceTableInfo {

  public:
  ReferenceTableInfo( storage::DataTable* PrimaryKeyTable,
                      std::vector<std::string> pk_column_names,
                      std::vector<std::string> fk_column_names,
                      char fk_update_action,
                      char fk_delete_action,
                      std::string& name ) 

                      : PrimaryKeyTable(PrimaryKeyTable),
                      pk_column_names(pk_column_names),
                      fk_column_names(fk_column_names),
                      fk_update_action(fk_update_action),
                      fk_delete_action(fk_delete_action),
                      name(name)
                      { }

  std::vector<std::string> GetFKColumnNames(){
    return fk_column_names;
  }

  storage::DataTable* GetPrimaryKeyTable()
  {
    return PrimaryKeyTable;
  }

  private:

  storage::DataTable* PrimaryKeyTable = nullptr;

  std::vector<std::string> pk_column_names;
  std::vector<std::string> fk_column_names;

  char fk_update_action;
  char fk_delete_action;

  std::string name ;

};



} // End catalog namespace
} // End peloton namespace
