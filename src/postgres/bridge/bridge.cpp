/*
 * bridge.cpp
 *
 *  Created on: Jun 11, 2015
 *      Author: Jinwoong Kim
 */

extern "C"{
#include "postgres.h"

#include "access/heapam.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
}

class Bridge {

 public:

  /**
   * @brief Getting the relation name.
   * @param relation_id relation id
   */
  static char* GetRelationName(Oid relation_id) {
      Relation relation;
      char *relation_name;

      relation = relation_open(relation_id, NoLock);

      // Get name for given relation
     relation_name = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)), RelationGetRelationName(relation));

      // Simply version of above function
     // relation_name = RelationGetRelationName(relation);


      relation_close(relation, NoLock);

      return relation_name;
   }

  /**
   * @brief Getting the number of attributes.
   * @param relation_id relation id
   */
  int GetNumberOfAttributes(Oid relation_id) {
      Relation relation;
      AttrNumber numOfAttris;
      
      relation = relation_open(relation_id, NoLock);
      
      //Get the number of attributes
      numOfAttris = RelationGetNumberOfAttributes(relation);

      relation_close(relation, NoLock);
      
      return numOfAttris;
  }
};

