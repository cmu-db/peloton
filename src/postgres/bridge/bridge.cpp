/*
 * bridge.cpp
 *
 *  Created on: Jun 11, 2015
 *      Author: parallels
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
   * @param relation id relation id
   */
  static char* GetRelationName(Oid relation_id) {
      Relation relation;
      char *relation_name;

      relation = relation_open(relation_id, NoLock);

      // Get name for given relation
      relation_name = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)), RelationGetRelationName(relation));

      relation_close(relation, NoLock);

      return relation_name;
   }

};


