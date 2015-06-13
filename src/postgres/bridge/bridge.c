/*
 * bridge.c
 *
 *  Created on: Jun 11, 2015
 *      Author: Jinwoong Kim
 */

#include "postgres.h"

#include "access/heapam.h"
#include "bridge/bridge.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/**
 * @brief Getting the relation name.
 * @param relation_id relation id
 */
char* GetRelationName(Oid relation_id) {
  Relation relation;
  char *relation_name;

  relation = relation_open(relation_id, NoLock);

  if(relation == NULL)
    return "";

  // Get name for given relation
  relation_name = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)), RelationGetRelationName(relation));

  // Simple version of above function
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

