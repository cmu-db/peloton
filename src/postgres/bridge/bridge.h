/*
 * bridge.h
 *
 *  Created on: Jun 11, 2015
 *      Author: parallels
 */

#pragma once

#include "postgres.h"

class Bridge {

 public:
  static char* GetRelationName(Oid relation_id);

};
