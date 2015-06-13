/*
 * bridge.h
 *
 *  Created on: Jun 11, 2015
 *      Author: parallels
 */

#pragma once

#include "postgres.h"

char* GetRelationName(Oid relation_id);

int GetNumberOfAttributes(Oid relation_id);
