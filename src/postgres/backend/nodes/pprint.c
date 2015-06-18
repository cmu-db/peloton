/*
 * pprint.c
 *	Copyright(c) 2015 CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 *
 */

#include "nodes/pprint.h"
#include "stdio.h"

static void print_planstate(const PlanState *planstate);
static void print_list(const List* list);
static const FILE *stream = stdout;

void print_planDesc(const QueryDesc *queryDesc) {
  print_planstate(queryDesc->planstate);
}

static void print_planstate(const PlanState *planstate) {
  if (planstate == NULL) {
    fprintf(stream, "Void\n");
  } else {
    fprintf(stream, "Plan\n");

    print_list(planstate->state->es_subplanstates);

    print_planstate(planstate->lefttree);
    print_planstate(planstate->righttree);

  }

}

static void print_list(const List* list) {
  ListCell *l;
  foreach(l, list)
  {
    PlanState *planstate = (PlanState *) lfirst(l);
    print_planstate(planstate);
  }
}

