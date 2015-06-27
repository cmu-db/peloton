/*------------------------------------------------------------------------
 *
 * geqo_random.c
 *	   random number generator
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/geqo/geqo_random.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "optimizer/geqo_random.h"


void
geqo_set_seed(PlannerInfo *root, double seed)
{
	GeqoPrivateData *cprivate = (GeqoPrivateData *) root->join_search_private;

	/*
	 * XXX. This seeding algorithm could certainly be improved - but it is not
	 * critical to do so.
	 */
	memset(cprivate->random_state, 0, sizeof(cprivate->random_state));
	memcpy(cprivate->random_state,
		   &seed,
		   Min(sizeof(cprivate->random_state), sizeof(seed)));
}

double
geqo_rand(PlannerInfo *root)
{
	GeqoPrivateData *cprivate = (GeqoPrivateData *) root->join_search_private;

	return pg_erand48(cprivate->random_state);
}
