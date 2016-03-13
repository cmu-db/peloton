/*
    Copyright (c) 2012-2014 Martin Sustrik  All rights reserved.
    Copyright (c) 2013 GoPivotal, Inc.  All rights reserved.

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom
    the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
    IN THE SOFTWARE.
*/

#include "tcpmux.h"
#include "btcpmux.h"
#include "ctcpmux.h"

#include "../../tcpmux.h"

#include "../utils/port.h"
#include "../utils/iface.h"

#include "../../utils/err.h"
#include "../../utils/alloc.h"
#include "../../utils/fast.h"
#include "../../utils/list.h"
#include "../../utils/cont.h"

#include <string.h>

#if defined NN_HAVE_WINDOWS
#include "../../utils/win.h"
#else
#include <unistd.h>
#endif

/*  TCPMUX-specific socket options. */

struct nn_tcpmux_optset {
    struct nn_optset base;
    int nodelay;
};

static void nn_tcpmux_optset_destroy (struct nn_optset *self);
static int nn_tcpmux_optset_setopt (struct nn_optset *self, int option,
    const void *optval, size_t optvallen);
static int nn_tcpmux_optset_getopt (struct nn_optset *self, int option,
    void *optval, size_t *optvallen);
static const struct nn_optset_vfptr nn_tcpmux_optset_vfptr = {
    nn_tcpmux_optset_destroy,
    nn_tcpmux_optset_setopt,
    nn_tcpmux_optset_getopt
};

/*  nn_transport interface. */
static int nn_tcpmux_bind (void *hint, struct nn_epbase **epbase);
static int nn_tcpmux_connect (void *hint, struct nn_epbase **epbase);
static struct nn_optset *nn_tcpmux_optset (void);

static struct nn_transport nn_tcpmux_vfptr = {
    "tcpmux",
    NN_TCPMUX,
    NULL,
    NULL,
    nn_tcpmux_bind,
    nn_tcpmux_connect,
    nn_tcpmux_optset,
    NN_LIST_ITEM_INITIALIZER
};

struct nn_transport *nn_tcpmux = &nn_tcpmux_vfptr;

static int nn_tcpmux_bind (void *hint, struct nn_epbase **epbase)
{
#if defined NN_HAVE_WINDOWS
    return -EPROTONOSUPPORT;
#else
    return nn_btcpmux_create (hint, epbase);
#endif
}

static int nn_tcpmux_connect (void *hint, struct nn_epbase **epbase)
{
    return nn_ctcpmux_create (hint, epbase);
}

static struct nn_optset *nn_tcpmux_optset ()
{
    struct nn_tcpmux_optset *optset;

    optset = nn_alloc (sizeof (struct nn_tcpmux_optset), "optset (tcpmux)");
    alloc_assert (optset);
    optset->base.vfptr = &nn_tcpmux_optset_vfptr;

    /*  Default values for TCPMUX socket options. */
    optset->nodelay = 0;

    return &optset->base;   
}

static void nn_tcpmux_optset_destroy (struct nn_optset *self)
{
    struct nn_tcpmux_optset *optset;

    optset = nn_cont (self, struct nn_tcpmux_optset, base);
    nn_free (optset);
}

static int nn_tcpmux_optset_setopt (struct nn_optset *self, int option,
    const void *optval, size_t optvallen)
{
    struct nn_tcpmux_optset *optset;
    int val;

    optset = nn_cont (self, struct nn_tcpmux_optset, base);

    /*  At this point we assume that all options are of type int. */
    if (optvallen != sizeof (int))
        return -EINVAL;
    val = *(int*) optval;

    switch (option) {
    case NN_TCPMUX_NODELAY:
        if (nn_slow (val != 0 && val != 1))
            return -EINVAL;
        optset->nodelay = val;
        return 0;
    default:
        return -ENOPROTOOPT;
    }
}

static int nn_tcpmux_optset_getopt (struct nn_optset *self, int option,
    void *optval, size_t *optvallen)
{
    struct nn_tcpmux_optset *optset;
    int intval;

    optset = nn_cont (self, struct nn_tcpmux_optset, base);

    switch (option) {
    case NN_TCPMUX_NODELAY:
        intval = optset->nodelay;
        break;
    default:
        return -ENOPROTOOPT;
    }
    memcpy (optval, &intval,
        *optvallen < sizeof (int) ? *optvallen : sizeof (int));
    *optvallen = sizeof (int);
    return 0;
}

