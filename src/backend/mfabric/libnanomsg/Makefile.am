######################################################################
# LIBNANOMSG
######################################################################

nnincludedir = backend/mfabric/libnanomsg/

nninclude_HEADERS = \
    backend/mfabric/libnanomsg/src/nn.h \
    backend/mfabric/libnanomsg/src/inproc.h \
    backend/mfabric/libnanomsg/src/ipc.h \
    backend/mfabric/libnanomsg/src/tcp.h \
    backend/mfabric/libnanomsg/src/ws.h \
    backend/mfabric/libnanomsg/src/pair.h \
    backend/mfabric/libnanomsg/src/pubsub.h \
    backend/mfabric/libnanomsg/src/reqrep.h \
    backend/mfabric/libnanomsg/src/pipeline.h \
    backend/mfabric/libnanomsg/src/survey.h \
    backend/mfabric/libnanomsg/src/bus.h \
    backend/mfabric/libnanomsg/src/tcpmux.h

NANOMSG_CORE = \
    backend/mfabric/libnanomsg/src/core/ep.h \
    backend/mfabric/libnanomsg/src/core/ep.c \
    backend/mfabric/libnanomsg/src/core/epbase.c \
    backend/mfabric/libnanomsg/src/core/global.h \
    backend/mfabric/libnanomsg/src/core/global.c \
    backend/mfabric/libnanomsg/src/core/pipe.c \
    backend/mfabric/libnanomsg/src/core/poll.c \
    backend/mfabric/libnanomsg/src/core/sock.h \
    backend/mfabric/libnanomsg/src/core/sock.c \
    backend/mfabric/libnanomsg/src/core/sockbase.c \
    backend/mfabric/libnanomsg/src/core/symbol.c

NANOMSG_DEVICES =\
    backend/mfabric/libnanomsg/src/devices/device.c \
    backend/mfabric/libnanomsg/src/devices/device.h \
    backend/mfabric/libnanomsg/src/devices/tcpmuxd.c

NANOMSG_AIO = \
    backend/mfabric/libnanomsg/src/aio/ctx.h \
    backend/mfabric/libnanomsg/src/aio/ctx.c \
    backend/mfabric/libnanomsg/src/aio/fsm.h \
    backend/mfabric/libnanomsg/src/aio/fsm.c \
    backend/mfabric/libnanomsg/src/aio/poller.h \
    backend/mfabric/libnanomsg/src/aio/poller.c \
    backend/mfabric/libnanomsg/src/aio/poller_epoll.h \
    backend/mfabric/libnanomsg/src/aio/poller_epoll.inc \
    backend/mfabric/libnanomsg/src/aio/poller_kqueue.h \
    backend/mfabric/libnanomsg/src/aio/poller_kqueue.inc \
    backend/mfabric/libnanomsg/src/aio/poller_poll.h \
    backend/mfabric/libnanomsg/src/aio/poller_poll.inc \
    backend/mfabric/libnanomsg/src/aio/pool.h \
    backend/mfabric/libnanomsg/src/aio/pool.c \
    backend/mfabric/libnanomsg/src/aio/timer.h \
    backend/mfabric/libnanomsg/src/aio/timer.c \
    backend/mfabric/libnanomsg/src/aio/timerset.h \
    backend/mfabric/libnanomsg/src/aio/timerset.c \
    backend/mfabric/libnanomsg/src/aio/usock.h \
    backend/mfabric/libnanomsg/src/aio/usock.c \
    backend/mfabric/libnanomsg/src/aio/usock_posix.h \
    backend/mfabric/libnanomsg/src/aio/usock_posix.inc \
    backend/mfabric/libnanomsg/src/aio/usock_win.h \
    backend/mfabric/libnanomsg/src/aio/usock_win.inc \
    backend/mfabric/libnanomsg/src/aio/worker.h \
    backend/mfabric/libnanomsg/src/aio/worker.c \
    backend/mfabric/libnanomsg/src/aio/worker_posix.h \
    backend/mfabric/libnanomsg/src/aio/worker_posix.inc \
    backend/mfabric/libnanomsg/src/aio/worker_win.h \
    backend/mfabric/libnanomsg/src/aio/worker_win.inc

NANOMSG_UTILS = \
    backend/mfabric/libnanomsg/src/utils/alloc.h \
    backend/mfabric/libnanomsg/src/utils/alloc.c \
    backend/mfabric/libnanomsg/src/utils/atomic.h \
    backend/mfabric/libnanomsg/src/utils/atomic.c \
    backend/mfabric/libnanomsg/src/utils/attr.h \
    backend/mfabric/libnanomsg/src/utils/chunk.h \
    backend/mfabric/libnanomsg/src/utils/chunk.c \
    backend/mfabric/libnanomsg/src/utils/chunkref.h \
    backend/mfabric/libnanomsg/src/utils/chunkref.c \
    backend/mfabric/libnanomsg/src/utils/clock.h \
    backend/mfabric/libnanomsg/src/utils/clock.c \
    backend/mfabric/libnanomsg/src/utils/closefd.h \
    backend/mfabric/libnanomsg/src/utils/closefd.c \
    backend/mfabric/libnanomsg/src/utils/cont.h \
    backend/mfabric/libnanomsg/src/utils/efd.h \
    backend/mfabric/libnanomsg/src/utils/efd.c \
    backend/mfabric/libnanomsg/src/utils/efd_eventfd.h \
    backend/mfabric/libnanomsg/src/utils/efd_eventfd.inc \
    backend/mfabric/libnanomsg/src/utils/efd_pipe.h \
    backend/mfabric/libnanomsg/src/utils/efd_pipe.inc \
    backend/mfabric/libnanomsg/src/utils/efd_socketpair.h \
    backend/mfabric/libnanomsg/src/utils/efd_socketpair.inc \
    backend/mfabric/libnanomsg/src/utils/efd_win.h \
    backend/mfabric/libnanomsg/src/utils/efd_win.inc \
    backend/mfabric/libnanomsg/src/utils/err.h \
    backend/mfabric/libnanomsg/src/utils/err.c \
    backend/mfabric/libnanomsg/src/utils/fast.h \
    backend/mfabric/libnanomsg/src/utils/fd.h \
    backend/mfabric/libnanomsg/src/utils/glock.h \
    backend/mfabric/libnanomsg/src/utils/glock.c \
    backend/mfabric/libnanomsg/src/utils/hash.h \
    backend/mfabric/libnanomsg/src/utils/hash.c \
    backend/mfabric/libnanomsg/src/utils/int.h \
    backend/mfabric/libnanomsg/src/utils/list.h \
    backend/mfabric/libnanomsg/src/utils/list.c \
    backend/mfabric/libnanomsg/src/utils/msg.h \
    backend/mfabric/libnanomsg/src/utils/msg.c \
    backend/mfabric/libnanomsg/src/utils/mutex.h \
    backend/mfabric/libnanomsg/src/utils/mutex.c \
    backend/mfabric/libnanomsg/src/utils/queue.h \
    backend/mfabric/libnanomsg/src/utils/queue.c \
    backend/mfabric/libnanomsg/src/utils/random.h \
    backend/mfabric/libnanomsg/src/utils/random.c \
    backend/mfabric/libnanomsg/src/utils/sem.h \
    backend/mfabric/libnanomsg/src/utils/sem.c \
    backend/mfabric/libnanomsg/src/utils/sleep.h \
    backend/mfabric/libnanomsg/src/utils/sleep.c \
    backend/mfabric/libnanomsg/src/utils/stopwatch.h \
    backend/mfabric/libnanomsg/src/utils/stopwatch.c \
    backend/mfabric/libnanomsg/src/utils/thread.h \
    backend/mfabric/libnanomsg/src/utils/thread.c \
    backend/mfabric/libnanomsg/src/utils/thread_posix.h \
    backend/mfabric/libnanomsg/src/utils/thread_posix.inc \
    backend/mfabric/libnanomsg/src/utils/thread_win.h \
    backend/mfabric/libnanomsg/src/utils/thread_win.inc \
    backend/mfabric/libnanomsg/src/utils/win.h \
    backend/mfabric/libnanomsg/src/utils/wire.h \
    backend/mfabric/libnanomsg/src/utils/wire.c

PROTOCOLS_UTILS = \
    backend/mfabric/libnanomsg/src/protocols/utils/dist.h \
    backend/mfabric/libnanomsg/src/protocols/utils/dist.c \
    backend/mfabric/libnanomsg/src/protocols/utils/excl.h \
    backend/mfabric/libnanomsg/src/protocols/utils/excl.c \
    backend/mfabric/libnanomsg/src/protocols/utils/fq.h \
    backend/mfabric/libnanomsg/src/protocols/utils/fq.c \
    backend/mfabric/libnanomsg/src/protocols/utils/lb.h \
    backend/mfabric/libnanomsg/src/protocols/utils/lb.c \
    backend/mfabric/libnanomsg/src/protocols/utils/priolist.h \
    backend/mfabric/libnanomsg/src/protocols/utils/priolist.c

PROTOCOLS_BUS = \
    backend/mfabric/libnanomsg/src/protocols/bus/bus.h \
    backend/mfabric/libnanomsg/src/protocols/bus/bus.c \
    backend/mfabric/libnanomsg/src/protocols/bus/xbus.h \
    backend/mfabric/libnanomsg/src/protocols/bus/xbus.c

PROTOCOLS_PIPELINE = \
    backend/mfabric/libnanomsg/src/protocols/pipeline/push.h \
    backend/mfabric/libnanomsg/src/protocols/pipeline/push.c \
    backend/mfabric/libnanomsg/src/protocols/pipeline/pull.h \
    backend/mfabric/libnanomsg/src/protocols/pipeline/pull.c \
    backend/mfabric/libnanomsg/src/protocols/pipeline/xpull.h \
    backend/mfabric/libnanomsg/src/protocols/pipeline/xpull.c \
    backend/mfabric/libnanomsg/src/protocols/pipeline/xpush.h \
    backend/mfabric/libnanomsg/src/protocols/pipeline/xpush.c

PROTOCOLS_PAIR = \
    backend/mfabric/libnanomsg/src/protocols/pair/pair.h \
    backend/mfabric/libnanomsg/src/protocols/pair/pair.c \
    backend/mfabric/libnanomsg/src/protocols/pair/xpair.h \
    backend/mfabric/libnanomsg/src/protocols/pair/xpair.c

PROTOCOLS_PUBSUB = \
    backend/mfabric/libnanomsg/src/protocols/pubsub/pub.h \
    backend/mfabric/libnanomsg/src/protocols/pubsub/pub.c \
    backend/mfabric/libnanomsg/src/protocols/pubsub/sub.h \
    backend/mfabric/libnanomsg/src/protocols/pubsub/sub.c \
    backend/mfabric/libnanomsg/src/protocols/pubsub/trie.h \
    backend/mfabric/libnanomsg/src/protocols/pubsub/trie.c \
    backend/mfabric/libnanomsg/src/protocols/pubsub/xpub.h \
    backend/mfabric/libnanomsg/src/protocols/pubsub/xpub.c \
    backend/mfabric/libnanomsg/src/protocols/pubsub/xsub.h \
    backend/mfabric/libnanomsg/src/protocols/pubsub/xsub.c

PROTOCOLS_REQREP = \
    backend/mfabric/libnanomsg/src/protocols/reqrep/req.h \
    backend/mfabric/libnanomsg/src/protocols/reqrep/req.c \
    backend/mfabric/libnanomsg/src/protocols/reqrep/rep.h \
    backend/mfabric/libnanomsg/src/protocols/reqrep/rep.c \
    backend/mfabric/libnanomsg/src/protocols/reqrep/task.h \
    backend/mfabric/libnanomsg/src/protocols/reqrep/task.c \
    backend/mfabric/libnanomsg/src/protocols/reqrep/xrep.h \
    backend/mfabric/libnanomsg/src/protocols/reqrep/xrep.c \
    backend/mfabric/libnanomsg/src/protocols/reqrep/xreq.h \
    backend/mfabric/libnanomsg/src/protocols/reqrep/xreq.c

PROTOCOLS_SURVEY = \
    backend/mfabric/libnanomsg/src/protocols/survey/respondent.h \
    backend/mfabric/libnanomsg/src/protocols/survey/respondent.c \
    backend/mfabric/libnanomsg/src/protocols/survey/surveyor.h \
    backend/mfabric/libnanomsg/src/protocols/survey/surveyor.c \
    backend/mfabric/libnanomsg/src/protocols/survey/xrespondent.h \
    backend/mfabric/libnanomsg/src/protocols/survey/xrespondent.c \
    backend/mfabric/libnanomsg/src/protocols/survey/xsurveyor.h \
    backend/mfabric/libnanomsg/src/protocols/survey/xsurveyor.c

NANOMSG_PROTOCOLS = \
    $(PROTOCOLS_BUS) \
    $(PROTOCOLS_PIPELINE) \
    $(PROTOCOLS_PAIR) \
    $(PROTOCOLS_PUBSUB) \
    $(PROTOCOLS_REQREP) \
    $(PROTOCOLS_SURVEY) \
    $(PROTOCOLS_UTILS)

TRANSPORTS_UTILS = \
    backend/mfabric/libnanomsg/src/transports/utils/backoff.h \
    backend/mfabric/libnanomsg/src/transports/utils/backoff.c \
    backend/mfabric/libnanomsg/src/transports/utils/dns.h \
    backend/mfabric/libnanomsg/src/transports/utils/dns.c \
    backend/mfabric/libnanomsg/src/transports/utils/dns_getaddrinfo.h \
    backend/mfabric/libnanomsg/src/transports/utils/dns_getaddrinfo.inc \
    backend/mfabric/libnanomsg/src/transports/utils/dns_getaddrinfo_a.h \
    backend/mfabric/libnanomsg/src/transports/utils/dns_getaddrinfo_a.inc \
    backend/mfabric/libnanomsg/src/transports/utils/iface.h \
    backend/mfabric/libnanomsg/src/transports/utils/iface.c \
    backend/mfabric/libnanomsg/src/transports/utils/literal.h \
    backend/mfabric/libnanomsg/src/transports/utils/literal.c \
    backend/mfabric/libnanomsg/src/transports/utils/port.h \
    backend/mfabric/libnanomsg/src/transports/utils/port.c \
    backend/mfabric/libnanomsg/src/transports/utils/streamhdr.h \
    backend/mfabric/libnanomsg/src/transports/utils/streamhdr.c \
    backend/mfabric/libnanomsg/src/transports/utils/base64.h \
    backend/mfabric/libnanomsg/src/transports/utils/base64.c

TRANSPORTS_INPROC = \
    backend/mfabric/libnanomsg/src/transports/inproc/binproc.h \
    backend/mfabric/libnanomsg/src/transports/inproc/binproc.c \
    backend/mfabric/libnanomsg/src/transports/inproc/cinproc.h \
    backend/mfabric/libnanomsg/src/transports/inproc/cinproc.c \
    backend/mfabric/libnanomsg/src/transports/inproc/inproc.h \
    backend/mfabric/libnanomsg/src/transports/inproc/inproc.c \
    backend/mfabric/libnanomsg/src/transports/inproc/ins.h \
    backend/mfabric/libnanomsg/src/transports/inproc/ins.c \
    backend/mfabric/libnanomsg/src/transports/inproc/msgqueue.h \
    backend/mfabric/libnanomsg/src/transports/inproc/msgqueue.c \
    backend/mfabric/libnanomsg/src/transports/inproc/sinproc.h \
    backend/mfabric/libnanomsg/src/transports/inproc/sinproc.c

TRANSPORTS_IPC = \
    backend/mfabric/libnanomsg/src/transports/ipc/aipc.h \
    backend/mfabric/libnanomsg/src/transports/ipc/aipc.c \
    backend/mfabric/libnanomsg/src/transports/ipc/bipc.h \
    backend/mfabric/libnanomsg/src/transports/ipc/bipc.c \
    backend/mfabric/libnanomsg/src/transports/ipc/cipc.h \
    backend/mfabric/libnanomsg/src/transports/ipc/cipc.c \
    backend/mfabric/libnanomsg/src/transports/ipc/ipc.h \
    backend/mfabric/libnanomsg/src/transports/ipc/ipc.c \
    backend/mfabric/libnanomsg/src/transports/ipc/sipc.h \
    backend/mfabric/libnanomsg/src/transports/ipc/sipc.c

TRANSPORTS_TCP = \
    backend/mfabric/libnanomsg/src/transports/tcp/atcp.h \
    backend/mfabric/libnanomsg/src/transports/tcp/atcp.c \
    backend/mfabric/libnanomsg/src/transports/tcp/btcp.h \
    backend/mfabric/libnanomsg/src/transports/tcp/btcp.c \
    backend/mfabric/libnanomsg/src/transports/tcp/ctcp.h \
    backend/mfabric/libnanomsg/src/transports/tcp/ctcp.c \
    backend/mfabric/libnanomsg/src/transports/tcp/stcp.h \
    backend/mfabric/libnanomsg/src/transports/tcp/stcp.c \
    backend/mfabric/libnanomsg/src/transports/tcp/tcp.h \
    backend/mfabric/libnanomsg/src/transports/tcp/tcp.c

TRANSPORTS_WS = \
    backend/mfabric/libnanomsg/src/transports/ws/aws.h \
    backend/mfabric/libnanomsg/src/transports/ws/aws.c \
    backend/mfabric/libnanomsg/src/transports/ws/bws.h \
    backend/mfabric/libnanomsg/src/transports/ws/bws.c \
    backend/mfabric/libnanomsg/src/transports/ws/cws.h \
    backend/mfabric/libnanomsg/src/transports/ws/cws.c \
    backend/mfabric/libnanomsg/src/transports/ws/sws.h \
    backend/mfabric/libnanomsg/src/transports/ws/sws.c \
    backend/mfabric/libnanomsg/src/transports/ws/ws.h \
    backend/mfabric/libnanomsg/src/transports/ws/ws.c \
    backend/mfabric/libnanomsg/src/transports/ws/ws_handshake.h \
    backend/mfabric/libnanomsg/src/transports/ws/ws_handshake.c \
    backend/mfabric/libnanomsg/src/transports/ws/sha1.h \
    backend/mfabric/libnanomsg/src/transports/ws/sha1.c

TRANSPORTS_TCPMUX = \
    backend/mfabric/libnanomsg/src/transports/tcpmux/atcpmux.h \
    backend/mfabric/libnanomsg/src/transports/tcpmux/atcpmux.c \
    backend/mfabric/libnanomsg/src/transports/tcpmux/btcpmux.h \
    backend/mfabric/libnanomsg/src/transports/tcpmux/btcpmux.c \
    backend/mfabric/libnanomsg/src/transports/tcpmux/ctcpmux.h \
    backend/mfabric/libnanomsg/src/transports/tcpmux/ctcpmux.c \
    backend/mfabric/libnanomsg/src/transports/tcpmux/stcpmux.h \
    backend/mfabric/libnanomsg/src/transports/tcpmux/stcpmux.c \
    backend/mfabric/libnanomsg/src/transports/tcpmux/tcpmux.h \
    backend/mfabric/libnanomsg/src/transports/tcpmux/tcpmux.c

NANOMSG_TRANSPORTS = \
    $(TRANSPORTS_UTILS) \
    $(TRANSPORTS_INPROC) \
    $(TRANSPORTS_IPC) \
    $(TRANSPORTS_TCP) \
    $(TRANSPORTS_WS) \
    $(TRANSPORTS_TCPMUX)

libnanomsg_la_SOURCES = \
    backend/mfabric/libnanomsg/src/transport.h \
    backend/mfabric/libnanomsg/src/protocol.h \
    $(NANOMSG_CORE) \
    $(NANOMSG_AIO) \
    $(NANOMSG_UTILS) \
    $(NANOMSG_PROTOCOLS) \
    $(NANOMSG_TRANSPORTS) \
    $(NANOMSG_DEVICES)

DEFS =	-DNN_HAVE_GCC=1 \
	-DNN_HAVE_LINUX=1 -DHAVE_PTHREAD_PRIO_INHERIT=1 \
	-DSTDC_HEADERS=1 -DHAVE_SYS_TYPES_H=1 -DHAVE_SYS_STAT_H=1 \
	-DHAVE_STDLIB_H=1 -DHAVE_STRING_H=1 -DHAVE_MEMORY_H=1 \
	-DHAVE_STRINGS_H=1 -DHAVE_INTTYPES_H=1 -DHAVE_STDINT_H=1 \
	-DHAVE_UNISTD_H=1 -DHAVE_NETINET_IN_H=1 -DHAVE_NETDB_H=1 \
	-DHAVE_ARPA_INET_H=1 -DHAVE_UNISTD_H=1 -DHAVE_SYS_SOCKET_H=1 \
	-DHAVE_SYS_IOCTL_H=1 -DHAVE_STDINT_H=1 -DNN_HAVE_STDINT=1 \
	-DHAVE_EVENTFD=1 -DNN_HAVE_EVENTFD=1 -DHAVE_PIPE=1 \
	-DNN_HAVE_PIPE=1 -DHAVE_PIPE2=1 -DNN_HAVE_PIPE2=1 \
	-DNN_HAVE_CLOCK_MONOTONIC=1 -DHAVE_LIBRT=1 \
	-DHAVE_CLOCK_GETTIME=1 -DHAVE_POLL=1 -DNN_HAVE_POLL=1 \
	-DHAVE_EPOLL_CREATE=1 -DNN_USE_EPOLL=1 -DHAVE_GETIFADDRS=1 \
	-DNN_USE_IFADDRS=1 -DHAVE_ACCEPT4=1 -DNN_HAVE_ACCEPT4=1 \
	-DNN_HAVE_GETADDRINFO_A=1 -DNN_HAVE_SOCKETPAIR=1 \
	-DNN_HAVE_SEMAPHORE=1 -DNN_HAVE_GCC_ATOMIC_BUILTINS=1 \
	-DNN_HAVE_MSG_CONTROL=1 -DNN_USE_EVENTFD=1 -DHAVE_DLFCN_H=1 \
	-DLT_OBJDIR=\".libs/\" -D_GNU_SOURCE

libnanomsg_la_LDFLAGS = -no-undefined 
libnanomsg_la_CFLAGS = -DNN_EXPORTS $(DEFS)
