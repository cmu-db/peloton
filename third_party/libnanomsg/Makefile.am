######################################################################
# LIBNANOMSG
######################################################################

LIBNANOMSG_SRCDIR = $(top_srcdir)/third_party/libnanomsg

nnincludedir = third_party/libnanomsg/

nninclude_HEADERS = \
    third_party/libnanomsg/src/nn.h \
    third_party/libnanomsg/src/inproc.h \
    third_party/libnanomsg/src/ipc.h \
    third_party/libnanomsg/src/tcp.h \
    third_party/libnanomsg/src/ws.h \
    third_party/libnanomsg/src/pair.h \
    third_party/libnanomsg/src/pubsub.h \
    third_party/libnanomsg/src/reqrep.h \
    third_party/libnanomsg/src/pipeline.h \
    third_party/libnanomsg/src/survey.h \
    third_party/libnanomsg/src/bus.h \
    third_party/libnanomsg/src/tcpmux.h

lib_LTLIBRARIES = libnanomsg.la

NANOMSG_CORE = \
    third_party/libnanomsg/src/core/ep.h \
    $(LIBNANOMSG_SRCDIR)/src/core/ep.c \
    third_party/libnanomsg/src/core/epbase.c \
    third_party/libnanomsg/src/core/global.h \
    third_party/libnanomsg/src/core/global.c \
    third_party/libnanomsg/src/core/pipe.c \
    third_party/libnanomsg/src/core/poll.c \
    third_party/libnanomsg/src/core/sock.h \
    third_party/libnanomsg/src/core/sock.c \
    third_party/libnanomsg/src/core/sockbase.c \
    third_party/libnanomsg/src/core/symbol.c

NANOMSG_DEVICES =\
    third_party/libnanomsg/src/devices/device.c \
    third_party/libnanomsg/src/devices/device.h \
    third_party/libnanomsg/src/devices/tcpmuxd.c

NANOMSG_AIO = \
    third_party/libnanomsg/src/aio/ctx.h \
    third_party/libnanomsg/src/aio/ctx.c \
    third_party/libnanomsg/src/aio/fsm.h \
    third_party/libnanomsg/src/aio/fsm.c \
    third_party/libnanomsg/src/aio/poller.h \
    third_party/libnanomsg/src/aio/poller.c \
    third_party/libnanomsg/src/aio/poller_epoll.h \
    third_party/libnanomsg/src/aio/poller_epoll.inc \
    third_party/libnanomsg/src/aio/poller_kqueue.h \
    third_party/libnanomsg/src/aio/poller_kqueue.inc \
    third_party/libnanomsg/src/aio/poller_poll.h \
    third_party/libnanomsg/src/aio/poller_poll.inc \
    third_party/libnanomsg/src/aio/pool.h \
    third_party/libnanomsg/src/aio/pool.c \
    third_party/libnanomsg/src/aio/timer.h \
    third_party/libnanomsg/src/aio/timer.c \
    third_party/libnanomsg/src/aio/timerset.h \
    third_party/libnanomsg/src/aio/timerset.c \
    third_party/libnanomsg/src/aio/usock.h \
    third_party/libnanomsg/src/aio/usock.c \
    third_party/libnanomsg/src/aio/usock_posix.h \
    third_party/libnanomsg/src/aio/usock_posix.inc \
    third_party/libnanomsg/src/aio/usock_win.h \
    third_party/libnanomsg/src/aio/usock_win.inc \
    third_party/libnanomsg/src/aio/worker.h \
    third_party/libnanomsg/src/aio/worker.c \
    third_party/libnanomsg/src/aio/worker_posix.h \
    third_party/libnanomsg/src/aio/worker_posix.inc \
    third_party/libnanomsg/src/aio/worker_win.h \
    third_party/libnanomsg/src/aio/worker_win.inc

NANOMSG_UTILS = \
    third_party/libnanomsg/src/utils/alloc.h \
    third_party/libnanomsg/src/utils/alloc.c \
    third_party/libnanomsg/src/utils/atomic.h \
    third_party/libnanomsg/src/utils/atomic.c \
    third_party/libnanomsg/src/utils/attr.h \
    third_party/libnanomsg/src/utils/chunk.h \
    third_party/libnanomsg/src/utils/chunk.c \
    third_party/libnanomsg/src/utils/chunkref.h \
    third_party/libnanomsg/src/utils/chunkref.c \
    third_party/libnanomsg/src/utils/clock.h \
    third_party/libnanomsg/src/utils/clock.c \
    third_party/libnanomsg/src/utils/closefd.h \
    third_party/libnanomsg/src/utils/closefd.c \
    third_party/libnanomsg/src/utils/cont.h \
    third_party/libnanomsg/src/utils/efd.h \
    third_party/libnanomsg/src/utils/efd.c \
    third_party/libnanomsg/src/utils/efd_eventfd.h \
    third_party/libnanomsg/src/utils/efd_eventfd.inc \
    third_party/libnanomsg/src/utils/efd_pipe.h \
    third_party/libnanomsg/src/utils/efd_pipe.inc \
    third_party/libnanomsg/src/utils/efd_socketpair.h \
    third_party/libnanomsg/src/utils/efd_socketpair.inc \
    third_party/libnanomsg/src/utils/efd_win.h \
    third_party/libnanomsg/src/utils/efd_win.inc \
    third_party/libnanomsg/src/utils/err.h \
    third_party/libnanomsg/src/utils/err.c \
    third_party/libnanomsg/src/utils/fast.h \
    third_party/libnanomsg/src/utils/fd.h \
    third_party/libnanomsg/src/utils/glock.h \
    third_party/libnanomsg/src/utils/glock.c \
    third_party/libnanomsg/src/utils/hash.h \
    third_party/libnanomsg/src/utils/hash.c \
    third_party/libnanomsg/src/utils/int.h \
    third_party/libnanomsg/src/utils/list.h \
    third_party/libnanomsg/src/utils/list.c \
    third_party/libnanomsg/src/utils/msg.h \
    third_party/libnanomsg/src/utils/msg.c \
    third_party/libnanomsg/src/utils/mutex.h \
    third_party/libnanomsg/src/utils/mutex.c \
    third_party/libnanomsg/src/utils/queue.h \
    third_party/libnanomsg/src/utils/queue.c \
    third_party/libnanomsg/src/utils/random.h \
    third_party/libnanomsg/src/utils/random.c \
    third_party/libnanomsg/src/utils/sem.h \
    third_party/libnanomsg/src/utils/sem.c \
    third_party/libnanomsg/src/utils/sleep.h \
    third_party/libnanomsg/src/utils/sleep.c \
    third_party/libnanomsg/src/utils/stopwatch.h \
    third_party/libnanomsg/src/utils/stopwatch.c \
    third_party/libnanomsg/src/utils/thread.h \
    third_party/libnanomsg/src/utils/thread.c \
    third_party/libnanomsg/src/utils/thread_posix.h \
    third_party/libnanomsg/src/utils/thread_posix.inc \
    third_party/libnanomsg/src/utils/thread_win.h \
    third_party/libnanomsg/src/utils/thread_win.inc \
    third_party/libnanomsg/src/utils/win.h \
    third_party/libnanomsg/src/utils/wire.h \
    third_party/libnanomsg/src/utils/wire.c

PROTOCOLS_UTILS = \
    third_party/libnanomsg/src/protocols/utils/dist.h \
    third_party/libnanomsg/src/protocols/utils/dist.c \
    third_party/libnanomsg/src/protocols/utils/excl.h \
    third_party/libnanomsg/src/protocols/utils/excl.c \
    third_party/libnanomsg/src/protocols/utils/fq.h \
    third_party/libnanomsg/src/protocols/utils/fq.c \
    third_party/libnanomsg/src/protocols/utils/lb.h \
    third_party/libnanomsg/src/protocols/utils/lb.c \
    third_party/libnanomsg/src/protocols/utils/priolist.h \
    third_party/libnanomsg/src/protocols/utils/priolist.c

PROTOCOLS_BUS = \
    third_party/libnanomsg/src/protocols/bus/bus.h \
    third_party/libnanomsg/src/protocols/bus/bus.c \
    third_party/libnanomsg/src/protocols/bus/xbus.h \
    third_party/libnanomsg/src/protocols/bus/xbus.c

PROTOCOLS_PIPELINE = \
    third_party/libnanomsg/src/protocols/pipeline/push.h \
    third_party/libnanomsg/src/protocols/pipeline/push.c \
    third_party/libnanomsg/src/protocols/pipeline/pull.h \
    third_party/libnanomsg/src/protocols/pipeline/pull.c \
    third_party/libnanomsg/src/protocols/pipeline/xpull.h \
    third_party/libnanomsg/src/protocols/pipeline/xpull.c \
    third_party/libnanomsg/src/protocols/pipeline/xpush.h \
    third_party/libnanomsg/src/protocols/pipeline/xpush.c

PROTOCOLS_PAIR = \
    third_party/libnanomsg/src/protocols/pair/pair.h \
    third_party/libnanomsg/src/protocols/pair/pair.c \
    third_party/libnanomsg/src/protocols/pair/xpair.h \
    third_party/libnanomsg/src/protocols/pair/xpair.c

PROTOCOLS_PUBSUB = \
    third_party/libnanomsg/src/protocols/pubsub/pub.h \
    third_party/libnanomsg/src/protocols/pubsub/pub.c \
    third_party/libnanomsg/src/protocols/pubsub/sub.h \
    third_party/libnanomsg/src/protocols/pubsub/sub.c \
    third_party/libnanomsg/src/protocols/pubsub/trie.h \
    third_party/libnanomsg/src/protocols/pubsub/trie.c \
    third_party/libnanomsg/src/protocols/pubsub/xpub.h \
    third_party/libnanomsg/src/protocols/pubsub/xpub.c \
    third_party/libnanomsg/src/protocols/pubsub/xsub.h \
    third_party/libnanomsg/src/protocols/pubsub/xsub.c

PROTOCOLS_REQREP = \
    third_party/libnanomsg/src/protocols/reqrep/req.h \
    third_party/libnanomsg/src/protocols/reqrep/req.c \
    third_party/libnanomsg/src/protocols/reqrep/rep.h \
    third_party/libnanomsg/src/protocols/reqrep/rep.c \
    third_party/libnanomsg/src/protocols/reqrep/task.h \
    third_party/libnanomsg/src/protocols/reqrep/task.c \
    third_party/libnanomsg/src/protocols/reqrep/xrep.h \
    third_party/libnanomsg/src/protocols/reqrep/xrep.c \
    third_party/libnanomsg/src/protocols/reqrep/xreq.h \
    third_party/libnanomsg/src/protocols/reqrep/xreq.c

PROTOCOLS_SURVEY = \
    third_party/libnanomsg/src/protocols/survey/respondent.h \
    third_party/libnanomsg/src/protocols/survey/respondent.c \
    third_party/libnanomsg/src/protocols/survey/surveyor.h \
    third_party/libnanomsg/src/protocols/survey/surveyor.c \
    third_party/libnanomsg/src/protocols/survey/xrespondent.h \
    third_party/libnanomsg/src/protocols/survey/xrespondent.c \
    third_party/libnanomsg/src/protocols/survey/xsurveyor.h \
    third_party/libnanomsg/src/protocols/survey/xsurveyor.c

NANOMSG_PROTOCOLS = \
    $(PROTOCOLS_BUS) \
    $(PROTOCOLS_PIPELINE) \
    $(PROTOCOLS_PAIR) \
    $(PROTOCOLS_PUBSUB) \
    $(PROTOCOLS_REQREP) \
    $(PROTOCOLS_SURVEY) \
    $(PROTOCOLS_UTILS)

TRANSPORTS_UTILS = \
    third_party/libnanomsg/src/transports/utils/backoff.h \
    third_party/libnanomsg/src/transports/utils/backoff.c \
    third_party/libnanomsg/src/transports/utils/dns.h \
    third_party/libnanomsg/src/transports/utils/dns.c \
    third_party/libnanomsg/src/transports/utils/dns_getaddrinfo.h \
    third_party/libnanomsg/src/transports/utils/dns_getaddrinfo.inc \
    third_party/libnanomsg/src/transports/utils/dns_getaddrinfo_a.h \
    third_party/libnanomsg/src/transports/utils/dns_getaddrinfo_a.inc \
    third_party/libnanomsg/src/transports/utils/iface.h \
    third_party/libnanomsg/src/transports/utils/iface.c \
    third_party/libnanomsg/src/transports/utils/literal.h \
    third_party/libnanomsg/src/transports/utils/literal.c \
    third_party/libnanomsg/src/transports/utils/port.h \
    third_party/libnanomsg/src/transports/utils/port.c \
    third_party/libnanomsg/src/transports/utils/streamhdr.h \
    third_party/libnanomsg/src/transports/utils/streamhdr.c \
    third_party/libnanomsg/src/transports/utils/base64.h \
    third_party/libnanomsg/src/transports/utils/base64.c

TRANSPORTS_INPROC = \
    third_party/libnanomsg/src/transports/inproc/binproc.h \
    third_party/libnanomsg/src/transports/inproc/binproc.c \
    third_party/libnanomsg/src/transports/inproc/cinproc.h \
    third_party/libnanomsg/src/transports/inproc/cinproc.c \
    third_party/libnanomsg/src/transports/inproc/inproc.h \
    third_party/libnanomsg/src/transports/inproc/inproc.c \
    third_party/libnanomsg/src/transports/inproc/ins.h \
    third_party/libnanomsg/src/transports/inproc/ins.c \
    third_party/libnanomsg/src/transports/inproc/msgqueue.h \
    third_party/libnanomsg/src/transports/inproc/msgqueue.c \
    third_party/libnanomsg/src/transports/inproc/sinproc.h \
    third_party/libnanomsg/src/transports/inproc/sinproc.c

TRANSPORTS_IPC = \
    third_party/libnanomsg/src/transports/ipc/aipc.h \
    third_party/libnanomsg/src/transports/ipc/aipc.c \
    third_party/libnanomsg/src/transports/ipc/bipc.h \
    third_party/libnanomsg/src/transports/ipc/bipc.c \
    third_party/libnanomsg/src/transports/ipc/cipc.h \
    third_party/libnanomsg/src/transports/ipc/cipc.c \
    third_party/libnanomsg/src/transports/ipc/ipc.h \
    third_party/libnanomsg/src/transports/ipc/ipc.c \
    third_party/libnanomsg/src/transports/ipc/sipc.h \
    third_party/libnanomsg/src/transports/ipc/sipc.c

TRANSPORTS_TCP = \
    third_party/libnanomsg/src/transports/tcp/atcp.h \
    third_party/libnanomsg/src/transports/tcp/atcp.c \
    third_party/libnanomsg/src/transports/tcp/btcp.h \
    third_party/libnanomsg/src/transports/tcp/btcp.c \
    third_party/libnanomsg/src/transports/tcp/ctcp.h \
    third_party/libnanomsg/src/transports/tcp/ctcp.c \
    third_party/libnanomsg/src/transports/tcp/stcp.h \
    third_party/libnanomsg/src/transports/tcp/stcp.c \
    third_party/libnanomsg/src/transports/tcp/tcp.h \
    third_party/libnanomsg/src/transports/tcp/tcp.c

TRANSPORTS_WS = \
    third_party/libnanomsg/src/transports/ws/aws.h \
    third_party/libnanomsg/src/transports/ws/aws.c \
    third_party/libnanomsg/src/transports/ws/bws.h \
    third_party/libnanomsg/src/transports/ws/bws.c \
    third_party/libnanomsg/src/transports/ws/cws.h \
    third_party/libnanomsg/src/transports/ws/cws.c \
    third_party/libnanomsg/src/transports/ws/sws.h \
    third_party/libnanomsg/src/transports/ws/sws.c \
    third_party/libnanomsg/src/transports/ws/ws.h \
    third_party/libnanomsg/src/transports/ws/ws.c \
    third_party/libnanomsg/src/transports/ws/ws_handshake.h \
    third_party/libnanomsg/src/transports/ws/ws_handshake.c \
    third_party/libnanomsg/src/transports/ws/sha1.h \
    third_party/libnanomsg/src/transports/ws/sha1.c

TRANSPORTS_TCPMUX = \
    third_party/libnanomsg/src/transports/tcpmux/atcpmux.h \
    third_party/libnanomsg/src/transports/tcpmux/atcpmux.c \
    third_party/libnanomsg/src/transports/tcpmux/btcpmux.h \
    third_party/libnanomsg/src/transports/tcpmux/btcpmux.c \
    third_party/libnanomsg/src/transports/tcpmux/ctcpmux.h \
    third_party/libnanomsg/src/transports/tcpmux/ctcpmux.c \
    third_party/libnanomsg/src/transports/tcpmux/stcpmux.h \
    third_party/libnanomsg/src/transports/tcpmux/stcpmux.c \
    third_party/libnanomsg/src/transports/tcpmux/tcpmux.h \
    third_party/libnanomsg/src/transports/tcpmux/tcpmux.c

NANOMSG_TRANSPORTS = \
    $(TRANSPORTS_UTILS) \
    $(TRANSPORTS_INPROC) \
    $(TRANSPORTS_IPC) \
    $(TRANSPORTS_TCP) \
    $(TRANSPORTS_WS) \
    $(TRANSPORTS_TCPMUX)

libnanomsg_la_SOURCES = \
    third_party/libnanomsg/src/transport.h \
    third_party/libnanomsg/src/protocol.h \
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
