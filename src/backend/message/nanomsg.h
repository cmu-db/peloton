//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nanomsg.h
//
// Identification: src/backend/message/nanomsg.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/message/abstract_message.h"

#include "nanomsg/src/nn.h"
#include "nanomsg/src/reqrep.h"
#include "nanomsg/src/pipeline.h"
#include "nanomsg/src/pair.h"
#include "nanomsg/src/pubsub.h"
#include <cassert>
#include <exception>

#if defined __GNUC__
#define nn_slow(x) __builtin_expect ((x), 0)
#else
#define nn_slow(x) (x)
#endif

namespace peloton {
namespace message {

class exception : public std::exception {
public:

	exception() :
			err(nn_errno()) {
	}

	virtual const char *what() const throw () {
		return nn_strerror(err);
	}

	int num() const {
		return err;
	}

private:

	int err;
};

inline const char *symbol (int i, int *value) {

    return nn_symbol (i, value);
}

inline void *allocmsg (size_t size, int type) {

    void *msg = nn_allocmsg (size, type);

    if (nn_slow (!msg))
        throw peloton::message::exception ();

    return msg;
}

inline int freemsg (void *msg) {

    int rc = nn_freemsg (msg);

    if (nn_slow (rc != 0))
        throw peloton::message::exception ();

    return rc;
}

inline void term () {

    nn_term ();
}

class NanoMsg : public AbstractMessage {

public:

  NanoMsg(int domain, int protocol) {

    socket_ = nn_socket(domain, protocol);
    if (nn_slow (socket_ < 0))
      throw peloton::message::exception();
  }
  ~NanoMsg() {
    int rc = nn_close(socket_);
    assert(rc == 0);
  }

  /** @brief BindSocket function nanomsg implementation */
  int Bind(const char *address) {

    int rc = nn_bind(socket_, address);

    if (nn_slow (rc < 0))
      throw peloton::message::exception();

      return rc;
  }

  /** @brief SetSocketOpt function nanomsg implementation */
  int SetSocketOpt(int level, int option, const void *opt_val,
            size_t opt_val_len) {

    int rc = nn_setsockopt(socket_, level, option, opt_val, opt_val_len);
    if (nn_slow (rc != 0))
      throw peloton::message::exception();
  }

  /** @brief GetSocketOpt function nanomsg implementation */
  int GetSocketOpt(int level, int option, void *opt_val,
        size_t *opt_val_len) {

    int rc = nn_getsockopt(socket_, level, option, opt_val, opt_val_len);

    if (nn_slow (rc != 0))
      throw peloton::message::exception();
  }

  /** @brief Connect function nanomsg implementation */
  int Connect(const char *address) {

    int rc = nn_connect(socket_, address);

    if (nn_slow (rc < 0))
      throw peloton::message::exception();

    return rc;
  }

  /** @brief Send function nanomsg implementation */
  int Send(const void *buffer, size_t length, int flags) {

    int rc = nn_send(socket_, buffer, length, flags);

    if (nn_slow (rc < 0)) {

        if (nn_slow (nn_errno () != EAGAIN))
          throw peloton::message::exception();
		return -1;
    }

    return rc;
  }

  /** @brief Receive function nanomsg implementation */
  int Receive(void *buffer, size_t length, int flags) {

    int rc = nn_recv(socket_, buffer, length, flags);

    if (nn_slow (rc < 0)) {

      if (nn_slow (nn_errno () != EAGAIN))
        throw peloton::message::exception();

      return -1;
    }

    return rc;
  }

  /** @brief Shutdown function nanomsg implementation */
  void Shutdown(int how) {

    int rc = nn_shutdown(socket_, how);

    if (nn_slow (rc != 0))
      throw peloton::message::exception ();
  }

private:

  int socket_;

};

}  // end of message namespace
}  // end of peloton namespace
