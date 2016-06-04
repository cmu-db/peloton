//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.h
//
// Identification: src/wire/marshal.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_MARSHALL_H
#define PELOTON_MARSHALL_H

#include <vector>
#include <string>
#include "socket_base.h"
#include "wire.h"
#include "logger.h"
#include "assert.h"

namespace peloton {
namespace wire {

typedef unsigned char uchar;

/*
 * Marshallers
 */

/* packet_put_byte - used to write a single byte into a packet */
extern void packet_put_byte(std::unique_ptr<Packet> &pkt, const uchar c);

/* packet_put_string - used to write a string into a packet */
extern void packet_put_string(std::unique_ptr<Packet> &pkt, const std::string &str);

/* packet_put_int - used to write a single int into a packet */
extern void packet_put_int(std::unique_ptr<Packet> &pkt, int n, int base);

/* packet_put_cbytes - used to write a uchar* into a packet */
extern void packet_put_cbytes(std::unique_ptr<Packet> &pkt, const uchar *b,
                             int len);

/* packet_put_bytes - used to write a uchar vector into a packet */
extern void packet_put_bytes(std::unique_ptr<Packet> &pkt,
														 const std::vector<uchar>& data);

/*
 * Unmarshallers
 */

/*
 * packet_get_int -  Parse an int out of the head of the
 * 	packet. "base" bytes determine the number of bytes of integer
 * 	we are parsing out.
 */
extern int packet_get_int(Packet *pkt, uchar base);

/*
 * packet_get_string - parse out a string of size len.
 * 		if len=0? parse till the end of the string
 */
extern void packet_get_string(Packet *pkt, size_t len, std::string& result);

/* packet_get_bytes - Parse out "len" bytes of pkt as raw bytes */
extern void packet_get_bytes(Packet *pkt, size_t len, PktBuf& result);

/*
 * get_string_token - used to extract a string token
 * 		from an unsigned char vector
 */
extern void get_string_token(Packet *pkt, std::string& result);

/*
 * Socket layer interface - Link the protocol to the socket buffers
 */

/* Write a batch of packets to the socket write buffer */
extern bool write_packets(std::vector<std::unique_ptr<Packet>> &packets,
                          Client *client);

/* Read a single packet from the socket read buffer */
extern bool read_packet(Packet *pkt, bool has_type_field, Client *client);
}
}
#endif  // PELOTON_MARSHALL_H
