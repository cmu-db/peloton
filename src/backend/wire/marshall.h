//
// Created by Siddharth Santurkar on 4/4/16.
//
//	marshall.h - Helpers to marshal and unmarshal packets in Pelotonwire
//
#ifndef PELOTON_MARSHALL_H
#define PELOTON_MARSHALL_H

#include <vector>
#include <string>
#include "socket_base.h"
#include "wire.h"

namespace peloton {
namespace wire {

typedef unsigned char uchar;

/*
 * Marshallers
 */
extern void packet_putbyte(std::unique_ptr<Packet> &pkt, const uchar c);

extern void packet_putstring(std::unique_ptr<Packet> &pkt, const std::string &str);

extern void packet_putint(std::unique_ptr<Packet> &pkt, int n, int base);

extern void packet_putcbytes(std::unique_ptr<Packet> &pkt, const uchar *b,
                             int len);

extern void packet_putbytes(std::unique_ptr<Packet> &pkt, const std::vector<uchar>& data);

/*
 * Unmarshallers
 */
extern int packet_getint(Packet *pkt, uchar base);

extern void packet_getstring(Packet *pkt, size_t len, std::string& result);

extern void packet_getbytes(Packet *pkt, size_t len, PktBuf& result);

extern void get_string_token(Packet *pkt, std::string& result);

/*
 * Socket layer interface
 */
extern bool write_packets(std::vector<std::unique_ptr<Packet>> &packets,
                          Client *client);

extern bool read_packet(Packet *pkt, bool has_type_field, Client *client);
}
}
#endif  // PELOTON_MARSHALL_H
