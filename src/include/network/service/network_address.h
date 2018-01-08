//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_address.h
//
// Identification: src/include/network/service/network_address.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <netinet/in.h>
#include <stdint.h>
#include <string>
#include <vector>

namespace peloton {
namespace network {
namespace service {

class NetworkAddress {
 public:
  NetworkAddress() : ip_address_(0), port_(0) {}
  NetworkAddress(const NetworkAddress &addr);
  NetworkAddress(const sockaddr_in &addrin);
  NetworkAddress(const sockaddr &addr);
  NetworkAddress(const std::string &address);

  // Returns true if the address is parsed successfully.
  bool Parse(const std::string &address);

  bool operator==(const NetworkAddress &other) const {
    return ip_address_ == other.ip_address_ && port_ == other.port_;
  }

  bool operator==(const sockaddr_in &other) const;

  bool operator<(const NetworkAddress &other) const {
    if (ip_address_ == other.ip_address_) {
      return port_ < other.port_;
    }
    return ip_address_ < other.ip_address_;
  }

  /** Returns IP:port as a string. */
  std::string ToString() const;

  /** Returns the IP address formatted as a string. */
  std::string IpToString() const;

  // Fills the addr structure with this address.
  void FillAddr(struct sockaddr_in *addr) const;

  // Returns a sockaddr_in for this address. ::fill() can be more efficient.
  struct sockaddr_in Sockaddr() const;

  // Returns the port in host byte order.
  uint16_t GetPort() const;

  // Set the port to new_port in host byte order.
  void SetPort(uint16_t new_port);

 private:
  // IPv4 address in network byte order
  uint32_t ip_address_;

  // Port in network byte order.
  uint16_t port_;
};

// Make operator== for sockaddr_in bidirectional
inline bool operator==(const sockaddr_in &a, const NetworkAddress &b) {
  return b == a;
}

// Returns a pointer to the raw array in a string.
// NOTE: const_cast<char*>(s->data()) fails due to reference counting
// implementations. See test.
inline char *stringArray(std::string *s) {
  if (s->empty()) return NULL;
  return &(*s->begin());
}

}  // namespace service
}  // namespace network
}  // namespace peloton
