//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcp_address.h
//
// Identification: src/backend/networking/tcp_address.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <vector>
#include <string>
#include <netinet/in.h>

namespace peloton {
namespace networking {

class NetworkAddress {
 public:
  NetworkAddress() : ip_address_(0), port_(0) {}
  NetworkAddress(const NetworkAddress& addr);
  NetworkAddress(const sockaddr_in& addrin);
  NetworkAddress(const sockaddr& addr);
  NetworkAddress(const std::string& address);

  // Returns true if the address is parsed successfully.
  bool Parse(const std::string& address);

  bool operator==(const NetworkAddress& other) const {
    return ip_address_ == other.ip_address_ && port_ == other.port_;
  }

  bool operator==(const sockaddr_in& other) const;

  bool operator<(const NetworkAddress& other) const {
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
  void FillAddr(struct sockaddr_in* addr) const;

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
inline bool operator==(const sockaddr_in& a, const NetworkAddress& b) {
  return b == a;
}

inline std::vector<std::string> splitExcluding(const std::string& input,
                                               char split) {
  std::vector<std::string> splits;

  size_t last = 0;
  while (last <= input.size()) {
    size_t next = input.find(split, last);
    if (next == std::string::npos) {
      next = input.size();
    }

    // Push the substring [last, next) on to splits
    splits.push_back(input.substr(last, next - last));
    last = next + 1;
  }

  return splits;
}

// Returns a pointer to the raw array in a string.
// NOTE: const_cast<char*>(s->data()) fails due to reference counting
// implementations. See test.
inline char* stringArray(std::string* s) {
  if (s->empty()) return NULL;
  return &(*s->begin());
}

}  // namespace networking
}  // namespace peloton
