//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tcp_address.h
//
// Identification: /peloton/src/backend/message/tcp_address.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once


#include <stdint.h>

#include <string>


namespace peloton {
namespace message {

struct sockaddr_in;

class NetworkAddress {
public:
    NetworkAddress() : ip_address_(0), port_(0) {}

    // Returns true if the address is parsed successfully.
    bool parse(const std::string& address);

    bool operator==(const NetworkAddress& other) const {
        return ip_address_ == other.ip_address_ && port_ == other.port_;
    }

    bool operator==(const sockaddr_in& other) const;

    /** Returns IP:port as a string. */
    std::string toString() const;

    /** Returns the IP address formatted as a string. */
    std::string ipToString() const;

    // Fills the addr structure with this address.
    void fill(struct sockaddr_in* addr) const;

    // Returns a sockaddr_in for this address. ::fill() can be more efficient.
    struct sockaddr_in sockaddr() const;

    // Returns the port in host byte order.
    uint16_t port() const;

    // Set the port to new_port in host byte order.
    void set_port(uint16_t new_port);

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

inline vector<string> splitExcluding(const std::string& input, char split) {
    std::vector<string> splits;

    size_t last = 0;
    while (last <= input.size()) {
        size_t next = input.find(split, last);
        if (next == string::npos) {
            next = input.size();
        }

        // Push the substring [last, next) on to splits
        splits.push_back(input.substr(last, next - last));
        last = next+1;
    }

    return splits;
}

}  // namespace message
}  // namespace peloton
