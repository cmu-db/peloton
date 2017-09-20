#===------------------------------------------------====#
# nops.py                                               #
#                                                       #
# Simple script to evaluate the empty query (i.e. nop)  #
# performance of peloton.                               #
#                                                       #
# NOTE: THIS SCRIPT ONLY WORKS WITH PYTHON >=3.2        #
#                                                       #
#===-----------------------------------------------=====#

import socket
import time

current_milli_time = lambda: int(round(time.time() * 1000))

startup_packet = [
    # packet size
    (21).to_bytes(4, byteorder='big'),
    # protocol version
    (196608).to_bytes(4, byteorder='big'), 
    # user kv pair
    bytearray('user', 'ascii'), 
    bytearray('postgres', 'ascii'),
    # null terminator
    bytearray(1) 
]

nop_query_packet = [
    # query packet
    bytearray('Q', 'ascii'),
    # packet size
    (6).to_bytes(4, byteorder='big'),
    # query
    bytearray(';', 'ascii'),
    # null-terminator
    bytearray(1)
]

def to_byte_array(in_arr):
    byte_arr = bytearray()
    for in_byte_arr in in_arr:
        byte_arr += in_byte_arr
    return byte_arr

def nop_loop():
    i = 0
    num_req = 4*(10**6)
    start = current_milli_time()
    # send a million request
    for i in range(num_req):
        s.send(to_byte_array(nop_query_packet))
        s.recv(2048)
    end = current_milli_time()
    print("%.2f nops/s" % (num_req*(10**3)/(end-start)))

if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 57721))
    s.send(to_byte_array(startup_packet))
    # pull response and ignore
    s.recv(2048)
    nop_loop()
    
