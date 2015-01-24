High Performance Network library
===

# Purpose
The Netty library is a benchmark for high performance libraries. 
    Netty is designed for scalability in particular, horizontal scalability is
    based on the Selector and it has highly tuned this facility.
   
This library is designed to be lower latency and support higher throughputs 
    by employing techniques used in low latency trading systems.
    
# Support
This library will require Java 8
   
# Simplicity
The library is a cut down version of the functionality Netty provides, 
    so it needs to be simpler to reflect this.
   
# Testing
The target environment is to support TCP over 10 Gig-E ethernet.  In prototype
    testing, this library has half the latency and support 30% more bandwidth.

# Downsides
This comes at the cost of scalability for large number os connections.
     In this situation, this library should perform at least as well as netty.

