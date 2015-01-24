High Performance Network library
===

# Purpose
The Netty library is a benchmark for high performance libraries. 
    Netty is designed for scalability in particular, horizontal scalability is
    based on the Selector and it has highly tuned this facility.
   
This library is designed to be lower latency and support higher throughputs 
    by employing techniques used in low latency trading systems.
    
# Transports
Network current support TCP only. 
 
Planned support for
* Shared Memory
* Unreliable UDP

# Support
This library will require Java 8
   
# Simplicity
The library is a cut down version of the functionality Netty provides, 
    so it needs to be simpler to reflect this.
   
# Testing
The target environment is to support TCP over 10 Gig-E ethernet.  In prototype
    testing, this library has half the latency and support 30% more bandwidth.
    
A key test is that it shouldn't GC more than once (to allow for warm up) with -mx64m

# Downsides
This comes at the cost of scalability for large number os connections.
     In this situation, this library should perform at least as well as netty.

# Comparisons

## Netty
Netty has a much wider range of functionality, however it creates some 
   garbage in it's operation (less than using plain NIO Selectors) and isn't 
   designed to support busy waiting which gives up a small but significant delay.
    
