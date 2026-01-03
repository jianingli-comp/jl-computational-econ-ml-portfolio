### PageRank & Personalized PageRank (RDD-based)

This project implements scalable PageRank and multi-source personalized PageRank
using low-level Spark RDDs.

Key system challenges addressed:
- Handling dead-end nodes without quadratic message explosion
- Preserving partitioning to avoid unnecessary shuffles
- Iterative computation over billion-scale edges
- Memory-aware design using caching and controlled unpersisting

Tested on:
- Gnutella P2P network (~6K nodes)
- Wikipedia link graph (14M nodes, 117M edges)

Based on large-scale graph processing techniques discussed in distributed systems coursework. 

