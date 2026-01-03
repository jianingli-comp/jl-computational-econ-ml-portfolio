## PageRank & Personalized PageRank (RDD-based)

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

### Data
- Small graphs: Gnutella P2P network
- Large graphs: Wikipedia link graph (14M nodes)

Data is not included in this repository.
Paths are provided at runtime via --input.

Based on large-scale graph processing techniques discussed in distributed systems coursework. 

