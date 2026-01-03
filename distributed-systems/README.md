# Distributed Systems Projects (Spark / Large-Scale Data Processing)

This directory contains a collection of distributed systems projects
that demonstrate how different large-scale data paradigms are implemented
using low-level Spark primitives.

Rather than relying on high-level abstractions, these projects focus on
execution behavior, memory management, and data movement in distributed
environments.

Together, they cover the three dominant paradigms of large-scale data
processing:

- Graph analytics (iterative computation over massive graphs)
- Machine learning (distributed optimization and model aggregation)
- Relational analytics (join-heavy SQL-style workloads)

All projects were developed and tested in both local and cluster settings.


## Projects

### 1. PageRank & Personalized PageRank (`pagerank/`)

Implements PageRank and multi-source personalized PageRank using Spark RDDs.

**Key system concepts:**
- Iterative graph algorithms at scale
- Handling dead-end nodes without quadratic message explosion
- Preserving partitioning to avoid unnecessary shuffles
- Memory-aware design with caching and controlled unpersisting

The implementation was validated on both small graphs (e.g., P2P networks)
and large real-world graphs with tens of millions of nodes.


### 2. Distributed Spam Classification with SGD (`spam-classifier-sgd/`)

Implements a large-scale spam classifier trained using stochastic gradient
descent in Spark.

**Key system concepts:**
- Distributed training via single-reducer aggregation
- Order dependence in SGD and the effect of data shuffling
- Sparse model representation for high-dimensional feature spaces
- Model distribution and inference using broadcast variables
- Ensemble inference versus single-model training trade-offs

The project emphasizes system-level design choices rather than
application-specific modeling.


### 3. Distributed SQL Analytics over TPC-H (`distributed-sql/`)

Manually implements analytical SQL-style queries using Spark RDDs.

**Key system concepts:**
- Reduce-side joins via `cogroup`
- Broadcast hash joins for small dimension tables
- Join ordering and memory-aware planning
- Group-by aggregation without Spark SQL or DataFrame APIs
- Performance comparison across different data formats

The implementation mirrors physical execution plans typically generated
by SQL engines, making execution behavior explicit.


## Data and Reproducibility

Datasets used in these projects (e.g., graph data, spam classification data,
and TPC-H benchmark data) are **not included** in this public repository.

Reasons include:
- Large data volume (GB–scale)
- Course-provided or cluster-resident datasets
- Separation of public code and private data for clean data governance

All programs accept input paths via command-line arguments and were designed
to run with externally provided datasets in both local and cluster
environments.


## Running the Code

Each subproject contains detailed instructions for execution.
In general, programs are run using `spark-submit`, with dataset paths and
execution parameters provided at runtime.

Refer to individual project directories for concrete examples and
recommended configurations.
