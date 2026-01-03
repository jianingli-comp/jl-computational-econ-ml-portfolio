## Distributed SQL Analytics over TPC-H (RDD-based)

This project manually implements a subset of TPC-H analytical queries
using low-level Spark RDD transformations.

Key system concepts demonstrated:
- Reduce-side joins via cogroup
- Broadcast hash joins for small dimension tables
- Join ordering and memory-aware planning
- Aggregation and grouping without Spark SQL
- Performance comparison across text vs Parquet formats

Queries are based on the TPC-H benchmark.
