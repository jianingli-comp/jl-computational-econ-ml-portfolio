# Experimental Results

This page summarizes the main experimental findings reported in
`JianingLi_CS651_FinalProjectReport.pdf`.

The goal of these experiments is not benchmarking for peak performance,
but understanding **how Spark GraphX operators behave in practice**
under different graph sizes and partition settings.

---

## Experimental setup (summary)

- Graphs: synthetic Barabási–Albert (BA) networks  
  - 10k, 30k, 50k edges
- Platform: Apache Spark GraphX (Scala)
- Operators tested:
  - PageRank
  - Connected Components
  - Triangle Count
  - Pregel-based reachability
- Partition strategy:
  - `PartitionStrategy.EdgePartition2D`
- Timing:
  - Wall-clock execution time (single run per configuration)

---

## Result 1: Relative cost of GraphX operators

Across all graph sizes, **PageRank is consistently the most expensive
operation**, followed by Triangle Count.

- PageRank: ~4.1s → ~5.6s (10k → 50k edges)
- Triangle Count: ~1.0s → ~2.25s
- Connected Components: ~1.0s → ~1.5s
- Pregel reachability: ~0.75s → ~0.95s

This ordering is stable across configurations and reflects the
communication-heavy nature of iterative algorithms like PageRank.

(See Table 1 in the project report.)

---

## Result 2: Effect of graph size

Increasing graph size increases runtime for all operators, but **growth
is moderate rather than linear**, even when edge count increases by 5×.

Example:
- PageRank increases by ~36% from 10k to 50k edges
- Connected Components remains close to 1 second even for the largest graph

This suggests that GraphX handles **sparse BA graphs efficiently** at
this scale, and that graph size alone is not the dominant bottleneck.

---

## Result 3: Effect of partition count

Increasing the number of partitions does **not** always improve
performance.

Observed pattern:
- Moving from 8 → 16 partitions yields minimal change
- Moving from 16 → 32 partitions often *increases* runtime

This effect is most visible for PageRank and Pregel reachability, where
additional partitions introduce communication and coordination overhead
that outweigh parallelism gains.

These observations align with Spark documentation: more partitions are
not always better, especially for small or sparse graphs.

(See Table 2 in the project report.)

---

## Result 4: Pregel reachability and scalability limits

Pregel-based reachability propagates quickly through BA graphs, and
almost all vertices are reachable from a single source node, confirming
the presence of a giant connected component.

However, extending this approach to dead-node detection by running
Pregel once per vertex is computationally infeasible.

Estimated cost:
- 10k graph: ~2 hours
- 30k graph: ~7 hours
- 50k graph: ~14 hours

As a result, this design was evaluated conceptually and tested only on
small graphs.

(See Table 3 in the project report.)

---

## Key takeaways

- Iterative, message-passing algorithms are the dominant cost drivers in
  GraphX
- Partitioning introduces real trade-offs; more parallelism can hurt
  performance
- Small example graphs are essential for correctness and understanding,
  while larger synthetic graphs are better for performance observation
- Naive extensions of Pregel-style algorithms do not scale and require
  redesign to be practical

---

## Full report

For full methodology, tables, and figures, see:

`finalproject/JianingLi_CS651_FinalProjectReport.pdf`
