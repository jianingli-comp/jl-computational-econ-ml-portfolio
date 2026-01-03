# Distributed Graph Analytics with Spark GraphX

This project implements and studies large-scale graph analytics using
Apache Spark GraphX, with an emphasis on **iterative computation,
partitioning strategies, and scalability behavior**.

The work was originally developed as a graduate-level final project
(CS651: Data-Intensive Distributed Computing, University of Waterloo)
and is presented here as a standalone portfolio project focused on
distributed systems reasoning rather than algorithm design.

---

## What this project demonstrates

- Practical use of **GraphX APIs** for real graph algorithms
- Understanding of **iterative, message-passing computation** at scale
- Experimental analysis of **runtime behavior as graph size grows**
- Awareness of **partitioning and communication costs** in Spark

This project prioritizes *system-level insight* over inventing new
graph algorithms.

---

## Algorithms implemented

- **PageRank**
- **Connected Components**
- **Triangle Count**
- **Basic GraphX operators** (`mapVertices`, `subgraph`, `joinVertices`)
- **Pregel-style reachability** (tested on small graphs)

---

## Experimental setup

- **Graphs**: Synthetic Barabási–Albert (BA) graphs  
  - 10k, 30k, and 50k edges
- **Platform**: Apache Spark GraphX (Scala)
- **Partitioning**: `PartitionStrategy.EdgePartition2D`
- **Metrics**:
  - Runtime
  - Convergence behavior
  - Scaling trends across graph sizes

Real-world datasets were intentionally avoided to keep the focus on
GraphX behavior rather than data cleaning overhead.

---

## Key observations

- Runtime grows non-linearly with graph size for iterative algorithms
  such as PageRank
- Communication-heavy algorithms exhibit different scaling behavior
  compared to aggregation-heavy ones (e.g., Triangle Count)
- Partitioning strategy significantly affects performance even for
  moderately sized graphs
- Pregel-based approaches are conceptually powerful but costly in
  practice for larger graphs

Detailed results and discussion are documented in the accompanying
project report.

---

## Experimental results

A concise summary of experimental findings is available in `results/`,
with full methodology, tables, and figures documented in the project
report (PDF).

---

## Repository structure
```text
graphx-distributed-graph-analytics/
├── finalproject/ # Scala implementations and coursework materials
├── data/ # Synthetic graph datasets
└── README.md # Project overview (this file)
```
- **`finalproject/`** contains the full Scala implementation, experiment
  code, and the original CS651 project report.
- **`data/`** contains synthetic BA graph datasets used for benchmarking.

---

## Implementation details

Full implementation notes, command-line options, and experiment
descriptions are available in:

```text
finalproject/README.md
finalproject/JianingLi_CS651_FinalProjectReport.pdf
```

These files document the original coursework submission and provide
additional technical depth.

---

## How to run (conceptual)

This project is designed to run in Spark local or cluster mode.

Example:
```bash
spark-submit \
  --class ca.uwaterloo.cs451.finalproject.FinalProject \
  target/assignments-1.0.jar \
  --input ba_10k_edges.csv \
  --partitions 8
```
Exact build and execution details are documented in **`finalproject/README.md`**

## Note

This project is learning- and analysis-oriented.
The goal is to understand how distributed graph algorithms behave in
practice, not to optimize a single benchmark or propose new algorithms.

---
