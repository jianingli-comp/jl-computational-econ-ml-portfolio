# Computational Projects Portfolio

This repository presents my technical work at the intersection of
- distributed systems
- large-scale data processing
- machine learning
- computational economics.

My primary focus is on understanding how algorithms behave at scale:
how data is partitioned, shuffled, and aggregated across machines,
and how system-level design choices affect performance, scalability,
and modeling outcomes.

The projects here are developed through graduate-level coursework and
independent exploration, and are intended to support research-oriented
Co-op, Research Assistant, and PhD applications.

---

## Featured Project

### Distributed Graph Analytics with Spark GraphX
Graduate-level distributed systems project analyzing PageRank,
Connected Components, and Triangle Count using Apache Spark GraphX.

- Focus: iterative computation, partitioning strategies, and scalability
- Graphs: synthetic Barabási–Albert networks (10k–50k edges)
- Methods: GraphX operators, Pregel-style message passing
- Evidence: full experimental report (PDF)

-> **`graphx-distributed-graph-analytics/`**

This project serves as a deep, research-style investigation into distributed
graph processing and complements the lower-level system implementations
described below.

### Distributed Systems with Spark

A collection of projects implemented using low-level Spark primitives (RDDs),
designed to make execution behavior explicit rather than abstracted away.

These projects cover the three dominant paradigms of large-scale data processing:

- Graph processing: Iterative algorithms such as PageRank implemented directly on RDDs,
with explicit handling of dead-end nodes and shuffle behavior.
- Maching learning: Distributed spam classification using stochastic gradient descent,
focusing on order dependence, data shuffling, model aggregation,
and broadcast-based inference.
- Relational analytics: Manual implementation of SQL-style analytical queries (TPC-H),
including reduce-side joins, broadcast hash joins, and group-by aggregation,
without using Spark SQL or DataFrame APIs.

-> **`distributed-systems/`**

Together, these projects form a system-level capability matrix
that complements the GraphX flagship study.

---

## Project Structure

- distributed-systems/
- graphx-distributed-graph-analytics/

Each directory contains runnable code, experimental notes, and
small-scale prototypes developed during coursework and independent study.

---

## Data and Reproducibility

Large datasets used in these projects (e.g., graph data, spam classification data,
TPC-H benchmarks) are not included in this public repository.

This design reflects:
- large data volumes (GB-scale)
- course-provided or cluster-resident datasets
- a clean separation between public code and private data

All programs are designed to accept dataset paths via command-line arguments
and were tested in both local and cluster environments.

---

## Technologies
- **Languages**: Scala, Python, SQL
- **Frameworks**: Apache Spark, Spark GraphX
- **Concepts**: distributed systems, graph analytics, machine learning,
large-scale data processing, computational economics

---

## Contact

Email: **jianing.li.comp.econ@outlook.com / jianing.li@uwaterloo.ca**

GitHub: **jianingli-comp**

---

Additional projects in machine learning, computational economics,
and text analysis may be added as this portfolio evolves.

---

MIT License © 2025
