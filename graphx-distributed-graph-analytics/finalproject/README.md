# CS 651 Final Project – GraphX Experiments

This is the final project for **CS 651: Data-Intensive Distributed Computing** (Winter 2025).

The goal of this project is **learning and exploring Apache Spark GraphX**, not designing new graph algorithms.  
I focus on understanding how common GraphX operators behave and how runtime changes with graph size and partition settings. I also experiment with a simple Pregel-based reachability algorithm.

---

## Project Struture

```text
cs451-f25/
├── data/
│   ├── followers.txt
│   ├── ba_10k_edges.csv
│   ├── ba_30k_edges.csv
│   └── ba_50k_edges.csv
│
├── src/
│   └── main/
│       └── scala/
│           └── ca/
│               └── uwaterloo/
│                   └── cs451/
│                       └── finalproject/
│                           ├── GraphXExample.scala
│                           ├── FinalProject.scala
│                           ├── Utils/
│                           │   ├── GraphLoader.scala
│                           │   └── Timer.scala
│                           ├── BAgenerate.ipynb
│                           └── README.md
│
└── target/
    └── assignments-1.0.jar
```


---

## Part 1: GraphXExample

`GraphXExample.scala` is a small program based on the Spark GraphX Programming Guide.

- Uses a very small example graph (`data/followers.txt`)
- Demonstrates basic GraphX operators:
  - `mapVertices`, `subgraph`, `joinVertices`
  - PageRank, Connected Components, Triangle Count
  - A simple Pregel reachability example
- Purpose: learn APIs and verify correctness on a small graph

### Run GraphXExample

```bash
spark-submit \
  --class ca.uwaterloo.cs451.finalproject.GraphXExample \
  target/assignments-1.0.jar
```

## Part 2: FinalProject

`FinalProject.scala` extends the example to larger Barabási–Albert (BA) graphs and measures runtime.
- Graph sizes: 10k / 30k / 50k edges (in data/)
- Uses PartitionStrategy.EdgePartition2D
- Measures runtime for:
  - PageRank
  - Connected Components
  - Triangle Count
  - Pregel reachability
- Supports command-line options (Scallop)

### Run FinalProject

```bash
spark-submit \
  --class ca.uwaterloo.cs451.finalproject.FinalProject \
  target/assignments-1.0.jar \
  --input ba_10k_edges.csv \
  --partitions 8
```
Optional arguments:
- `--source <id>` : source node for Pregel reachability
- `--find-dead` : run dead-node detection (small graphs only)

## BA Graph Generation
`BAgenerate.ipynb` was used to generate the BA graphs stored in `data/`.
It is not required for running the Spark jobs.

## Notes
- This project is learning-oriented and follows the Spark documentation closely.
- Real-world datasets were avoided to reduce data-cleaning overhead.
- Dead-node detection using Pregel is included conceptually but only tested on small graphs due to runtime cost.

Author: Jianing Li
CS 651 - Fall 2025
