# Distributed Graph Analytics with Spark GraphX

This project implements and studies large-scale graph algorithms using
Apache Spark GraphX, with a focus on iterative computation, partitioning,
and scalability.

The work was originally developed as a graduate-level final project (CS651)
and has been adapted here as a standalone portfolio project.

## Algorithms implemented
- PageRank (basic and personalized variants)
- Connected Components
- Triangle Count
- Graph preprocessing and loading utilities

## Experimental setup
- Graphs: synthetic Barabási–Albert graphs (10k / 30k / 50k edges)
- Platform: Spark GraphX (Scala)
- Metrics: runtime, convergence behavior, scalability trends

## Project structure
```text
graphx-distributed-graph-analytics/
├── finalproject/ # Scala implementations and experiment code
├── data/ # Synthetic graphs and sample datasets
└── README.md # Project overview (this file)
```

## Key results & observations
- PageRank runtime is sensitive to graph size and partitioning
- Iterative message-passing algorithms exhibit non-linear scaling
- Graph structure (degree distribution) strongly affects convergence

Detailed experimental discussion and results can be found in:
- `finalproject/JianingLi_CS651_FinalProjectReport.pdf`

## How to run (local example)
This project is designed for Spark local or cluster mode.

Example (conceptual):
```bash
spark-submit --class FinalProject target/scala-2.xx/your-jar.jar
```
