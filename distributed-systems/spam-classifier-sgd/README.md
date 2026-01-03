### Distributed Spam Classification with SGD

This project implements a large-scale spam classifier trained via
stochastic gradient descent in Spark.

System-level focus:
- Distributed training via single-reducer aggregation
- Order dependence and data shuffling effects in SGD
- Model distribution using broadcast variables
- Ensemble inference vs single-model training trade-offs

Model:
- Logistic regression with hashed n-gram features
- Sparse weight vector trained over hundreds of millions of tokens

Result:
- Shuffling the training data stabilizes ROCA across runs, confirming order sensitivity in SGD. 
