## Monte Carlo Simulation: Model Misspecification and Omitted Variable Bias

This repository contains a Monte Carlo simulation study examining the
finite-sample properties of OLS estimators under model misspecification.
The focus is on omitted variable bias and how estimator performance
changes with the correlation structure among regressors.

The project is implemented in R using reproducible R Markdown, with all
figures and summary statistics generated programmatically.

### Project Overview

The simulation studies a linear data-generating process with two
regressors, comparing estimates from the correctly specified model and
an under-specified model that omits a relevant variable.

Key questions explored include:
- How omitted variable bias behaves in finite samples
- How estimator bias and MSE change as correlation between regressors increases
- How model misspecification affects inference even when sample size is moderate

### Simulation Design

- True model:
  $$y = \alpha + \beta x_1+\gamma x_2 + u$$

- Under-specified model:
  $$y = \alpha + \beta x_1 + e$$

Monte Carlo experiments are conducted under alternative correlation
levels between $x_1$ and $x_2$, with repeated simulations to evaluate:

- Mean estimates
- Bias
- Variance
- Mean squared error (MSE)

## Implementation

- Language: R
- Core methods: Monte Carlo simulation, OLS estimation
- Visualization: ggplot2
- All simulations are fully reproducible via the R Markdown file

## Files

- `JianingLi_R_Example_MonteCarlo.Rmd`  
  Reproducible R Markdown containing all simulation code, analysis, and figures.

- `JianingLi_R_Example_MonteCarlo.pdf`  
  Compiled report presenting results and interpretation.

## Notes

This project emphasizes methodological understanding of estimator
behavior under misspecification, rather than predictive performance.
It is intended as a research-oriented simulation study in applied
econometrics.
