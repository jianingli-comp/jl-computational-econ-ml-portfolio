# FNETS Robustness Study: Model Performance Under Violation of Assumptions

Empirical evaluation of FNETS (Factor-Adjusted Network Estimation and Forecasting) under assumption violations, applied to 46 U.S. financial firms across a calm period (2004–2006) and the Great Financial Crisis (2007–2009).

Paper: *Robustness of FNETS: Model Performance Under Violation of Assumptions* — Jianing Li, University of Waterloo

---

## Repository Structure

```
jl-computational-econ-ml-portfolio/
├── README.md
├── paper/
│   └── JianingLi_21226895-FinalProject.pdf
├── code/
│   ├── JianingLi_FNETS_Test.Rmd        # Primary implementation (R)
│   └── JianingLi_FNET_Test.ipynb       # Python replication via rpy2
└── output/
    └── figures/
        ├── fig0_mean_logvol.pdf         # Cross-sectional mean log-volatility
        ├── fig1_femax_timeseries.pdf    # FE_max over time
        ├── fig2_edge_density.pdf        # Network edge density over time
        ├── fig3_boxplot_avg.pdf         # FE_avg boxplots by period & window
        └── fig4_boxplot_max.pdf         # FE_max boxplots by period & window
```

---

## Code Overview

### `JianingLi_FNETS_Test.Rmd` (R — primary)

The main implementation. Adapted from `real_forecast_submission.R` in Barigozzi, Cho & Owens (2024) with the following modifications:

- Removed FARM baseline method (not the focus of this paper)
- Removed Dantzig selector variant (`fl_fd`); robustness is done via rolling window length instead
- Removed factor-only forecast (`fl_fc`); only full FNETS forecast is assessed
- Fixed VAR lag at `d = 1` throughout (original paper uses `d = 1:5` for sensitivity)
- Uses restricted static factor model for common component forecasting only

**Pipeline:**
1. Load `blm.RData`, transpose to `x0` (46 × 3269), parse dates
2. Split into calm (`tt_calm`) and crisis (`tt_crisis`) index sets, both requiring `tt > n`
3. Run `check_assumptions()` on each regime: ADF stationarity test, Jarque-Bera, Anscombe-Glynn kurtosis test
4. Rolling-window FNETS loop over `tt.ind = c(tt_calm, tt_crisis)`:
   - Dynamic PCA via `fnets:::dyn.pca()` with Bartlett kernel bandwidth `floor(4*(n/log(n))^(1/3))`
   - Factor count selected by Hallin-Liska IC (`ic.op = 5`); fallback to `q = 1` if NA
   - Common component forecast via `fnets:::common.predict()` (restricted static factor model)
   - Idiosyncratic VAR(1) via `fnets:::var.lasso()` with fixed λ = `lambda.path[9]`
   - Sparsity diagnostics: edge density `sum(beta != 0) / p²`, max in-degree, estimated `q`
   - Forecast errors: `FE_avg` and `FE_max`
5. Robustness check: repeat with `n = 126`, `kern.bw126 = floor(4*(126/log(126))^(1/3))`
6. Results summary and Welch t-tests across regimes
7. ggplot2 figures saved as PDFs

**Runtime:** ~1h 30min for n=252, ~30–50min for n=126.

**Dependencies:** `fnets`, `glmnet`, `tseries`, `moments`, `ggplot2`

---

### `JianingLi_FNET_Test.ipynb` (Python — replication)

A Python replication of the R pipeline using `rpy2` to call `fnets` internals directly. Mirrors the R code structure with equivalent logic in NumPy/SciPy.

- Data loaded from `blm.RData` via `pyreadr`
- Assumption tests use `statsmodels.tsa.stattools.adfuller` and `scipy.stats`
- FNETS estimation loop calls R via `rpy2.robjects`, passing flattened arrays as strings
- Results extracted back to NumPy arrays; summary and t-tests run in Python
- Figures produced with `matplotlib`

**Note:** Requires `R_HOME` to be set manually (see top of notebook). R packages `fnets` and `glmnet` must be installed in the linked R environment.

**Dependencies:** `rpy2`, `pyreadr`, `numpy`, `pandas`, `scipy`, `statsmodels`, `matplotlib`

---

## Data

Source: Barigozzi, Cho & Owens (2024), retrieved from WRDS. The panel contains daily log-volatility measures for 46 U.S. financial firms (GICS classification), January 3, 2000 – December 31, 2012 (3,267 trading days). Volatility is computed as `log(0.361 * (p_high - p_low)²)`.

The raw data file `blm.RData` is included in this directory.

---

## Key Results

| | Calm (2004–2006) | Crisis (2007–2009) |
|---|---|---|
| ADF stationary | 100% | 4.3% |
| Mean kurtosis | 3.392 | 2.838 |
| Edge density (n=252) | 0.1008 | 0.1316 |
| FE_avg mean (n=252) | 0.5884 | 0.5529 |
| FE_max mean (n=252) | 0.8526 | 0.7443 |

FE_max difference is statistically significant across regimes (Welch t = 8.394, p < 0.001); FE_avg is not (p = 0.248).

---

## Reference

Barigozzi M, Cho H, Owens D (2024). FNETS: Factor-Adjusted Network Estimation and Forecasting for High-Dimensional Time Series. *Journal of Business & Economic Statistics*. https://doi.org/10.1080/07350015.2023.2257270
