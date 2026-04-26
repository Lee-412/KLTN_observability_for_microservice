# V11_IMPLEMENT_REPORT

## 1) Why V11
- Guard term in V10 could dominate RCA discriminative signal at low budget.
- Confidence values were not comparable across legacy outputs and proxy outputs due scale mismatch.
- V11 adds RCA-aware relevance to align objective with A@1 optimization.

## 2) Objective
- V11 score: lambda_base * base + mu_evidence * evidence + lambda_rca * rca_relevance.
- Guard is annealed after quota deficits approach zero.
- Evidence uses log1p scaling instead of sigmoid compression to preserve score dynamics.

## 3) Tests
- Status: PASS (7/7)

## 4) RCA Compare
budget_pct,method,A1,A3,MRR,top1_confidence_proxy,top1_gap_proxy,confidence_scale_note
0.1,microrank_baseline,0.233660,0.464869,0.410463,,,legacy_unscaled
0.1,v9_metric_aware,0.222018,0.423203,0.396181,,,legacy_unscaled
0.1,v10_guard_aware_proxy,0.167688,0.439338,0.373358,0.12801395966436555,0.0017839913355975553,proxy_ratio
0.1,v11_rca_aware,0.210580,0.377042,0.371317,0.1663853430481276,2.0147888756605656,proxy_ratio
0.1,paper_trastrainer,0.425900,0.777400,0.550900,,,paper_reported
1.0,microrank_baseline,0.261234,0.564134,0.444330,,,legacy_unscaled
1.0,v9_metric_aware,0.161765,0.408088,0.339710,,,legacy_unscaled
1.0,v10_guard_aware_proxy,0.142157,0.380310,0.327976,0.12284330885171325,0.009442325936275685,proxy_ratio
1.0,v11_rca_aware,0.196487,0.361928,0.352412,0.17505976926389621,2.9411382269480817,proxy_ratio
1.0,paper_trastrainer,0.451600,0.785200,0.588900,,,paper_reported
2.5,microrank_baseline,0.185049,0.532884,0.398845,,,legacy_unscaled
2.5,v9_metric_aware,0.116830,0.382761,0.304711,,,legacy_unscaled
2.5,v10_guard_aware_proxy,0.117851,0.328227,0.293836,0.13126124819890897,0.7011426039485364,proxy_ratio
2.5,v11_rca_aware,0.210376,0.366422,0.363430,0.1884636581569861,4.383122344833565,proxy_ratio
2.5,paper_trastrainer,0.500000,0.822600,0.655600,,,paper_reported

## 5) Ablation
variant,budget_pct,A1,A3,MRR,jaccard,kendall_tau,swap_loss,score_std
V11-A,0.1,0.127042,0.331699,0.290345,1.000000,1.000000,0.000000,0.155151
V11-A,1.0,0.107435,0.357026,0.279995,1.000000,1.000000,0.000000,0.155151
V11-A,2.5,0.107435,0.382353,0.309329,1.000000,1.000000,0.000000,0.155151
V11-B,0.1,0.097222,0.261438,0.273632,1.000000,1.000000,0.000000,0.217924
V11-B,1.0,0.210376,0.404616,0.365941,1.000000,1.000000,0.000000,0.217924
V11-B,2.5,0.199959,0.392974,0.356940,1.000000,1.000000,0.000000,0.217924
V11-C,0.1,0.068219,0.320261,0.257406,1.000000,1.000000,0.000000,0.127303
V11-C,1.0,0.186070,0.348039,0.345803,1.000000,1.000000,0.000000,0.127303
V11-C,2.5,0.180351,0.376838,0.344258,1.000000,1.000000,0.000000,0.127303
V11-D,0.1,0.210580,0.377042,0.371317,0.921062,0.902809,0.075288,0.136395
V11-D,1.0,0.196487,0.361928,0.352412,0.996663,0.996312,0.048858,0.136395
V11-D,2.5,0.210376,0.366422,0.363430,0.999401,0.999281,0.014593,0.136395

## 6) Score-Scale Hypothesis
- budget 0.1: std_ratio_v11_over_v10=1.392823, A1_delta_v11_minus_v10=0.042892
- budget 1.0: std_ratio_v11_over_v10=1.392823, A1_delta_v11_minus_v10=0.054330
- budget 2.5: std_ratio_v11_over_v10=1.392823, A1_delta_v11_minus_v10=0.092525