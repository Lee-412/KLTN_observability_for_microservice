# V10_IMPLEMENT_REPORT

## 1) Objective
Implement V10 sampler end-to-end with guard-aware marginal selection, RCA proxy evaluation, ablation, coherence telemetry, and unit tests.

## 2) V10 Formulation
- Base score: combines incident proximity, latency/span pressure, error flag, and time proximity.
- Evidence score: aligned metric evidence from pod snapshots with sigmoid gate and selectivity term.
- Guard marginal: enforces contrast (normal/error), scenario floor support, and novelty against selected services.
- Objective: maximize lambda*Base + mu*Evidence + eta*Guard with redundancy penalty.
- Residual repair: bounded swaps strictly under 5% of selected traces.

## 3) Unit Tests
- Status: PASS (6/6)
- Coverage includes budget exactness, hard-constraint guards, coherence bounds, swap-limit, determinism, and evidence-weight effect.

## 4) RCA Compare
budget_pct,method,A1,A3,MRR,top1_confidence,top1_gap
0.1,microrank_baseline,0.233660,0.464869,0.410463,11234584.163550645,7376343.51883538
0.1,v9_metric_aware,0.222018,0.423203,0.396181,30042025.81692953,1901653.5945420864
0.1,v10_guard_aware,0.167688,0.439338,0.373358,0.12855103704620838,0.0015163926352579238
0.1,paper_trastrainer,0.425900,0.777400,0.550900,,
1.0,microrank_baseline,0.261234,0.564134,0.444330,3147694.819842393,189124.28189174517
1.0,v9_metric_aware,0.161765,0.408088,0.339710,48.56011782230393,10.099131656658498
1.0,v10_guard_aware,0.142157,0.380310,0.327976,0.12300772527423177,0.008953143384336778
1.0,paper_trastrainer,0.451600,0.785200,0.588900,,
2.5,microrank_baseline,0.185049,0.532884,0.398845,41.664864898284314,8.994439681781047
2.5,v9_metric_aware,0.116830,0.382761,0.304711,47.257951534722224,7.7743533876634
2.5,v10_guard_aware,0.117851,0.328227,0.294393,0.13077849491665552,0.6013733043097947
2.5,paper_trastrainer,0.500000,0.822600,0.655600,,

## 5) Ablation
variant,budget_pct,A1,A3,MRR,jaccard,kendall_tau,swap_loss
V10-A,0.1,0.127042,0.331699,0.290692,1.000000,1.000000,0.000000
V10-A,1.0,0.107435,0.357026,0.280377,1.000000,1.000000,0.000000
V10-A,2.5,0.107435,0.382353,0.310457,1.000000,1.000000,0.000000
V10-B,0.1,0.087827,0.365196,0.279913,1.000000,1.000000,0.000000
V10-B,1.0,0.128268,0.387255,0.319871,1.000000,1.000000,0.000000
V10-B,2.5,0.117851,0.338644,0.299254,1.000000,1.000000,0.000000
V10-C,0.1,0.119077,0.401144,0.333576,1.000000,1.000000,0.000000
V10-C,1.0,0.128268,0.366422,0.315476,1.000000,1.000000,0.000000
V10-C,2.5,0.117851,0.328227,0.294393,1.000000,1.000000,0.000000
V10-D,0.1,0.167688,0.439338,0.373358,0.983486,0.980369,0.044477
V10-D,1.0,0.142157,0.380310,0.327976,0.999451,0.999351,0.027359
V10-D,2.5,0.117851,0.328227,0.294393,1.000000,1.000000,0.000000

## 6) Coherence
- budget 0.1: jaccard=0.983486, kendall_tau=0.980369, displacement_rate=0.008373, avg_swap_loss=0.044477
- budget 1.0: jaccard=0.999451, kendall_tau=0.999351, displacement_rate=0.000275, avg_swap_loss=0.027359
- budget 2.5: jaccard=1.000000, kendall_tau=1.000000, displacement_rate=0.000000, avg_swap_loss=0.000000

## 7) Verdict
V10 implementation is complete with reproducible artifacts for comparison, ablation, telemetry, and tests.