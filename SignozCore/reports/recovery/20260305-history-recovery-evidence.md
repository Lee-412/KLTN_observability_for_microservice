# History Recovery Evidence (2026-03-05)

## 1) Incident summary

- Original Git history is no longer available in both local and remote repository.
- Current `origin/main` contains only one commit: `45329cf`.
- No additional remote refs (heads/tags/pull refs) were found.
- No secondary local clone, `.git.old`, `.bundle`, or KLTN archive backup was found in common work directories.

## 2) Reconstructed work timeline from generated artifacts

Evidence from timestamped report files:

- 2026-03-02 17:07:10: `sampler-proof` generated.
- 2026-03-02 22:20:13: AB proof compare reports generated.
- 2026-03-02 22:18:54 → 23:10:09: multiple retention-thesis and compare reports generated.
- 2026-03-03 00:10:52: one-click proof + combined thesis proof reports generated.
- 2026-03-03 00:39:36: one-click proof + combined thesis proof reports generated.

Recent artifact filenames observed:

- `SignozCore/reports/compare/20260302-220013-ab-hotrod-sameinput-evaluate.txt`
- `SignozCore/reports/compare/20260302-231009-retention-thesis-proof-20260302-230730.txt`
- `SignozCore/reports/compare/20260303-001052-combined-proof-thesis-proof-20260303-000747.txt`
- `SignozCore/reports/sampler-proof/20260303-003936-oneclick-proof.txt`

### Additional evidence before 2026-03-02

Pre-02/03 report artifacts exist and prove work activity in February:

- `SignozCore/reports/compare/20260202-234100-baseline2_vs_model1.txt`
- `SignozCore/reports/compare/20260205-101558-20260205-101028-traces.json_VS_20260205-101334-traces.model.json.txt`
- `SignozCore/reports/compare/20260209-213452-20260209-212930-traces.json_VS_20260209-213406-traces.model.json.txt`
- `SignozCore/reports/compare/20260225-090340-20260225-084446-traces.json_VS_20260225-090234-traces.model.json.txt`
- `SignozCore/reports/compare/20260226-160046-traces.baseline.json_VS_traces.sampled.json.txt`

This confirms experiment and comparison runs were already performed before 2026-03-02.

Additional pre-02/03 evidence recovered from Trash:

- `SignozCore/reports/recovery/trash_restored/compare/20260301-151501-baseline2_vs_model1.txt`
- `SignozCore/reports/recovery/trash_restored/compare/20260301-151657-baseline2_vs_model1.txt`
- `SignozCore/reports/recovery/trash_restored/compare/20260301-151750-baseline2_vs_model1.txt`

These files further prove work activity existed on 2026-03-01, i.e., before 2026-03-02.

## 3) Reconstructed coding activity from file modification times

Python script mtimes (local filesystem):

- 2026-03-02 23:54:57: `run_ab_matrix.py`, `run_compare_and_save.py`, `run_hotrod_baseline_model.py`, `test_adaptive_sampling.py`, `tracegen_latency_mix.py`, `verify_adaptive_sampling.py`.
- 2026-03-03 00:17:22: `compare_otlp_traces.py`.
- 2026-03-03 00:31:14: `evaluate_ab_sampling.py`, `one_click_sampling_proof.py`.

## 4) Reconstructed Git activity from shell history

Commands found in shell history indicate iterative development and repository operations, including:

- `git add .`
- `git commit -m "feat(SignozCore): update adaptive sampling, create script AB testing "`
- `git push origin main`
- repeated cleanup attempts around `otel-export` tracking
- `git filter-repo`
- `git init`
- `git push origin main --force`

This supports that multiple development/cleanup steps occurred before history collapse.

Additional shell-history evidence (pre-2026-03-02) includes commands referencing:

- baseline/model trace pairs from `20260205`, `20260209`, and `20260225`
- repeated `python3 scripts/run_compare_and_save.py ...`
- repeated `docker-compose up -d --force-recreate otel-collector`
- AB evaluation and latency trace generation (`evaluate_ab_sampling.py`, `tracegen_latency_mix.py`)

These command patterns corroborate sustained development/testing before 2026-03-02.

## 4.1) Findings for newly created folder `KLTN_`

Terminal inspection of `/home/leeduc/Desktop/KLTN/KLTN_` shows:

- Only a minimal directory skeleton under `signoz/deploy/...`
- No `.git` metadata inside this folder
- No regular files detected (directories only, including names ending with `.yaml` created as folders)
- Ownership mostly `root:root`, timestamps around `2026-03-02 19:42`

Conclusion: `KLTN_` does not contain restorable Git history or additional pre-02/03 commit evidence.

## 4.2) Findings from system Trash

Inspection of `~/.local/share/Trash` found deleted project artifacts (not Git metadata):

- deleted paths map to `SignozCore/...` via `.trashinfo`
- no `.git`, `.bundle`, `.pack`, or `.idx` found in Trash
- useful artifacts were restored to `SignozCore/reports/recovery/trash_restored/`

Conclusion: Trash can strengthen timeline evidence, but cannot restore original commit DAG.

## 5) Practical note for thesis/demo transparency

Because original commit DAG cannot currently be restored from available Git sources, this report serves as a forensic timeline from surviving artifacts and command history.

Recommended disclosure sentence:

"Git history was accidentally overwritten during repository cleanup. The process timeline is reconstructed from generated experiment reports, script modification timestamps, and shell command history."

## 6) Official recovery path still worth attempting

Open a GitHub Support request with:

- Repository URL
- Approximate incident window (2026-03-03)
- Statement that branch history was overwritten after re-init/force push
- Request to restore deleted/overwritten refs if still retained in backend snapshot

Success is not guaranteed, but this is the only realistic path to recover true historical commits without another clone/backup.
