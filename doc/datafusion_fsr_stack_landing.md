# DataFusion + FSR Stack Landing Playbook

This playbook keeps stack ownership tight while landing the `stack/datafusion -> stack/fsr`
sequence.

## Branch Order

1. `stack/plan-ir`
2. `stack/plan-errors`
3. `stack/kdf-framework`
4. `stack/kdf-delta`
5. `stack/sm-framework`
6. `stack/datafusion`
7. `stack/fsr`

## Ownership Contract

- `stack/datafusion`
  - DataFusion crate integration, compile/lowering/runtime plumbing, vendor patches, generic
    feature-gating hardening.
  - Must not own FSR domain wiring as the primary behavior slice.
- `stack/fsr`
  - FSR state-machine behavior, FSR fixture matrix tests, DAT FSR add-only harness wiring.

## Pre-PR Audit Commands

Run from repository root.

```bash
git stack ls
git status --short
```

Validate parent-child ancestry and diff stats:

```bash
git merge-base --is-ancestor stack/datafusion stack/fsr
git diff --stat stack/sm-framework..stack/datafusion
git diff --stat stack/datafusion..stack/fsr
```

Quick leakage checks (should return no output for parent-only ownership leaks):

```bash
git diff --name-only stack/sm-framework..stack/datafusion | rg "plans/state_machines/fsr|snapshot/full_state|tests/fsr_real"
```

Expected overlap from child branch:

```bash
git diff --name-only stack/datafusion..stack/fsr
```

## Verification Gates

```bash
cargo check -p delta_kernel --no-default-features
cargo check -p delta_kernel --no-default-features --features declarative-plans
cargo check -p delta-kernel-datafusion-engine
cargo test -p delta-kernel-datafusion-engine --test fsr_real
cargo test -p acceptance --test dat_reader_datafusion --no-run
```

## Push Sequence

```bash
git push -u origin stack/datafusion
git push -u origin stack/fsr
```

If history was rewritten during ownership cleanup, use `--force-with-lease` only on the rewritten
branches and avoid force-pushing unrelated stack nodes.
