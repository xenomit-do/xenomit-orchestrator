# Runbooks (Phase 9)

One file per operational condition. Format is deliberately uniform:

> **Symptom** (what you see/what fires) → **Diagnose** (exact commands +
> SigNoz queries) → **Act** (exact commands) → **Verify/exit criteria**.

Queries are PromQL for SigNoz's Metrics → Query Builder (metric names are
OTel dots rendered as underscores, e.g. `xenomit.bot.phase` →
`xenomit_bot_phase`). `kubectl` commands assume the default context — add
`-n` as shown; every bot-facing command carries the namespace explicitly.

| Runbook | Alert that leads here |
|---|---|
| [vault-unseal.md](vault-unseal.md) | `XenomitVaultSealed`, `XenomitVaultUnreachable` |
| [bot-needshuman.md](bot-needshuman.md) | `xenomit_bot_phase{phase="needs_human"} > 0` (panel shows the reason) |
| [bot-needsupdate.md](bot-needsupdate.md) | `XenomitNeedsUpdateGameChanged` |
| [fenced-loops.md](fenced-loops.md) | `XenomitGatewayFencedSpike` |
| [proxy-failure-storm.md](proxy-failure-storm.md) | `XenomitProxyUnhealthyRatio`, exit-69 wave |
| [crash-quarantine.md](crash-quarantine.md) | `XenomitBotsQuarantined` |
| [host-reboot-recovery.md](host-reboot-recovery.md) | whole-fleet outage after host/Docker restart |
| [disk-pressure.md](disk-pressure.md) | node `DiskPressure` / image GC events |
| [registry-outage.md](registry-outage.md) | rollouts stalling on ImagePullBackOff |
| [bad-rollout-rollback.md](bad-rollout-rollback.md) | `XenomitSnapshotRestoreFailed` during rollout, regression gates |

Related: `docs/rpo-rto.md` (backup targets), `ops/drills/` (restore drills),
`docs/security-review-rbac.md`, `ci/policies/README.md` (admission denials —
if a pod won't *create*, check `kubectl get events -n <ns>` for a Kyverno
denial before anything else).
