# Crash quarantine — restart budget exhausted

`botd` exits with a non-classified code (not 65–70) = real crash. The
operator restarts under backoff+jitter, but N crashes inside M minutes
exceeds the crash budget → phase `quarantined`, **no further restarts**
(docs/04 §4.4). Quarantine is the platform saying "this bot needs a human
opinion, not another restart". Exit 70 (strict snapshot failure) funnels
here too — the bot can't safely resume.

## Symptom

- Alert `XenomitBotsQuarantined`: `sum(xenomit_bot_phase{phase="quarantined"}) > 0`.
- Panel: bot `quarantined`; pod stays down, not recreated.

## Diagnose

```powershell
kubectl get xenomitbot <bot> -n xenomit-bots -o jsonpath='{.status.lastExits} {.status.phase}'
kubectl logs <pod> -n xenomit-bots --tail=200 --previous   # last crash log (pod kept 1h per docs/04 §4.7)
kubectl get events -n xenomit-bots --field-selector involvedObject.name=<bot>
```

SigNoz:

```promql
sum(xenomit_bot_phase{phase="quarantined"})
increase(xenomit_bot_restarts_total[30m])                    # how hard it crashed
increase(xenomit_snapshot_restore_total{outcome="failed"}[1h]) # exit-70 path?
```

Traces on `xenomit.bot.id=<bot>`: the terminating span carries the panic
context; look for the last `PhaseChanged` before death — mid-`restoring`
deaths point at snapshot restore, mid-`running` at module/engine state.

**Crash classes:**

| Evidence | Meaning |
|---|---|
| `outcome="failed"` restores + exit 70 | snapshot row corrupt/tampered (sha256 mismatch or schema-invalid — docs/09 §9.3) |
| panic in module code, same step every time | game state or config the engine can't handle |
| OOMKill (pod `reason: OOMKilled`, no app crash) | memory limit too tight for the module — `kubectl describe pod` shows it |
| image/config changed just before crashes began | bad rollout — go to bad-rollout-rollback.md |

## Act

**Snapshot-corruption path (exit 70):** restore from an earlier clean
snapshot instead of the latest:

```
GET  /v1/bots/{id}/snapshots            # pick last-known-good (clean, pre-incident)
POST /v1/bots/{id}/restore              { "snapshot_id": "<id>" }
```

Then clear the quarantine so the reconciler may retry:

```
PATCH /v1/bots/{id}   { "desiredState": "running" }
```

(if the API exposes a dedicated `clear-quarantine` action, prefer it; the
patch is the fallback that re-arms the reconciler.)

**Deterministic module crash:** don't unquarantine blind — the crash budget
will just re-trip. Reproduce under supervision: set
`desiredState: running` and `kubectl logs -f` the new pod; if it dies at
the same step, the config or the bot's game state is the bug — fix the
config via `PATCH /v1/bots/{id}` or leave quarantined pending a botd fix.

**OOMKill:** raise the bot's memory via config/operator resources — check
the rendered pod's limits vs `BENCHMARK.md` numbers first (a 21 MiB p50 bot
hitting a 64 MiB cap suggests a real leak, not a tight limit).

## Verify

- `sum(xenomit_bot_phase{phase="quarantined"})` decreases; the bot reaches
  `armed`/`running` and survives past its previous crash point.
- `xenomit_snapshot_restore_total{outcome="clean"}` ticks for the restore
  path; no new `failed`.
- Watch one full crash-budget window (M minutes) before declaring fixed —
  a bot that re-quarantines was never fixed.
