# Fenced loops — stale-epoch streams hammering the gateway

Fencing (ADR-12, docs/03 §3.7): one account, one driver. The bot's epoch
must equal the CR's `status.epoch`; a pod that opens a stream with an old
epoch gets `FENCED` and exits **67**. A *loop* means a pod keeps coming
back with a stale epoch — something is recreating it, or the CR's epoch
bumped without the handoff completing.

## Symptom

- Alert `XenomitGatewayFencedSpike`: `increase(xenomit_gateway_fenced_total[10m]) > 5`.
- Repeated `FENCED` rejects for the same `bot_id` in gateway logs.
- A bot stuck `terminated → provisioning` cycling, or two pods for one bot.

## Diagnose

```powershell
kubectl get xenomitbot <bot> -n xenomit-bots -o jsonpath='{.status.epoch} {.status.phase}'
kubectl get pods -n xenomit-bots -l xenomit.io/bot=<bot> -o wide        # count them — must be ≤1 active
kubectl logs deploy/xenomit-gateway -n xenomit-system --tail=200 | Select-String "FENCED|epoch"
```

SigNoz — find the offender:

```promql
increase(xenomit_gateway_fenced_total[15m])
sum by (pod) (xenomit_bot_restarts_total)      # restart storm co-occurrence
```

Traces: filter on `xenomit.bot.id=<bot>` — look for `Hello` rejects with
`epoch` mismatches (`carried < stored`).

**Three distinct causes:**

| Pattern | Cause |
|---|---|
| One pod, restart loop, epoch never matches | Pod predates an epoch bump; its projected view is stale. Usually a bot CR that was `spec`-edited while the pod was mid-handoff. |
| Two pods, one fenced repeatedly | Successor was created but never `Activate`d — the old pod was already deleted, successor has wrong epoch (reconcile lost the ack). |
| Many bots fenced at once | Operator restarted with an epoch it persisted inconsistently, or a bad rollout bumped epochs then rolled back pods only (not CRs). |

## Act

**Single bot:** force a clean re-provision — delete the stale pod AND
resync the CR:

```powershell
kubectl delete pod -n xenomit-bots -l xenomit.io/bot=<bot>
# re-assert desired state so the reconciler re-arms with the CURRENT epoch:
kubectl annotate xenomitbot <bot> -n xenomit-bots xenomit.io/resync="$(Get-Date -Format s)" --overwrite
```

The operator recreates the pod with the stored epoch — `Hello` then
matches and the stream opens.

**Fleet-wide fencing:** pause all rollouts first (a rollout mid-wave is the
most common mass-fence source):

```
POST /v1/rollouts/{id}/pause
```

then inspect `status.epoch` deltas on a few fenced bots vs their pod's
`xenomit.io/epoch` label:

```powershell
kubectl get pods -n xenomit-bots -o custom-columns=POD:.metadata.name,EPOCH:.metadata.labels.xenomit\.io/epoch
kubectl get xenomitbots -n xenomit-bots -o custom-columns=NAME:.metadata.name,EPOCH:.status.epoch
```

Pods showing an epoch LOWER than the CR are the fenced ones — delete them;
the reconciler replaces them at the right epoch. If the CRs themselves
have a wrong (too-high) epoch — i.e. no pod ever matched — roll the CR
back via `POST /v1/rollouts/{id}/rollback` (see bad-rollout-rollback.md),
which restores the pre-rollout epoch ledger.

## Verify

- `increase(xenomit_gateway_fenced_total[10m])` back to ~0; alert clears.
- `xenomit_bot_phase` shows `running` for the affected bots; stream
  `Hello` accepted in gateway logs.
- No bot has two non-terminated pods: `kubectl get pods -n xenomit-bots |
  sort | uniq -c` on the bot label.
