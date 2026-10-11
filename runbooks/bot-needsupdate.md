# Bot in `NeedsUpdate` — the game changed

Exit **68**: `botd` detected a game-version mismatch, self-drained, and
parked (ADR-15). Fleet-wide `needs_update` means the *game* updated and our
packet schema is stale — the expected pipeline is detect → apply → PR →
fast-lane rollout (`docs/07 §7.1`), and this alert is the page that starts
it.

## Symptom

- Alert `XenomitNeedsUpdateGameChanged` (`sum(xenomit_bot_phase{phase="needs_update"}) > 0`).
- Panel: bots parked `needs_update`; rollouts refuse to start new waves.

## Diagnose

```powershell
kubectl get xenomitbots -n xenomit-bots -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,SCHEMA:.status.packetSchema
kubectl get cronjob packet-update-detect -n ci          # is detection running?
kubectl get jobs -n ci --sort-by=.metadata.creationTimestamp | tail -5
```

SigNoz:

```promql
sum by (packet_schema) (xenomit_bot_phase{phase="needs_update"})   # stale schema hash(es)
increase(xenomit_bot_restarts_total[1h])                           # exit-68 self-drain wave
```

Check the game side: did `main.swf` change? The detect Job logs its fetched
sha256 — compare to the one baked into the running image
(`JOB_TEMPLATE`/CI build log). If detect has been failing for hours, the
fleet found the change before the pipeline did — runbook stays the same,
but fix the CronJob afterward.

## Act

1. **Confirm the pipeline knows:** look for a recent `auto/packet-*` PR
   (packet-updater apply Job) or run the detect Job manually:

   ```powershell
   kubectl create job --from=cronjob/packet-update-detect detect-manual-$(Get-Date -Format 'HHmm') -n ci
   kubectl logs -n ci job/detect-manual-* -f
   ```

2. **Apply the schema update** — merge the auto PR (it carries the new
   VERSION_HASH + regenerated packet table). CI builds + signs the new
   `xenomit-botd` image and publishes its digest (docs/07 §7.2).

3. **Roll the fleet** to the new image — fast-lane BotRollout:

   ```
   POST /v1/rollouts   { "targetImage": "<registry>@sha256:<new>",
                         "mode": "fast-lane", "maxDraining": 5 }
   ```

   Bots in `needs_update` are already offline — their handoffs skip the
   drain wait (nothing to drain), so the wave moves at pod-start speed.

## Verify

- `sum(xenomit_bot_phase{phase="needs_update"}) → 0` as waves land.
- New pods run the new digest: `kubectl get pods -n xenomit-bots -o jsonpath='{..containers[0].image}'` shows the rollout target.
- If `needs_update` persists after a rollout: the image didn't pick up the
  schema change — check `xenomit.packet.schema` attribute on bot metrics vs
  the game's live hash; an unchanged hash means the packet PR didn't
  actually update VERSION_HASH.
