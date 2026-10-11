# Bad rollout — pause and roll back safely

A rollout is "bad" when the new image breaks bots *after* the canary passed
(crash wave, restore failures, reward-rate regression, login failures) or
when it stalls mid-wave (registry outage, admission denial). The machinery:
`BotRollout` drives canary → waves under `maxDraining`/`maxOffline`, with
gates (`maxFailedRestores`, `maxLoginFailureRatePermille`, reward-rate
ratio) that auto-PAUSE; rollback replays the previous image digest through
the same handoff machinery — **the offline-window SLO still applies during
rollback**, it's a rollout in reverse, not a pod massacre.

## Symptom

- Rollout `status.phase: paused` with a gate reason (`maxFailedRestores`,
  `maxLoginFailureRatePermille`, reward ratio).
- Or manually suspected: exit-code wave on the NEW version only —
  `xenomit_snapshot_restore_total{outcome="failed"}` rising during the
  rollout, `needs_human`/`quarantined` clustering on `version=<new>`.

## Diagnose

```powershell
kubectl get botrollout <rollout> -n xenomit-bots -o jsonpath='{.status}'
kubectl get xenomitbots -n xenomit-bots -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,VER:.status.version
```

SigNoz — is the failure correlated to the new version?

```promql
increase(xenomit_snapshot_restore_total{outcome="failed"}[15m])
sum by (version) (xenomit_bot_phase)                                 # fleet by version
sum(rate(xenomit_login_duration_count{outcome="failed"}[15m]))
increase(xenomit_bot_restarts_total[15m])
```

Traces: filter `xenomit.bot.version=<new-digest-tag>`; a canary cohort that
restored `clean` but crashes in `running` is a module bug in the new image
— a fleet that fails at `restoring` is a snapshot-format break.

**First decision — pause or rollback?**
Pause if the cause might be *external* (registry flapping, game hiccup,
proxy storm) — rollback replays the same handoffs again and doubles the
offline seconds. Roll back if the new image itself is the problem.

## Act

1. **Stop the bleeding** — pause (idempotent, always safe):
   ```
   POST /v1/rollouts/{id}/pause
   ```
   In-flight handoffs COMPLETE (the drain already committed); queued waves
   don't start. Fleet sits at its current version mix.

2. **Rollback** when the image is the cause:
   ```
   POST /v1/rollouts/{id}/rollback
   ```
   The controller replays the prior digest at the same `maxDraining`
   discipline — bots hand off back to the old image with epoch bumps, so
   fencing stays intact (never hand-edit `spec.image` on a live rollout;
   that bypasses the wave logic).

3. **If a wave stalls with successors ImagePullBackOff** — that's registry
   trouble (registry-outage.md), not a bad image: fix the registry, then
   `resume`; do NOT rollback an image that never ran.

4. **If a bot is wedged between versions** (pod deleted, CR epoch bumped,
   no successor): the pod-less CR re-provisions on the operator's next
   reconcile at the *rolled-back* digest — give it one reconcile cycle
   before intervening.

5. **After rollback lands:** file the cause (bad image → rebuild via CI;
   the digest should never re-enter a rollout — admission only blocks
   *unsigned* images, not *bad* ones).

## Verify

- `sum by (version) (xenomit_bot_phase)` — fleet converged on the old
  digest; `status.phase` = `rolled back`/`completed`.
- `xenomit_snapshot_restore_total{outcome="failed"}` flatlines;
  `xenomit_bot_phase{phase="running"}` back to fleet size.
- Rollback offline-window cost: `histogram_quantile(0.95, sum by (le)
  (rate(xenomit_handoff_offline_window_bucket[1h])))` — expect a bump;
  sustained breach after the wave means handoffs, not the image, are
  sick (fenced-loops.md).
