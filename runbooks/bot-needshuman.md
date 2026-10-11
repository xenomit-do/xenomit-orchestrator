# Bot in `NeedsHuman` — CAPTCHA / credentials / expired session

Exit codes **65** (credentials rejected) and **66** (CAPTCHA) put the bot in
`needs_human` and it will NOT retry on its own (ADR-14). A third cause is an
expired `dosid` session supplied instead of a password. This is a
user-facing hold, not a platform bug — the platform's job is to surface the
right reason fast.

## Symptom

- Panel: bot shows `needs_human` with reason; `xenomit_bot_phase{phase="needs_human"} > 0`.
- SigNoz query:
  ```promql
  sum by (phase) (xenomit_bot_phase{phase="needs_human"})
  ```

## Diagnose

```powershell
kubectl get xenomitbot <bot> -n xenomit-bots -o jsonpath='{.status.needsHuman} {.status.lastExit}{.status.lastExitCode}'
kubectl logs <pod> -n xenomit-bots --tail=100        # terminal reason from botd
kubectl get events -n xenomit-bots --field-selector involvedObject.name=<bot> --sort-by=.lastTimestamp
```

Map the reason:

| Exit / `needsHuman` value | Meaning | Owner |
|---|---|---|
| 66 / `captcha` | game served a CAPTCHA on login | **user** |
| 65 / `credentials_rejected` | wrong password or banned account | **user** |
| `session_expired` | `dosid` supplied instead of password and it lapsed | **user** |

## Act

**CAPTCHA (66):** the user completes the challenge out-of-band and supplies
a fresh `dosid`:

```
POST /v1/bots/{id}/dosid        { "dosid": "<new session id>" }
```
(Panel: bot page → "supply session id".) The operator clears `needs_human`
and re-arms the bot on the next reconcile — no pod surgery.

**Credentials rejected (65):** the user re-enters credentials in the panel
(→ Vault `secret/data/bots/<id>`, never the pod). Then:

```
PATCH /v1/bots/{id}             { "desiredState": "running" }
```

**Expired session:** same as CAPTCHA — fresh `dosid` via `POST …/dosid`.

Do **NOT** `kubectl delete pod` as a fix — the pod is intentionally stopped;
the block is the `needs_human` flag on the CR, cleared only by the
credentials/session path above.

## Verify

- `xenomit_bot_phase{phase="needs_human"} == 0` for the bot; phase moves
  `provisioning → armed → running`.
- Login succeeds: `xenomit_login_duration_count{outcome!="failed"}` ticks up
  for the bot on the next attempt.
- If the same bot re-enters `needs_human` within minutes with `captcha`
  twice in a row: stop driving it — repeated CAPTCHA triggers look like
  automation to the game. Flag the account to the user.
