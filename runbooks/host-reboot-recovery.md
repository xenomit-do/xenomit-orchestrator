# Host reboot recovery — Docker Desktop / k3d / Windows restart

Order of dependencies after the box comes back: Docker → k3d cluster →
Vault (sealed!) → Postgres → control plane → bots. Most of it self-heals;
two things never do on their own: **Vault stays sealed** and
**`kubectl port-forward` sessions die**.

## Symptom

- Everything down or `xenomit_bot_phase` absent (no telemetry flowing).
- SigNoz empty/stale; `kubectl` can't reach the API initially.
- After cluster return: bots `Pending` — Vault sealed (docs/09 §9.2a risk table).

## Diagnose

```powershell
docker ps -a --filter name=k3d --filter name=xenomit-vault
kubectl get nodes                       # API reachable? node Ready?
kubectl get pods -A | Select-String -NotMatch "Running|Completed"   # stragglers
docker exec xenomit-vault vault status  # sealed?
```

## Act — in dependency order

1. **Docker Desktop up, then the cluster:** k3d clusters auto-start with
   Docker's restart policy; if not:

   ```powershell
   k3d cluster start xenomit        # name per ops/k3d config
   kubectl get nodes                # Ready
   ```

2. **Vault:** container has `restart: unless-stopped` → it comes back
   **sealed**. Unseal per runbooks/vault-unseal.md:

   ```powershell
   $keys = Get-Content ..\xenomit-platform\ops\vault\cluster-keys.json | ConvertFrom-Json
   for ($i = 0; $i -lt 3; $i++) { docker exec xenomit-vault vault operator unseal $keys.unseal_keys_b64[$i] }
   ```

3. **Postgres + control plane:** StatefulSet/Deployments restart on their
   own; watch until ready:

   ```powershell
   kubectl wait --for=condition=Ready pod/postgres-0 -n xenomit-data --timeout=120s
   kubectl wait --for=condition=Ready pod -l app.kubernetes.io/part-of=xenomit-control-plane -n xenomit-system --timeout=120s
   ```

4. **Argo/Gitea (if used):** `kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=gitea -n git`; Argo apps self-heal — the `xenomit-bots` app needs nothing (its desired state is empty by design).

5. **Bots:** the operator reconciles desired vs live state. Bots that were
   `running` get new pods; those mid-handoff resume per the epoch protocol.
   **Do not mass-delete pods** — let the controller sequence it (rate-
   limited logins, docs/04 §4.7). If a bot wedges `Pending` >10 min with
   Vault unsealed, see fenced-loops.md.

6. **Port-forwards** (any you rely on — gitea push, SigNoz UI) are gone;
   re-establish only what you actively use.

## Verify

- `vault_core_unsealed == 1`, postgres `Ready`, control-plane pods `Ready`.
- `sum(xenomit_bot_phase{phase="running"})` returns to pre-reboot count
  within ~15 min (login-rate limiter throttles the wave by design).
- `xenomit_login_duration_count` resumes ticking; no mass
  `needs_human`/`quarantined` (those are *other* runbooks, not reboot
  fallout).
- If the reboot was Windows-update-driven, also check WSL2 memory reclaim —
  a squeezed VM presents as slow etcd, not dead pods (docs/11 wslconfig).
