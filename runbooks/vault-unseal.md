# Vault sealed — unseal runbook

Vault seals on **every** container restart (host reboot, `docker restart`,
crash, OOM). Sealed Vault = no credential reads: *new or restarting* bots
stay `Pending`; **already-running bots are unaffected** (credentials are in
memory). Expected operational state, not a failure — but it needs a human
with the unseal keys. (docs/09 §9.2a)

## Symptom

- Alert `XenomitVaultSealed` (and/or `XenomitVaultUnreachable` if the
  container itself is down).
- SigNoz: `vault_core_unsealed == 0`.
- `docker exec xenomit-vault vault status` → `Sealed: true` (exit 2).
- Bots stuck `Pending`/`provisioning`; orchestrator logs
  `vault login ... 503` / `sealed`.

## Diagnose

```powershell
docker ps -a --filter name=xenomit-vault        # is the container even up?
docker exec xenomit-vault vault status          # sealed? initialized?
docker logs --tail 50 xenomit-vault             # crash loop vs clean restart
```

- Container down → `docker compose -f ops/vault/docker-compose.yml up -d`, then continue below.
- `Initialized: false` on an *existing* data volume means the Raft data is
  gone — STOP, go to `ops/drills/drill-vault.ps1 -WithRestore` restore path
  (docs/rpo-rto.md) before re-initializing.

## Act

Keys live in `ops/vault/cluster-keys.json` (field `unseal_keys_b64`,
threshold = 3 by default — keep the real file off-box per docs/09):

```powershell
$keys = Get-Content ..\xenomit-platform\ops\vault\cluster-keys.json | ConvertFrom-Json
for ($i = 0; $i -lt 3; $i++) {
  docker exec xenomit-vault vault operator unseal $keys.unseal_keys_b64[$i]
}
docker exec xenomit-vault vault status    # Sealed: false
```

If `cluster-keys.json` is missing: fetch the off-box copy. **Without
threshold keys the data is unrecoverable by design** — the only path is
re-initializing and having every user re-enter credentials (docs/09 §9.2a).
This is why the keys file is part of `ops/drills/` point checks.

## Verify

- `vault_core_unsealed == 1` within one scrape interval (30 s); alert clears.
- `docker exec -e VAULT_TOKEN=<ops_token> xenomit-vault vault token lookup self` exits 0.
- Bots that were `Pending` proceed within their next reconcile:
  `kubectl get xenomitbots -n xenomit-bots -o wide` — no `Pending` older
  than ~5 min attributable to vault.
- If unseal keys fail with "unseal key invalid": check you're not mixing
  files from two vault instances (a re-`vault-init` generates NEW keys; the
  old data still needs the OLD keys).
