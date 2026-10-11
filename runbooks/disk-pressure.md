# Disk pressure — node `DiskPressure`, image GC churn, PVCs filling

Single-node k3d: the node's disk holds images, container layers, emptyDir
outboxes, etcd, AND the local-path PVCs (postgres 5 Gi, gitea 1 Gi, SigNoz
ClickHouse) — usually inside the Docker Desktop VHDX. Pressure shows up as
image GC churn, etcd slow warnings, then `DiskPressure` taint evicting pods.

## Symptom

- `kubectl describe node` → `DiskPressure` condition / taint
  `node.kubernetes.io/disk-pressure`.
- Pods evicted with `reason: Evicted`; image pulls failing with `no space`;
  SigNoz dashboards going blank (ClickHouse stops writing first).

## Diagnose

```powershell
kubectl describe node | Select-String "Pressure|Capacity|Allocatable|Allocated"
kubectl get pods -A --field-selector status.phase=Failed | Select-String Evicted
docker system df                                     # where the bytes actually are
docker exec k3d-xenomit-server-0 df -h / /var/lib/rancher 2>$null    # inside the node
```

SigNoz:

```promql
k8s_node_filesystem_usage_bytes / k8s_node_filesystem_capacity_bytes   # node fs (k8s_cluster receiver)
```

Sort the usual suspects by expected size: `docker system df -v` → images
(each botd image is small but every rollout adds one), container logs,
`volumes` (postgres/gitea/local-path PVCs live in the k3d node container's
`/var/lib/rancher/k3s/storage`).

## Act

Cheapest-first; stop when pressure clears:

1. **Evicted pods + old replicas:**
   ```powershell
   kubectl delete pods -A --field-selector status.phase=Failed
   ```

2. **Dangling images / stopped containers:**
   ```powershell
   docker system prune --volumes   # NOT -a — keep pulled images
   ```
   (Drop `--volumes` if unsure about unused named volumes; `vault-data` is
   `unless-stopped`-attached so it's safe, but prune carefully anyway.)

3. **Old botd images:** every rollout leaves the previous digest.
   Registry GC is manual — inside the registry container or delete+rebuild;
   node-side, `crictl rmi --prune` frees pulled copies:
   ```powershell
   docker exec k3d-xenomit-server-0 crictl rmi --prune
   ```

4. **Terminated pods retained for debugging** (docs/04 §4.7 keeps 1 h):
   force-clear if desperate:
   ```powershell
   kubectl delete pods -n xenomit-bots --field-selector status.phase!=Running
   ```

5. **If the VHDX itself is full at the Windows level:** expand/shrink per
   `ops/wslconfig.md`; `wsl --manage` compaction reclaims the sparse file.

Do **not** delete `local-path` PVC contents by hand — postgres data loss is
handled by `ops/drills/drill-postgres.ps1` restore, not improvisation.

## Verify

- `kubectl describe node` — `DiskPressure` condition gone, taint removed
  (kubelet clears it once usage < threshold).
- Evicted-but-recreatable pods return; `xenomit_bot_phase{phase="running"}`
  restored.
- `docker system df` shows reclaimed space; SigNoz ingests again
  (ClickHouse resumes when its volume has headroom).
