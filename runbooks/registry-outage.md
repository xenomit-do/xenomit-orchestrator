# Registry down — `xenomit-registry:5000` unreachable

The local registry holds every botd/control-plane image (digest-pinned, per
the admission policy). Down means: **new pods can't pull** (ImagePullBackOff),
in-flight rollouts stall mid-wave, `ops/verify.ps1` push checks fail. Running
pods keep running — their image is already on the node.

## Symptom

- `kubectl get pods -A` shows `ImagePullBackOff`/`ErrImagePull` on new pods.
- Rollout waves stall: `status.progress` stops moving with no failed exits.
- `ops/verify.ps1` registry check fails; `docker ps --filter name=registry`
  shows the container absent/exited.

## Diagnose

```powershell
docker ps -a --filter name=registry
docker logs --tail 50 <registry-container>
curl http://localhost:5000/v2/              # registry API from the host
kubectl get events -n xenomit-bots --field-selector reason=Failed -A | Select-String "pull|registry"
```

Distinguish:

| Symptom | Cause |
|---|---|
| container exited | Docker/registry daemon issue |
| container up, host `curl` fails | port mapping lost |
| host `curl` OK, cluster pulls fail | the `xenomit-registry` name doesn't resolve in-cluster (k3d registry wiring) or the container is off the `k3d-xenomit` network |
| pulls fail with `manifest unknown`/`401` on a *reachable* registry | image/digest missing or the registry store lost — NOT an outage, see "missing image" below |

## Act

**Container down/absent:**

```powershell
docker start <registry-container>
# or if it was removed entirely, recreate per ops/k3d registry setup:
k3d registry create xenomit-registry --port 5000   # name per ops/k3d config
docker network connect k3d-xenomit <registry-container>   # must share the cluster network
```

**Wrong network / DNS:** the name `xenomit-registry` must resolve
inside the node. Check:

```powershell
docker network inspect k3d-xenomit | Select-String registry
docker exec k3d-xenomit-server-0 getent hosts xenomit-registry
```

**Missing image (registry reachable):** the tag/digest was GC'd or never
pushed. Re-push from CI artifacts or rebuild per `ops/k3d/` + `ci/` docs —
for botd specifically the image comes from the CI build, not the local
tree.

**While the registry is down:** do NOT start rollouts (a wave stalls
mid-drain with successors unschedulable — that's *two* outages). Pause any
in-flight rollout:

```
POST /v1/rollouts/{id}/pause
```

Pods already running keep working — resist deleting them to "clear" the
pull errors: a Running pod deleted during an outage can't come back.

## Verify

- `curl http://localhost:5000/v2/` → `{}`; `docker exec k3d-xenomit-server-0 crictl pull xenomit-registry:5000/xenomit-botd@sha256:<latest>` succeeds.
- `ImagePullBackOff` pods progress to `Running` on kubelet retry (no
  intervention — backoff just takes a minute).
- Resume paused rollouts; watch `xenomit_handoff_offline_window` — waves
  stalled *before* drain don't count against the offline-window SLO, but a
  wave that drained and stalled does (check `status.progress` timestamps).
