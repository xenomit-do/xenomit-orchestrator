# Proxy failure storm — exit-69 wave / pool collapse

Exit **69**: `botd` couldn't reach its proxy (connect/auth/timeout). One
bot exiting 69 is a dead proxy; *many* bots exiting 69 within minutes is a
pool-level event — provider outage, bad credentials batch, or the egress
path itself broken (egress is default-deny + explicit allow, so a netpol or
node-level break looks identical to a proxy break).

## Symptom

- Alert `XenomitProxyUnhealthyRatio` (>20% of pool not `healthy`).
- `xenomit_proxy_health{state!="healthy"}` climbing; login failures with
  `outcome="proxy"` in traces; exit-69 restarts.

## Diagnose

```powershell
kubectl get proxies.fleet.xenomit.io -n xenomit-bots -o custom-columns=NAME:.metadata.name,STATE:.status.state,CAP:.spec.capacity
kubectl get xenomitbots -n xenomit-bots -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,PROXY:.spec.proxyRef
```

SigNoz — is it one proxy or everything?

```promql
sum by (state, pool) (xenomit_proxy_health)                          # pool view
sum(rate(xenomit_login_duration_count{outcome="failed"}[15m]))        # login collapse
increase(xenomit_bot_restarts_total[15m])                             # exit-69 wave
```

**Distinguish the blast radius:**

| `proxy_health` pattern | Likely cause |
|---|---|
| one proxy `unhealthy`, its bots 69-ing | dead/blocked proxy — provider side |
| a whole `pool` unhealthy | provider outage or shared credential set expired |
| **all** pools unhealthy + bots can't reach anything | egress break: the per-bot egress netpol, node routing, or the node's own outbound path |

If everything is dead, test egress from inside the namespace rather than
trusting the proxy state:

```powershell
kubectl run netprobe --rm -it --restart=Never -n xenomit-bots `
  --image=busybox:1.36 --labels="xenomit.io/verify=netpol,xenomit.io/bot=netprobe,xenomit.io/tenant=ops,xenomit.io/epoch=1" `
  --overrides='{"spec":{"automountServiceAccountToken":false,"securityContext":{"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"t","image":"busybox:1.36","command":["nc","-zvw3","<known-good-proxy-ip>","<port>"],"securityContext":{"runAsNonRoot":true,"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"resources":{"requests":{"cpu":"10m","memory":"16Mi"},"limits":{"cpu":"50m","memory":"32Mi"}}}]}}'
```

(same shape `ops/verify.ps1` uses — the `xenomit.io/verify` label exempts it
from the signature policy only; it still passes all other admission rules.)

## Act

**One/few proxies:** mark them down so `ProxyController` reassigns — the
reassign triggers a graceful handoff (docs/04 §4.4):

```powershell
kubectl annotate proxy <proxy> -n xenomit-bots xenomit.io/force-unhealthy="true" --overwrite
```

Bots move to healthy pool members; remove the annotation after the
provider confirms recovery.

**Whole pool down:** nothing to reassign TO inside the pool — bots ride it
out in `running` (proxy failures don't kill a logged-in session) or park on
restart. Page the provider; consider shifting `spec.proxyRef` to a
different pool for the most-affected bots via `PATCH /v1/bots/{id}`.

**Egress break:** check the shared policies still exist and a recent
platform sync didn't drop them:

```powershell
kubectl get netpol -n xenomit-bots
kubectl describe netpol -n xenomit-bots <bot-egress-policy> | Select-String "Egress"
```

If a policy was deleted, re-apply `ops/k8s/11-bots-allow-infra.yaml` and
the operator's rendered per-bot policies regenerate on next reconcile. If
the node's own egress broke (host VPN/proxy down), that's host-side — see
host-reboot-recovery.md.

## Verify

- `xenomit_proxy_health{state="healthy"}` back to the pool total; ratio
  alert clears.
- Exit-69 wave stops: `increase(xenomit_bot_restarts_total[15m])` ~ 0.
- Reassigned bots: `spec.proxyRef` updated, phase `running`, and
  `xenomit_login_duration_count{outcome!="failed"}` ticking.
