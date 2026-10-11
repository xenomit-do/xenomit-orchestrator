# ci/

Build of the orchestrator image and the Rust check workflow.

```
workflows/rust.yml                 fmt, clippy, check, and cargo test
docker/Dockerfile.orchestrator     xenomit-orchestratord image
docker/root.dockerignore           allow-list copied to the workspace root
```

The cluster Deployment, panel image, packet-updater image, and the image
promotion workflow live in `xenomit-platform`. Argo CD applies
`xenomit-platform/services/gitops`.
