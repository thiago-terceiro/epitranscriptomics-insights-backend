# Deployment TODO

## 2. Image Registry (Docker Hub)

- [ ] Create Docker Hub account (if not already)
- [ ] Create repository: `PLACEHOLDER_ORG_NAME/epimod`
- [ ] Images will be tagged as:
  - `PLACEHOLDER_ORG_NAME/epimod:api-0.1.0`
  - `PLACEHOLDER_ORG_NAME/epimod:worker-0.1.0`
- [ ] Run `make build-images && make push-images` to upload
- [ ] Replace `PLACEHOLDER_ORG_NAME` in `makefile` and `helm/values.yaml` with your Docker Hub username

---

## 3. SSL/TLS Configuration

- [x] **Platform-managed**: Render/Railway/Fly.io handle SSL automatically (and those platforms cant handle k8s)
- [x] **Helm/K8s**: Ingress with TLS configured in `values.yaml` (ready for future use)

---

## 4. Storage Strategy (Ephemeral + TTL)

- [x] Source files: Delete after successful analysis (implemented in `tasks.py`)
- [x] Files kept on failure so user can retry
- [x] PostgreSQL: Use platform-managed DB (automatic backups if needed)
- [x] No backup strategy required for demo/MVP phase

---

## Quick Reference

| Data | Strategy |
|------|----------|
| Uploaded files | Ephemeral - delete after analysis |
| Analysis results | TTL 7 days - auto-expire |
| Job metadata (DB) | Platform-managed PostgreSQL |
