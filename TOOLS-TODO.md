# Tools & Features TODO

Features marked as "coming soon" on the home page that need implementation.

---

## 🔭 Observability

### Prometheus Metrics

- [ ] Add `prometheus-fastapi-instrumentator` to FastAPI
- [ ] Expose `/metrics` endpoint
- [ ] Track request latency, error rates, active jobs
- [ ] Add Celery task metrics (queue depth, task duration)
- [ ] Configure Prometheus service in docker-compose

### Grafana Dashboards

- [ ] Add Grafana service to docker-compose
- [ ] Create API performance dashboard
- [ ] Create Celery worker dashboard
- [ ] Create job pipeline dashboard (throughput, success rate)
- [ ] Set up alerting rules for failures

---

## 🔐 Security

### Encrypted Storage

- [ ] Enable MinIO server-side encryption (SSE-S3)
- [ ] Configure encryption keys management
- [ ] Encrypt sensitive job payloads at rest
- [ ] Add TLS for MinIO connections in production

### Role-Based Access Control (RBAC)

- [ ] Add `role` field to User model (admin, researcher, viewer)
- [ ] Create permissions mapping per role
- [ ] Add `@require_role()` decorator for endpoints
- [ ] Implement organization/team-based access
- [ ] Restrict job access to owner + admins

### Audit Trails

- [ ] Create `AuditLog` database model
- [ ] Log all job submissions with user/timestamp
- [ ] Log file uploads and downloads
- [ ] Log authentication events (login, logout, token refresh)
- [ ] Log admin actions (user management, config changes)
- [ ] Add `/audit` endpoint for admins to query logs

---

## 🧬 Pipeline

### xPore Runner (Full Implementation)

- [ ] Implement two-condition comparison workflow
- [ ] Add differential modification detection
- [ ] Generate proper xPore output files
- [ ] Add xPore-specific parameters to job schema

---

## 📊 Dashboard

### Interactive Pipeline Tour

- [ ] Record walkthrough video or create animated demo
- [ ] Show job submission flow
- [ ] Show live telemetry visualization
- [ ] Show differential analysis results

---

## Priority

| Feature | Priority | Effort |
|---------|----------|--------|
| Prometheus Metrics | High | Medium |
| RBAC | High | High |
| Audit Trails | Medium | Medium |
| Encrypted Storage | Medium | Low |
| Grafana Dashboards | Medium | Medium |
| xPore Full Implementation | Low | High |
| Interactive Tour | Low | Medium |
