# Epitranscriptomics Insights Backend

A production-ready, scalable backend system for epitranscriptomics analysis supporting thousands of users per day.

## 📊 Overview

This backend provides a high-performance API and worker system for bioinformatics analysis, specifically focused on epitranscriptomics research. The architecture is designed to handle large-scale analysis workloads with horizontal scaling, fault tolerance, and comprehensive monitoring.

### Key Features

- **🚀 High Performance**: Async FastAPI with connection pooling and Redis caching
- **📈 Scalable**: Horizontal pod autoscaling and multi-priority task queues
- **🔒 Secure**: JWT authentication, rate limiting, and security headers
- **🔄 Reliable**: Circuit breakers, retry logic, and comprehensive error handling
- **📊 Observable**: Structured logging, metrics, and distributed tracing
- **🐳 Production-Ready**: Optimized Docker containers and Kubernetes deployment

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │     Frontend    │    │   API Gateway   │
│     (nginx)     │ -> │   (Next.js)     │ -> │   (FastAPI)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                       ┌─────────────────┐             │
                       │     Redis       │ <-----------┘
                       │    (Cache)      │
                       └─────────────────┘
                                │
    ┌──────────────────────────┼──────────────────────────┐
    │                          │                          │
┌───▼────┐            ┌────────▼────┐            ┌───────▼───┐
│High Pri│            │   Default   │            │  Low Pri  │
│Workers │            │   Workers   │            │  Workers  │
│(2 pods)│            │  (3 pods)   │            │ (1 pod)   │
└────────┘            └─────────────┘            └───────────┘
    │                          │                          │
    └──────────────────────────┼──────────────────────────┘
                               │
    ┌─────────────────┐   ┌────▼────┐   ┌─────────────────┐
    │   PostgreSQL    │   │RabbitMQ │   │     MinIO       │
    │   (Database)    │   │(Queue)  │   │   (Storage)     │
    └─────────────────┘   └─────────┘   └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- 16GB+ RAM (recommended for bioinformatics workloads)
- 100GB+ storage space

### Development Setup

1. **Clone and Navigate**
   ```bash
   cd backend/
   ```

2. **Environment Configuration**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Build and Start Services**
   ```bash
   cd infra/
   docker-compose up --build
   ```

4. **Verify Installation**
   ```bash
   curl http://localhost:8080/health
   ```

### Production Deployment

#### Using Docker Compose (Small Scale)

```bash
# Production environment
export ENVIRONMENT=production
export JWT_SECRET=$(openssl rand -base64 32)
export POSTGRES_PASSWORD=$(openssl rand -base64 32)

docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

#### Using Kubernetes (Large Scale)

```bash
# Install with Helm
cd infra/helm/
helm install epimod . -f values.prod.yaml
```

## 📁 Project Structure

```
backend/
├── services/
│   ├── api/                    # FastAPI application
│   │   ├── main.py            # Main application entry
│   │   ├── auth.py            # JWT authentication
│   │   ├── cache.py           # Redis caching service
│   │   ├── logging_config.py  # Structured logging
│   │   ├── db/                # Database models & sessions
│   │   │   ├── models.py      # SQLAlchemy models
│   │   │   ├── session.py     # DB session management
│   │   │   └── async_session.py # Async DB operations
│   │   └── routes/            # API endpoints
│   │       ├── auth_routes.py # Authentication endpoints
│   │       └── streaming.py   # WebSocket endpoints
│   └── worker/                # Celery worker service
│       ├── tasks.py          # Original task definitions
│       ├── enhanced_tasks.py # Production-ready tasks
│       ├── pipeline/         # Analysis pipeline
│       ├── runners/          # Model runners (m6anet, xpore, etc.)
│       └── notifications.py # Progress notifications
├── infra/                    # Infrastructure configuration
│   ├── docker-compose.yml   # Development containers
│   ├── helm/                # Kubernetes deployment
│   │   ├── Chart.yaml       # Helm chart definition
│   │   ├── values.yaml      # Default configuration
│   │   └── templates/       # K8s resource templates
├── Dockerfile.api           # API container definition
├── Dockerfile.worker        # Worker container definition
└── requirements.txt         # Python dependencies
```

## 🔧 Configuration

### Environment Variables

#### Core Settings
```bash
# Application
ENVIRONMENT=production          # development|staging|production
LOG_LEVEL=INFO                 # DEBUG|INFO|WARNING|ERROR
API_PORT=8080

# Security
JWT_SECRET=your-secret-key     # Generate with: openssl rand -base64 32
ALLOWED_ORIGINS=https://yourdomain.com
ALLOWED_HOSTS=yourdomain.com,*.yourdomain.com

# Database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=epi
POSTGRES_PASSWORD=secure-password
POSTGRES_DB=epimod
DB_ECHO=false                  # Set to true for SQL logging

# Cache
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
CACHE_DEFAULT_TTL=300          # 5 minutes

# Message Queue
RABBITMQ_HOST=rabbitmq
RABBITMQ_USER=admin
RABBITMQ_PASS=secure-password
RABBITMQ_URL=pyamqp://admin:secure-password@rabbitmq//

# Storage
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=secure-password
MINIO_BUCKET=epimod-data

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000
```

#### Worker Configuration
```bash
# Celery Workers
CELERY_WORKER_CONCURRENCY=4    # Processes per worker
CELERY_RESULT_BACKEND=redis://redis:6379/1
QUEUE=default                  # default|high_priority|low_priority
```

### Production Tuning

#### API Service
```yaml
# Gunicorn configuration for production
workers: 4                    # 2 * CPU cores
worker_class: uvicorn.workers.UvicornWorker
max_requests: 1000            # Restart worker after N requests
max_requests_jitter: 100      # Add randomness to max_requests
```

#### Database Optimization
```sql
-- PostgreSQL configuration for high load
max_connections = 200
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB
checkpoint_completion_target = 0.7
```

#### Redis Configuration
```redis
# Redis performance tuning
maxmemory 1gb
maxmemory-policy allkeys-lru
save 900 1                    # Persistence settings
save 300 10
save 60 10000
```

## 🔒 Security

### Authentication & Authorization

- **JWT Tokens**: Access tokens (15min) + Refresh tokens (7 days)
- **Password Hashing**: bcrypt with 12 rounds
- **Rate Limiting**: 10 requests/minute for job creation, 30/minute for status checks
- **CORS**: Restrictive origin and header policies

### Security Headers

```python
# Implemented security middleware
- TrustedHostMiddleware: Prevent host header injection
- CORS: Restricted origins and methods
- Rate limiting: Prevent API abuse
- Input validation: Comprehensive Pydantic models
```

### Production Security Checklist

- [ ] Set strong `JWT_SECRET` (32+ random bytes)
- [ ] Use environment variables for all secrets
- [ ] Enable HTTPS with valid certificates
- [ ] Configure firewall rules
- [ ] Regular security updates
- [ ] Monitor for suspicious activity

## 📊 Monitoring & Observability

### Structured Logging

```python
# Log format (JSON in production)
{
  "timestamp": "2024-10-08T10:30:00Z",
  "level": "INFO",
  "logger": "api.jobs",
  "message": "Job created successfully",
  "job_id": "uuid-here",
  "user_id": "user123",
  "correlation_id": "req-abc123"
}
```

### Health Checks

- **API Health**: `GET /health` - Redis connectivity, service status
- **Worker Health**: Celery inspect ping
- **Database Health**: PostgreSQL connection test
- **Queue Health**: RabbitMQ diagnostics

### Metrics (Prometheus Compatible)

```python
# Custom metrics available
- api_requests_total{method, endpoint, status}
- api_request_duration_seconds{method, endpoint}
- job_processing_duration_seconds{model}
- worker_active_jobs{queue}
- cache_hit_ratio
```

## 🚀 Performance & Scaling

### Capacity Planning

#### Small Scale (100-500 daily users)
```yaml
api:
  replicas: 2
  resources: {cpu: 1, memory: 1Gi}
worker:
  replicas: 2
  resources: {cpu: 2, memory: 4Gi}
postgres:
  resources: {cpu: 2, memory: 4Gi}
```

#### Medium Scale (500-2000 daily users)
```yaml
api:
  replicas: 3
  resources: {cpu: 2, memory: 2Gi}
worker:
  replicas: 5
  resources: {cpu: 4, memory: 8Gi}
postgres:
  resources: {cpu: 4, memory: 8Gi}
  read_replicas: 2
```

#### Large Scale (2000-5000 daily users)
```yaml
api:
  replicas: 5
  resources: {cpu: 2, memory: 2Gi}
  hpa: {min: 5, max: 20, cpu: 70%}
worker:
  replicas: 10
  resources: {cpu: 4, memory: 8Gi}
  hpa: {min: 10, max: 50, cpu: 80%}
postgres:
  resources: {cpu: 8, memory: 16Gi}
  read_replicas: 3
redis_cluster: true
```

### Performance Optimizations

1. **Database**
   - Connection pooling (20 connections)
   - Read replicas for GET operations
   - Async operations with asyncpg
   - Proper indexing on job queries

2. **Caching**
   - Redis for session storage
   - API response caching (30s TTL)
   - Database query result caching
   - Job status caching (1 hour TTL)

3. **Workers**
   - Priority-based queues
   - Exponential backoff retries
   - Resource-based auto-scaling
   - Efficient task routing

## 🛠️ API Reference

### Authentication

```bash
# Register new user
POST /auth/register
{
  "username": "researcher1",
  "password": "secure-password",
  "email": "researcher@university.edu"
}

# Login
POST /auth/token
{
  "username": "researcher1",
  "password": "secure-password"
}
```

### Job Management

```bash
# Create analysis job
POST /jobs
Authorization: Bearer <token>
{
  "accession": "SRR123456",
  "reference_fasta": "hg38",
  "models": ["m6anet", "xpore"],
  "priority": "normal"
}

# Get job status
GET /jobs/{job_id}
Authorization: Bearer <token>

# Response
{
  "id": "job-uuid",
  "status": "completed",
  "result": {
    "m6anet": {"sites_tsv": "/results/sites.tsv"},
    "xpore": {"modifications": "/results/mods.json"}
  }
}
```

### Health & Monitoring

```bash
# System health
GET /health

# Metrics (Prometheus format)
GET /metrics
```

## 🧪 Testing

### Unit Tests
```bash
# Run API tests
cd services/api/
pytest tests/ -v --cov=.

# Run worker tests
cd services/worker/
pytest tests/ -v --cov=.
```

### Integration Tests
```bash
# Full system test
docker-compose -f docker-compose.test.yml up --abort-on-container-exit
```

### Load Testing
```bash
# Install k6
# Test API performance
k6 run tests/load/api-load-test.js

# Test with 1000 concurrent users
k6 run --vus 1000 --duration 5m tests/load/stress-test.js
```

## 🚨 Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check database connectivity
docker-compose exec postgres pg_isready -U epi

# Check connection pool
docker-compose logs api | grep "connection pool"

# Fix: Increase pool size in database config
```

#### Worker Queue Backlog
```bash
# Check queue status
docker-compose exec rabbitmq rabbitmqctl list_queues

# Scale workers
docker-compose up --scale worker=5

# Monitor queue depth
watch "docker-compose exec rabbitmq rabbitmqctl list_queues"
```

#### Redis Cache Issues
```bash
# Check Redis connectivity
docker-compose exec redis redis-cli ping

# Monitor cache hit ratio
docker-compose exec redis redis-cli info stats | grep hit

# Clear cache if needed
docker-compose exec redis redis-cli flushall
```

#### High Memory Usage
```bash
# Check container memory usage
docker stats

# Identify memory leaks
docker-compose exec api ps aux --sort=-%mem

# Scale down workers if needed
docker-compose up --scale worker=2
```

## 📈 Production Monitoring

### Recommended Monitoring Stack

1. **Prometheus + Grafana**: Metrics and dashboards
2. **ELK Stack**: Centralized logging
3. **Jaeger**: Distributed tracing
4. **AlertManager**: Intelligent alerting

### Key Alerts

```yaml
# Critical alerts to configure
- API response time > 2s (95th percentile)
- Error rate > 5%
- Database connection pool exhaustion
- Worker queue depth > 100
- Disk usage > 80%
- Memory usage > 90%
```

## 🤝 Contributing

### Development Workflow

1. Fork the repository
2. Create feature branch: `git checkout -b feature/awesome-feature`
3. Run tests: `pytest tests/`
4. Commit changes: `git commit -m 'Add awesome feature'`
5. Push branch: `git push origin feature/awesome-feature`
6. Submit pull request

### Code Standards

- **Python**: Black formatting, isort imports, mypy type checking
- **Documentation**: Docstrings for all functions
- **Testing**: 90%+ test coverage required
- **Security**: No hardcoded secrets, input validation

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋 Support

- **Documentation**: [Wiki](https://github.com/yourorg/epimod/wiki)
- **Issues**: [GitHub Issues](https://github.com/yourorg/epimod/issues)
- **Security**: security@yourdomain.com
- **Community**: [Discord](https://discord.gg/yourserver)

---

**Built with ❤️ for the epitranscriptomics research community**