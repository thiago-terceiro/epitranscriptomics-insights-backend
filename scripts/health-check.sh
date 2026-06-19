#!/bin/bash
# Quick health check script for local development

set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🔍 Checking local development environment...${NC}\n"

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running. Please start Docker Desktop.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

# Check if services are up
if ! docker compose -f infra/docker-compose.local.yml ps | grep -q "Up"; then
    echo -e "${RED}❌ Services are not running. Run: make local-up${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose services are running${NC}"

# Check API health
echo -e "\n${YELLOW}Testing API...${NC}"
if curl -sf http://localhost:8080/health > /dev/null; then
    echo -e "${GREEN}✓ API is healthy (http://localhost:8080)${NC}"
else
    echo -e "${RED}❌ API not responding. Check logs: make local-logs${NC}"
fi

# Check API docs
if curl -sf http://localhost:8080/docs > /dev/null; then
    echo -e "${GREEN}✓ API docs available (http://localhost:8080/docs)${NC}"
else
    echo -e "${RED}⚠ API docs not available${NC}"
fi

# Check RabbitMQ
echo -e "\n${YELLOW}Testing RabbitMQ...${NC}"
if curl -sf http://localhost:15672 > /dev/null; then
    echo -e "${GREEN}✓ RabbitMQ management (http://localhost:15672 - guest/guest)${NC}"
else
    echo -e "${RED}❌ RabbitMQ not responding${NC}"
fi

# Check MinIO
echo -e "\n${YELLOW}Testing MinIO...${NC}"
if curl -sf http://localhost:9001 > /dev/null; then
    echo -e "${GREEN}✓ MinIO console (http://localhost:9001 - minioadmin/minioadmin)${NC}"
else
    echo -e "${RED}❌ MinIO not responding${NC}"
fi

# Check MLflow
echo -e "\n${YELLOW}Testing MLflow...${NC}"
if curl -sf http://localhost:5000 > /dev/null; then
    echo -e "${GREEN}✓ MLflow (http://localhost:5000)${NC}"
else
    echo -e "${RED}❌ MLflow not responding${NC}"
fi

# Check worker
echo -e "\n${YELLOW}Testing Celery Worker...${NC}"
if docker compose -f infra/docker-compose.local.yml exec -T worker celery -A tasks.celery_app inspect ping 2>&1 | grep -q "pong"; then
    echo -e "${GREEN}✓ Worker is responding to Celery tasks${NC}"
else
    echo -e "${RED}❌ Worker not responding. Check: docker compose -f infra/docker-compose.local.yml logs worker${NC}"
fi

# Database connection
echo -e "\n${YELLOW}Testing PostgreSQL...${NC}"
if docker compose -f infra/docker-compose.local.yml exec -T postgres pg_isready -U epi -d epimod > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PostgreSQL is ready${NC}"
else
    echo -e "${RED}❌ PostgreSQL not ready${NC}"
fi

# Redis connection
echo -e "\n${YELLOW}Testing Redis...${NC}"
if docker compose -f infra/docker-compose.local.yml exec -T redis redis-cli ping 2>&1 | grep -q "PONG"; then
    echo -e "${GREEN}✓ Redis is responding${NC}"
else
    echo -e "${RED}❌ Redis not responding${NC}"
fi

echo -e "\n${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ All checks complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "\n${YELLOW}📚 Quick Links:${NC}"
echo -e "   API Docs:    http://localhost:8080/docs"
echo -e "   RabbitMQ:    http://localhost:15672"
echo -e "   MinIO:       http://localhost:9001"
echo -e "   MLflow:      http://localhost:5000"
echo -e "\n${YELLOW}📝 Next Steps:${NC}"
echo -e "   1. Register a user: see LOCAL_TESTING.md"
echo -e "   2. Submit a test job"
echo -e "   3. Monitor logs: make local-logs"
