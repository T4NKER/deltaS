#!/bin/bash

set -e

echo "This will:"
echo "  1. Stop all Docker containers"
echo "  2. Remove all Docker volumes (postgres, localstack, pgadmin)"
echo "  3. Remove all data"
echo ""
read -p "continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Stopping containers and removing volumes"
docker-compose down -v

echo ""
echo "Cleaning up any orphaned volumes"
docker volume prune -f

echo ""
echo "Starting containers"
docker-compose up -d postgres localstack

echo ""
echo "Waiting for PostgreSQL to be ready"
sleep 5

max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec delta-sharing-postgres pg_isready -U deltasharing -d marketplace > /dev/null 2>&1; then
        echo "psql ready"
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done

if [ $attempt -eq $max_attempts ]; then
    echo "psql did not become ready in time"
    exit 1
fi

echo ""
echo "Data migration autorun on startup"
echo "Starting all services"
docker-compose up -d

echo ""
echo "Reset complete"
echo ""
echo "All data has been wiped and reset"
