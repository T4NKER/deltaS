# deltaS — Delta Sharing Marketplace Prototype

A research prototype of a privacy-preserving data marketplace built on top of the Delta Sharing protocol. Three services (Marketplace API, Seller / Delta Sharing server, Buyer CLI) plus PostgreSQL and LocalStack S3.

## Startup

From the repository root:

```bash
bash scripts/generate_dev_secrets.sh

docker compose --profile testing up -d --build
```

This builds and starts:

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5433 | Marketplace + seller databases |
| `localstack` | 4566 | S3-compatible storage |
| `marketplace` | 8000 | Marketplace API |
| `seller` | 8080 | Delta Sharing server |
| `buyer` | — | CLI container |

The marketplace API is then reachable at `http://localhost:8000` and the Delta Sharing server at `http://localhost:8080`.

## Verifying the stack

```bash
docker compose ps

docker compose exec buyer python -m pytest -q
```

## Buyer CLI

```bash
docker compose exec buyer python -m src.buyer.cli --help
```

## Reset / shutdown

```bash
docker compose --profile testing down

docker compose --profile testing down -v

docker compose --profile testing down -v --rmi local

bash scripts/reset_database.sh
```
