# Docker

Run TAG using Docker Compose. For all configuration options, see the [Configuration Reference](configuration.md).

## Build-from-source vs. released image

Two sets of Compose files ship in this repo:

- **`docker/docker-compose.yml`** / **`docker/docker-compose-cluster.yml`** — build the image from the local `Dockerfile`. Use these for local development against source.
- **`deploy/docker/docker-compose.release.yml`** / **`deploy/docker/docker-compose-cluster.release.yml`** — pull the published `tigrisdata/tag` image. Use these to run a released version without building, e.g. `docker-compose -f docker-compose.release.yml up -d`.

The examples below use the build-from-source files in `docker/`. To run a released image instead, `cd deploy/docker` and pass the `*.release.yml` files with `-f`. The released-image files default to the `latest` published image; to pin a specific release, set `TAG_VERSION` (e.g. `TAG_VERSION=v1.9.4`) in your `.env`.

## Prerequisites

Create a `.env` file in the directory of the Compose files you run (`docker/` for build-from-source, `deploy/docker/` for the released image) with your Tigris credentials:

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are TAG's own Tigris credentials with read-only access to all buckets accessed through TAG (required). Clients use their own credentials directly.

## Single Node

```bash
cd docker
docker-compose up -d
```

TAG will be available at `http://localhost:8080`.

```bash
# View logs
docker-compose logs -f tag

# Stop
docker-compose down
```

## Cluster Mode

Run 3 TAG nodes with an embedded distributed cache cluster:

```bash
cd docker
docker-compose -f docker-compose-cluster.yml up -d
```

TAG endpoints:

- `http://localhost:8081` (tag-1)
- `http://localhost:8082` (tag-2)
- `http://localhost:8083` (tag-3)

Each node discovers the others via gossip and shares cached objects across the cluster.

```bash
# View logs
docker-compose -f docker-compose-cluster.yml logs -f

# Stop and remove volumes
docker-compose -f docker-compose-cluster.yml down -v
```

## Environment Variables

You can add optional environment variables to the `.env` file:

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
TAG_LOG_LEVEL=info
```

See the full [Configuration Reference](configuration.md) for all options.

## Test

```bash
# Health check
curl http://localhost:8080/health

# Download an object using AWS CLI
aws s3 cp s3://your-bucket/your-key ./local-file \
  --endpoint-url http://localhost:8080
```
