# TAG Deployment Guide

This guide covers deploying TAG in various environments.

## Prerequisites

- Access to Tigris storage with API credentials

## Docker

### Quick Start

```bash
# Single node (TAG + ocache)
cd docker
docker-compose up -d

# Cluster mode (2 TAG + 3 ocache)
docker-compose -f docker-compose-cluster.yml up -d
```

### Environment Variables

Create a `.env` file in the `docker/` directory:

```bash
# Required
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# Optional
TAG_LOG_LEVEL=info
```

### Single Node Setup

```bash
# Start services
docker-compose -f docker/docker-compose.yml up -d

# View logs
docker-compose -f docker/docker-compose.yml logs -f tag

# Stop services
docker-compose -f docker/docker-compose.yml down
```

### Cluster Setup

```bash
# Start 2 TAG nodes + 3 ocache cluster
docker-compose -f docker/docker-compose-cluster.yml up -d

# TAG endpoints are available at:
# - http://localhost:8081 (tag-1)
# - http://localhost:8082 (tag-2)

# Stop cluster
docker-compose -f docker/docker-compose-cluster.yml down -v
```

### Test

```bash
# Test with curl
curl -X GET http://localhost:8080/your-bucket/your-key \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Test with AWS CLI
aws s3 cp s3://your-bucket/your-key ./local-file \
  --endpoint-url http://localhost:8080
```

## Kubernetes

### Prerequisites

1. A running Kubernetes cluster
2. kubectl configured to access the cluster

### Deploy

```bash
# Create namespace (optional)
kubectl create namespace tag

# Create credentials secret
kubectl create secret generic tag-credentials \
  --namespace tag \
  --from-literal=AWS_ACCESS_KEY_ID=your_key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your_secret

# Apply manifests
kubectl apply -f kubernetes/ --namespace tag
```

### Kubernetes Manifests

The `kubernetes/` directory contains:

| File | Description |
|------|-------------|
| `deployment.yaml` | TAG Deployment with replicas |
| `ocache.yaml` | ocache StatefulSet with 3 replicas |
| `service.yaml` | Service for internal access |
| `hpa.yaml` | Horizontal Pod Autoscaler |

## Production Considerations

### High Availability

- Deploy multiple TAG replicas behind a load balancer
- Use Kubernetes Deployment with anti-affinity rules
- Configure health checks for automatic recovery

### Scaling

**Horizontal Scaling:**
- TAG is stateless - scale horizontally as needed
- Use HPA based on CPU or custom metrics
- Each replica connects to the same ocache cluster

**Vertical Scaling:**
- Increase memory for high concurrent connection counts
- Increase CPU for high request throughput

### Health Checks

TAG exposes a health endpoint:

```
GET /health
```

Returns `200 OK` when healthy.

### Monitoring

1. Expose `/metrics` endpoint for Prometheus scraping
2. Set up alerts for:
   - High error rate (`tag_requests_total{status="error"}`)
   - Low cache hit ratio (`tag_cache_hits_total / (tag_cache_hits_total + tag_cache_misses_total)`)
   - High upstream latency (`tag_upstream_request_duration_seconds`)

### Integration with ocache

TAG requires an ocache cluster for caching. The deployment manifest includes an ocache StatefulSet with 3 replicas.

## Troubleshooting

### Common Issues

**No cache hits:**
- Verify ocache cluster is running: `kubectl get pods -l app=ocache` in the same namespace
- Check TAG logs for connection errors

**Authentication failures:**
- Verify credentials are set correctly
- Check clock sync between client and TAG
- Review signature calculation logs at debug level

**High latency:**
- Check upstream endpoint latency
- Monitor cache hit ratio
- Review ocache performance

### Debug Mode

Enable debug logging for troubleshooting:

```bash
TAG_LOG_LEVEL=debug ./tag
```

Or in Kubernetes:

```yaml
env:
  - name: TAG_LOG_LEVEL
    value: "debug"
```
