# TAG Deployment Guide

This guide covers deploying TAG in various environments.

## Prerequisites

- Access to Tigris storage with API credentials
- (Optional) An ocache cluster for caching

## Local Development

### Build

```bash
make build
```

### Run

```bash
# Set credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Run without caching (pass-through mode)
./tag

# Run with caching (requires ocache)
TAG_OCACHE_ENDPOINTS=localhost:9000 ./tag

# Run with debug logging
TAG_LOG_LEVEL=debug ./tag
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

## Docker

### Build Image

```bash
docker build -t tag:latest -f deploy/Dockerfile .
```

### Run Container

```bash
# Basic run (no caching)
docker run -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  tag:latest

# With caching
docker run -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  -e TAG_OCACHE_ENDPOINTS=ocache:9000 \
  tag:latest

# With configuration file
docker run -p 8080:8080 \
  -v /path/to/config.yaml:/etc/tag/config.yaml:ro \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  tag:latest
```

### Docker Compose

```yaml
version: '3.8'

services:
  tag:
    build:
      context: .
      dockerfile: deploy/Dockerfile
    ports:
      - "8080:8080"
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - TAG_OCACHE_ENDPOINTS=ocache:9000
      - TAG_LOG_LEVEL=info
    depends_on:
      - ocache

  ocache:
    image: tigrisdata/ocache:latest
    ports:
      - "9000:9000"
    volumes:
      - ocache-data:/data

volumes:
  ocache-data:
```

## Kubernetes

### Prerequisites

1. A running Kubernetes cluster
2. kubectl configured to access the cluster
3. An ocache StatefulSet deployed (see [ocache deployment](https://github.com/tigrisdata/ocache))

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
kubectl apply -f deploy/ --namespace tag
```

### Kubernetes Manifests

The `deploy/` directory contains:

| File | Description |
|------|-------------|
| `deployment.yaml` | TAG Deployment with replicas |
| `service.yaml` | ClusterIP Service for internal access |
| `configmap.yaml` | Configuration file |
| `hpa.yaml` | Horizontal Pod Autoscaler |

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: tag-config
data:
  config.yaml: |
    server:
      http_port: 8080
      bind_ip: "0.0.0.0"

    cache:
      enabled: true
      endpoints:
        - "ocache-0.ocache-headless:9000"
        - "ocache-1.ocache-headless:9000"
        - "ocache-2.ocache-headless:9000"
      ttl: 60m
      size_threshold: 1073741824

    log:
      level: "info"
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tag
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tag
  template:
    metadata:
      labels:
        app: tag
    spec:
      containers:
        - name: tag
          image: tag:latest
          ports:
            - containerPort: 8080
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: tag-credentials
                  key: AWS_ACCESS_KEY_ID
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: tag-credentials
                  key: AWS_SECRET_ACCESS_KEY
          volumeMounts:
            - name: config
              mountPath: /etc/tag
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 1000m
              memory: 512Mi
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: tag-config
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: tag
spec:
  selector:
    app: tag
  ports:
    - port: 8080
      targetPort: 8080
  type: ClusterIP
```

### Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: tag
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: tag
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

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

### Resource Recommendations

| Workload | CPU Request | Memory Request | Replicas |
|----------|-------------|----------------|----------|
| Light | 100m | 128Mi | 2 |
| Medium | 500m | 256Mi | 3-5 |
| Heavy | 1000m | 512Mi | 5-10 |

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

### Security

- Use TLS termination at load balancer or ingress
- Store credentials in Kubernetes Secrets
- Use network policies to restrict pod communication
- Run as non-root user (default in Dockerfile)

### Integration with ocache

TAG requires an ocache cluster for caching. See the [ocache documentation](https://github.com/tigrisdata/ocache) for deployment instructions.

**ocache Connection:**
- TAG auto-discovers ocache nodes from endpoint list
- Supports cluster mode (multiple nodes) or simple mode (single node)
- Handles node failures gracefully (cache misses, not errors)

**Recommended Setup:**
```
ocache cluster: 3 nodes (StatefulSet)
TAG deployment: 3 replicas
```

## Troubleshooting

### Common Issues

**No cache hits:**
- Check ocache endpoint configuration
- Verify ocache cluster is running: `kubectl get pods -l app=ocache`
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
