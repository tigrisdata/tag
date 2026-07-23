# Kubernetes Deployment

Deploy TAG as a StatefulSet with an embedded distributed cache cluster. For running locally, see the [README](../README.md). For all configuration options, see the [Configuration Reference](configuration.md).

## Prerequisites

- A running Kubernetes cluster
- `kubectl` configured to access the cluster
- Tigris access key and secret key with read access to all buckets that will be accessed through TAG

## Deploy

### 1. Create a namespace

```bash
kubectl create namespace tag
```

### 2. Create the credentials secret

```bash
kubectl create secret generic tag-credentials \
  --namespace tag \
  --from-literal=AWS_ACCESS_KEY_ID=your_access_key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your_secret_key
```

### 3. Apply the manifests

```bash
kubectl apply -k deploy/kubernetes/base/ -n tag
```

This deploys a 3-replica StatefulSet with:
- Embedded cache on each pod (400 GiB PVC per pod)
- Gossip-based cluster discovery via a headless service
- A LoadBalancer service for external access on port 8080
- Horizontal Pod Autoscaler (3-10 replicas)

### 4. Verify the deployment

```bash
# Check pod status
kubectl get pods -n tag

# Check health
kubectl exec -n tag tag-0 -- curl -s http://localhost:8080/health
```

## Kubernetes Manifests

The `deploy/kubernetes/base/` directory uses Kustomize:

| File | Description |
|------|-------------|
| `kustomization.yaml` | Kustomize configuration with image tag |
| `statefulset.yaml` | TAG StatefulSet (3 replicas, embedded cache) |
| `service.yaml` | LoadBalancer Service for external access |
| `service-headless.yaml` | Headless Service for cluster discovery |
| `hpa.yaml` | Horizontal Pod Autoscaler |

To customize the image version or other settings, create an overlay:

```yaml
# deploy/kubernetes/overlays/production/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
images:
  - name: tigrisdata/tag
    newTag: v1.12.0
```

## Production Considerations

### High Availability

- The StatefulSet deploys 3 replicas by default with pod anti-affinity to distribute across nodes.
- Each TAG pod has its own local cache, so losing a pod only affects cache hit ratio temporarily.
- Health checks (readiness and liveness probes) ensure automatic recovery.

### Scaling

**Horizontal:** The HPA scales from 3 to 10 replicas based on CPU (70%) and memory (80%) utilization. New nodes join the cache cluster automatically. Scaling down may temporarily reduce cache hit ratio.

**Vertical:** Adjust resource requests/limits in the StatefulSet. The default is 2-4 CPUs and 4-8 GiB memory per pod. SSD storage is recommended for cache performance. If you change the PVC volume size, also update `TAG_CACHE_MAX_DISK_USAGE` in the StatefulSet to match (value is in bytes).

### Health Checks

TAG exposes a health endpoint:

```
GET /health
```

Returns `200 OK` when healthy. The StatefulSet configures both readiness and liveness probes against this endpoint.

### Monitoring

TAG exposes Prometheus metrics at `/metrics`. The StatefulSet includes Prometheus annotations for automatic scraping.

Key metrics:
- `tag_requests_total{status="error"}` - error rate
- `tag_cache_hits_total / (tag_cache_hits_total + tag_cache_misses_total)` - cache hit ratio
- `tag_upstream_request_duration_seconds` - upstream latency

## Troubleshooting

### No cache hits

- Check TAG logs for cache initialization errors: `kubectl logs -n tag tag-0`
- Verify the cache PVC is bound: `kubectl get pvc -n tag`
- Ensure the disk path is writable

### Authentication failures

- Verify the credentials secret exists: `kubectl get secret -n tag tag-credentials`
- Check that credentials have read access to the target buckets
- Review signature logs at debug level: set `TAG_LOG_LEVEL=debug` in the StatefulSet

### High latency

- Check upstream endpoint latency
- Monitor cache hit ratio via Prometheus metrics
- Review disk I/O performance on the storage class

### Debug mode

Enable debug logging by updating the StatefulSet:

```yaml
env:
  - name: TAG_LOG_LEVEL
    value: "debug"
```
