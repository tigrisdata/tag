# TLS/HTTPS

TAG supports TLS encryption for serving requests over HTTPS. TLS is disabled by default and must be explicitly configured. For all configuration options, see the [Configuration Reference](configuration.md).

## Configuration

TLS requires both a certificate file and a private key file. Both must be provided together; setting only one will cause a validation error at startup.

### Environment Variables

```bash
export TAG_TLS_CERT_FILE=/path/to/cert.pem
export TAG_TLS_KEY_FILE=/path/to/key.pem
```

### Configuration File

```yaml
server:
  tls_cert_file: /path/to/cert.pem
  tls_key_file: /path/to/key.pem
```

The certificate file should contain the full chain: the server certificate followed by any intermediate certificates.

When TLS is enabled, TAG serves all requests over HTTPS. The startup logs will indicate the protocol in use.

## Generate Self-Signed Certificates

For testing and development, generate a self-signed certificate:

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

> **Note:** Self-signed certificates are suitable for development only. Use certificates from a trusted CA for production deployments.

## Docker

Mount the certificate and key files into the container and set the environment variables:

```yaml
services:
  tag:
    image: tigrisdata/tag:v1.11.1
    ports:
      - "8080:8080"
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - TAG_TLS_CERT_FILE=/etc/tag/tls/cert.pem
      - TAG_TLS_KEY_FILE=/etc/tag/tls/key.pem
    volumes:
      - ./certs/cert.pem:/etc/tag/tls/cert.pem:ro
      - ./certs/key.pem:/etc/tag/tls/key.pem:ro
```

Test the connection:

```bash
curl -k https://localhost:8080/health
```

## Kubernetes

Store the TLS certificate and key in a Kubernetes Secret:

```bash
kubectl create secret tls tag-tls \
  --namespace tag \
  --cert=cert.pem \
  --key=key.pem
```

Add the TLS configuration to the StatefulSet:

```yaml
containers:
  - name: tag
    env:
      - name: TAG_TLS_CERT_FILE
        value: "/etc/tag/tls/tls.crt"
      - name: TAG_TLS_KEY_FILE
        value: "/etc/tag/tls/tls.key"
    volumeMounts:
      - name: tls-certs
        mountPath: /etc/tag/tls
        readOnly: true
volumes:
  - name: tls-certs
    secret:
      secretName: tag-tls
```

When using TLS in Kubernetes, update the health check probes to use HTTPS:

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: 8080
    scheme: HTTPS
livenessProbe:
  httpGet:
    path: /health
    port: 8080
    scheme: HTTPS
```

## Native Binary

Set the environment variables before starting TAG:

```bash
export TAG_TLS_CERT_FILE=/path/to/cert.pem
export TAG_TLS_KEY_FILE=/path/to/key.pem
./deploy/native/run.sh start
```

When TLS is enabled, test with:

```bash
curl -k https://localhost:8080/health
```
