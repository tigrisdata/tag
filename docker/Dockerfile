FROM alpine:3.21

ARG TARGETARCH

# Install minimal runtime dependencies
RUN apk add --no-cache ca-certificates tzdata wget

# Create non-root user
RUN addgroup -g 1000 tag && \
    adduser -u 1000 -G tag -h /home/tag -D tag

# Copy pre-built binary (downloaded by workflow)
COPY --chmod=755 bin/${TARGETARCH}/tag /usr/local/bin/tag

# Create cache directory
RUN mkdir -p /var/cache/tag && chown -R tag:tag /var/cache/tag

USER tag
VOLUME ["/var/cache/tag"]

# Expose ports:
# 8080 - HTTP (S3 API)
# 7000 - Memberlist gossip (cluster discovery)
# 9000 - gRPC (cache routing)
EXPOSE 8080 7000 9000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["tag"]
