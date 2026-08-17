ARG KRUNTIMES_VERSION=0.0.3
FROM ghcr.io/kruntimes/python-runtime:${KRUNTIMES_VERSION}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --output /usr/local/bin/kubectl \
      https://dl.k8s.io/release/v1.32.0/bin/linux/amd64/kubectl \
    && chmod 0755 /usr/local/bin/kubectl
USER 65532
