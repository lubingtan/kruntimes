ARG KRUNTIMES_VERSION=0.0.3
ARG RUNTIME_IMAGE=ghcr.io/kruntimes/python-runtime:${KRUNTIMES_VERSION}
ARG KUBECTL_VERSION=v1.32.0
ARG KUBECTL_SHA256=646d58f6d98ee670a71d9cdffbf6625aeea2849d567f214bc43a35f8ccb7bf70
FROM ${RUNTIME_IMAGE}

ARG KUBECTL_VERSION
ARG KUBECTL_SHA256

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --output /usr/local/bin/kubectl \
      https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl \
    && echo "${KUBECTL_SHA256}  /usr/local/bin/kubectl" | sha256sum --check \
    && chmod 0755 /usr/local/bin/kubectl
USER 65532
