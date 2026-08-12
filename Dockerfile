# Secor (Pinterest Secor, built from https://github.com/Sanketika-Obsrv/secor).
# Base: official Temurin JRE on Ubuntu 24.04 (noble), glibc. Chosen over the
# previous DHI -dev build image (168 OS findings, all fix=none) and over the
# zero-CVE Alpine tags, which are musl and would break the glibc-linked
# snappy/zstd natives on the write path. The shell-less DHI runtime tags cannot
# run the entrypoint. Pinned to -noble rather than the floating 11-jre tag:
# 26.04 replaced GNU coreutils with rust-coreutils, which alone carries ~20
# no-fix MEDIUM findings; noble's GNU coreutils scans clean (16 MED total vs
# ~41, all fix=none). Revisit when the rust-coreutils advisories settle.
ARG BASE_IMAGE=eclipse-temurin:11-jre-noble

FROM ${BASE_IMAGE}

ENV SECOR_HOME=/opt/secor
WORKDIR ${SECOR_HOME}

USER 0
# pull in any security patches Ubuntu has published since the base tag was cut
# (the base pins libsystemd0/libudev1 etc. at whatever was current at its build time)
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && rm -rf /var/lib/apt/lists/*

# create the secor 9999 uid/gid directly (works on any base, with or without useradd)
RUN printf 'secor:x:9999:9999:secor:/opt/secor:/bin/bash\n' >> /etc/passwd \
 && printf 'secor:x:9999:\n' >> /etc/group

# secor distribution: maven-built tarball (target/secor-*-bin.tar.gz), same source as upstream.
# NOTE: ADD --chown does not reliably rewrite ownership of auto-extracted tar contents here
# (verified: extracted files keep the UID/GID baked into the tarball by the host that built it)
# -- the explicit chown -R below is NOT redundant, despite --chown already being on the ADD/COPY.
ADD --chown=9999:9999 target/secor-*-bin.tar.gz ${SECOR_HOME}/
COPY --chown=9999:9999 src/main/scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh && chown -R 9999:9999 "$SECOR_HOME" /docker-entrypoint.sh

USER 9999:9999
ENTRYPOINT ["/docker-entrypoint.sh"]
