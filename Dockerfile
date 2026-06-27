# FROM --platform=linux/amd64,linux/arm64 eclipse-temurin:11.0.20.1_1-jdk-focal
ARG TARGETPLATFORM
# Docker Hardened Image (near-zero-CVE). JRE (not JDK) is enough at runtime; the
# -dev variant retains the shell needed by docker-entrypoint.sh and the build steps.
FROM dhi.io/eclipse-temurin:11-jre-dev as base
# DHI images default to a non-root user; the build needs root to create the secor
# user and set ownership (the upstream temurin image ran as root).
USER root

RUN mkdir -p /opt/secor

# Prepare environment
ENV SECOR_HOME=/opt/secor
WORKDIR $SECOR_HOME

# The hardened base has no shadow utils (groupadd/useradd); create the system
# user directly in /etc/passwd + /etc/group.
RUN echo 'secor:x:9999:' >> /etc/group && \
    echo 'secor:x:9999:9999::/opt/secor:/usr/sbin/nologin' >> /etc/passwd

ADD target/secor-*-bin.tar.gz $SECOR_HOME

COPY src/main/scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN find $SECOR_HOME -type d -exec chown -R secor:secor {} \;
RUN chown secor:secor /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

USER secor
ENTRYPOINT ["/docker-entrypoint.sh"]
