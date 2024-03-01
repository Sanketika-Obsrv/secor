FROM --platform=linux/amd64 eclipse-temurin:11.0.20.1_1-jdk-focal
# https://github.com/docker-library/openjdk/issues/145#issuecomment-334561903
# https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=894979
# RUN rm /etc/ssl/certs/java/cacerts ; update-ca-certificates -f
RUN update-ca-certificates -f
RUN apt-get update
RUN apt-get install libcurl4 curl -y
# AMazon Linux
# RUN yum update ca-certificates ; yum install shadow-utils.x86_64 mlocate -y
RUN mkdir -p /opt/secor

# Prepare environment
ENV SECOR_HOME=/opt/secor
ENV HADOOP_HOME=/mnt/data
WORKDIR $SECOR_HOME

RUN groupadd --system --gid=9999 secor && \
    useradd --system --home-dir $SECOR_HOME --uid=9999 --gid=secor secor

ADD target/secor-*-bin.tar.gz $SECOR_HOME

COPY src/main/scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN find $SECOR_HOME -type d -exec chown -R secor:secor {} \;
# Amazon Linux
# RUN chown -R secor:secor $SECOR_HOME
RUN chown secor:secor /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

USER secor
ENTRYPOINT ["/docker-entrypoint.sh"]
