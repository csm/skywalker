FROM clojure AS builder
WORKDIR /tmp
COPY ./ ./
RUN apt-get update
RUN apt-get install -y git
RUN clj -A:server-deps -P
RUN clj -X:uberjar :jar ./skywalker.jar

FROM openjdk:11-jre
COPY --from=builder /root/.m2/repository/ch/qos/logback/logback-classic/1.2.3/logback-classic-1.2.3.jar /
COPY --from=builder /root/.m2/repository/ch/qos/logback/logback-core/1.2.3/logback-core-1.2.3.jar /
COPY --from=builder /tmp/skywalker.jar /
COPY server/logback.xml /
COPY server/run.sh /
RUN apt-get update
RUN apt-get install -y consul
COPY server/02-consul.json /etc/consul.d/
COPY server/10-skywalker.json /etc/consul.d/
CMD ["run.sh"]