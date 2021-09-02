FROM clojure AS builder
WORKDIR /tmp
COPY ./ ./
RUN apt-get update
RUN apt-get install -y git
RUN clj -X:uberjar :jar ./skywalker.jar

FROM openjdk:11-jre
COPY --from=builder /tmp/skywalker.jar /
CMD ["java", "-cp", "/skywalker.jar", "clojure.main", "-m", "skywalker.server.main"]