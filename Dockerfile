FROM maven:3.5-jdk-8-alpine AS build
WORKDIR /hadoop-graphite-reporter
COPY pom.xml /hadoop-graphite-reporter
RUN mvn verify clean --fail-never
COPY src /hadoop-graphite-reporter/src
RUN mvn -f /hadoop-graphite-reporter/pom.xml clean package

FROM openjdk:8-jre-alpine
COPY --from=build /hadoop-graphite-reporter/target/hadoop-graphite-reporter.jar /opt/hadoop-graphite-reporter.jar
COPY cluster.json /etc/cluster.json

ENV USE_PICKLED=false
ENV GRAPHITE_HOST=localhost
ENV GRAPHITE_PORT=2003
ENV GRAPHITE_PREFIX=hadoop
ENV PERIOD=15
ENV COLLECTOR_POOL_SIZE=50

ENTRYPOINT ["java","-server","-jar","/opt/hadoop-graphite-reporter.jar"]