FROM maven:3.5-jdk-8-alpine AS build
WORKDIR /hadoop-graphite-reporter
COPY pom.xml /hadoop-graphite-reporter
RUN mvn dependency:go-offline package
COPY src /hadoop-graphite-reporter/src
RUN mvn -f /hadoop-graphite-reporter/pom.xml clean package

FROM openjdk:8-jre-alpine
COPY --from=build /hadoop-graphite-reporter/target/hadoop-graphite-reporter.jar /opt/hadoop-graphite-reporter.jar
COPY cluster.json /etc/cluster.json
ENTRYPOINT ["java","-server","-jar","/opt/hadoop-graphite-reporter.jar"]