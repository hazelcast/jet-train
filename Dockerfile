# docker build -t hazelcast/jettrain .

FROM maven:3-openjdk-8 as build

WORKDIR work
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY load-static/pom.xml load-static/pom.xml
COPY local-jet/pom.xml local-jet/pom.xml
COPY stream-dynamic/pom.xml stream-dynamic/pom.xml
COPY web/pom.xml web/pom.xml
RUN mvn -B --projects \!common,\!load-static,\!stream-dynamic,\!local-jet dependency:resolve-plugins dependency:resolve
COPY web web
RUN mvn -B --projects \!common,\!load-static,\!stream-dynamic,\!local-jet package

FROM openjdk:8
COPY --from=build /work/web/target/web-1.0-SNAPSHOT-exec.jar /opt/jettrain.jar
ENTRYPOINT ["java", "-jar", "-Dhazelcast.client.config=/etc/jettrain/hazelcast-client.xml", "/opt/jettrain.jar"]