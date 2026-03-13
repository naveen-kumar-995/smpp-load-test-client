FROM eclipse-temurin:21-jdk

ENV TZ="Asia/Kolkata"
EXPOSE 20000/tcp

RUN apt update && apt install -y curl && apt install -y vim

RUN rm -rf /opt/apps/smpp-load-test-client/conf/logback.xml

RUN mkdir -p /opt/apps/smpp-load-test-client/conf

COPY smpp-load-test-client/logback.xml /opt/apps/smpp-load-test-client/conf/logback.xml


COPY ./target/smpp-load-test-client-1.0.0.jar /smpp-load-test-client-1.0.0.jar

ENTRYPOINT ["java","-Dlogback.configurationFile=/opt/apps/smpp-load-test-client/conf/logback.xml","-jar","smpp-load-test-client-1.0.0.jar"]

