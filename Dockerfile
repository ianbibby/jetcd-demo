FROM openjdk:8-jdk-alpine
VOLUME /tmp
ADD target/spring-locking-demo-0.0.1-SNAPSHOT.jar app.jar
ENV JAVA_OPTS=""
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar"]