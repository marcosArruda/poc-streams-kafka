FROM adoptopenjdk/openjdk11:latest
WORKDIR /workspace/app
VOLUME /tmp
ENV DEBUG_PORT=9999
EXPOSE 8080
COPY build/libs/*.jar orders-service.jar
ENTRYPOINT ["java","-jar", "orders-service.jar"]
