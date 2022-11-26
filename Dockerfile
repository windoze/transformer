FROM gradle:7.5.1-jdk11 AS builder
WORKDIR /usr/src/
COPY . ./
RUN gradle shadowJar

FROM adoptopenjdk/openjdk11:jdk-11.0.11_9-debian
RUN mkdir -p /conf /jars
COPY --from=builder /usr/src/build/libs/app.jar /app/app.jar
EXPOSE 8000
VOLUME ["/conf"]
WORKDIR /
CMD ["java", "-Xmx4G", "-cp", "/jars/*", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", "/app/app.jar", "-p", "/conf/pipeline.conf", "-l", "/conf/lookup.json"]
