#FROM mcr.microsoft.com/openjdk/jdk:11-mariner-cm1
FROM adoptopenjdk/openjdk11:jdk-11.0.11_9-debian
RUN mkdir -p /conf /jars
ADD add.tar /
EXPOSE 8000
VOLUME ["/conf"]
WORKDIR /
ENTRYPOINT ["java", "-Xmx4G", "-cp", "/jars/*", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", "/app/app.jar", "-p", "/conf/pipeline.conf", "-l", "/conf/lookup.json"]
