FROM mcr.microsoft.com/openjdk/jdk:11-mariner-cm1
RUN mkdir -p /conf
ADD add.tar /
EXPOSE 8000
VOLUME ["/conf"]
WORKDIR /
ENTRYPOINT ["java", "-Xmx4G", "-cp", "/conf/resources", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", "/app/app.jar", "-p", "/conf/pipeline.conf", "-l", "/conf/lookup.json"]
