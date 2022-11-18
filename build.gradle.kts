import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.image.DockerPushImage
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    application
    antlr
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("com.google.protobuf") version "0.9.1"
    id("com.bmuschko.docker-remote-api") version "8.1.0"
}

group = "com.azure"
version = "1.0-SNAPSHOT"

val kotlinCoroutineVersion = "1.6.4"
val vertxVersion = "4.3.4"
val protobufVersion = "3.21.9"

repositories {
    mavenCentral()
}

dependencies {
    antlr("org.antlr:antlr4:4.11.1") // use ANTLR version 4
    implementation("org.antlr:antlr4-runtime:4.11.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$kotlinCoroutineVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutineVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:$kotlinCoroutineVersion")
    implementation("ch.qos.logback:logback-classic:1.4.4")
    implementation("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("io.vertx:vertx-core:$vertxVersion")
    implementation("io.vertx:vertx-web:$vertxVersion")
    implementation("io.vertx:vertx-web-client:$vertxVersion")
    implementation("io.vertx:vertx-lang-kotlin:$vertxVersion")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:$vertxVersion")
    implementation("io.lettuce:lettuce-core:6.2.1.RELEASE")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.0")
    implementation("com.xenomachina:kotlin-argparser:2.0.7")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.noenv:vertx-jsonpath:4.3.4")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
    testImplementation("io.vertx:vertx-unit:$vertxVersion")

}

application {
    mainClass.value("com.azure.feathr.Main")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-package", "com.azure.feathr.pipeline.parser")
}

tasks.compileKotlin {
    dependsOn("generateGrammarSource")
    dependsOn("generateProto")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveBaseName.set("app")
    archiveVersion.set("")
    archiveClassifier.set("")

    manifest {
        attributes["Main-Class"] = "com.azure.feathr.Main"
    }
    mergeServiceFiles {
        include("META-INF/services/io.vertx.core.spi.VerticleFactory")
    }
}

tasks.register<Tar>("prepareDocker") {
    dependsOn("shadowJar")

    archiveFileName.set("add.tar")
    destinationDirectory.set(layout.buildDirectory.dir("docker"))

    from(projectDir) {
        include("conf/pipeline.conf")
        include("conf/lookup.json")
    }
    from("${buildDir}/libs/app.jar") {
        into("app")
    }
}

tasks.register<Copy>("collectArtifacts") {
    dependsOn("prepareDocker")

    into("${buildDir}/docker")
    from("${projectDir}") {
        include("Dockerfile")
    }
}

tasks.register<DockerBuildImage>("docker") {
    dependsOn("collectArtifacts")
    inputDir.set(layout.buildDirectory.dir("docker"))
    images.add("windoze/transformer:latest")
}
