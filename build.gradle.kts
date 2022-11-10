import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    application
    antlr
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("com.google.protobuf") version "0.9.1"
}

group = "com.azure"
version = "1.0-SNAPSHOT"

val vertxVersion = "4.3.4"
val protobufVersion = "3.21.9"
val mainClassName = "com.azure.feathr.MainKt"

repositories {
    mavenCentral()
}

dependencies {
    antlr("org.antlr:antlr4:4.11.1") // use ANTLR version 4
    implementation("org.antlr:antlr4-runtime:4.11.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.4")
    implementation("ch.qos.logback:logback-classic:1.4.4")
    implementation("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("io.vertx:vertx-core:$vertxVersion")
    implementation("io.vertx:vertx-web:$vertxVersion")
    implementation("io.vertx:vertx-web-client:$vertxVersion")
    implementation("io.vertx:vertx-redis-client:$vertxVersion")
    implementation("io.vertx:vertx-lang-kotlin:$vertxVersion")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:$vertxVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.0")
    implementation("com.xenomachina:kotlin-argparser:2.0.7")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
    testImplementation("io.vertx:vertx-unit:$vertxVersion")

}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-package", "com.azure.feathr.pipeline.parser")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

tasks.compileKotlin {
    dependsOn("generateGrammarSource")
    dependsOn("generateProto")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveBaseName.set("app")
    archiveVersion.set("")
    archiveClassifier.set("")

    manifest {
        attributes["Main-Class"] = "com.azure.feathr.MainKt"
    }
    mergeServiceFiles {
        include("META-INF/services/io.vertx.core.spi.VerticleFactory")
    }
}

application {
    mainClass.value("com.azure.feathr.MainKt")
}
