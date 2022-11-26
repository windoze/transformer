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

val kotlinCoroutineVersion = "1.6.4"
val vertxVersion = "4.3.5"
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
    implementation("ch.qos.logback:logback-classic:1.4.5")
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
    implementation("com.noenv:vertx-jsonpath:4.3.5")
    implementation("net.jodah:typetools:0.6.3")
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

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
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
