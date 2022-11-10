package com.azure.feathr

import ch.qos.logback.classic.Level
import com.azure.feathr.online.PipelineVerticle
import com.azure.feathr.online.WebVerticle
import com.azure.feathr.pipeline.lookup.LookupSource
import com.azure.feathr.pipeline.lookup.LookupSourceRepo
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import io.vertx.core.CompositeFuture
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.core.logging.SLF4JLogDelegateFactory
import io.vertx.kotlin.core.json.Json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

object Main {
    class AppArgs(parser: ArgParser) {
        val pipeline by parser.storing("-p", "--pipeline", help = "pipeline definition file name").default("pipeline.conf")
        val lookup by parser.storing("-l", "--lookup", help = "lookup source definition file name").default("")
        val debug by parser.flagging("-d", "--debug", help = "show all debug logs").default(false)
        val verbose by parser.flagging("-v", "--verbose", help = "show debug log for the service only").default(false)

        fun debugLog(switch: Boolean) {
            val rootLogger =
                LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
            rootLogger.level = Level.toLevel(if (switch) "debug" else "info")
        }

        fun devLog(switch: Boolean) {
            val logger = LoggerFactory.getLogger(this.javaClass.`package`.name) as ch.qos.logback.classic.Logger
            logger.level = Level.toLevel(if (switch) "debug" else "info")
        }
    }

    val vertx = Vertx.vertx()!!

    fun main(args: Array<String>) = mainBody {
        ArgParser(args).parseInto(::AppArgs).run {
            DatabindCodec.mapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)

            debugLog(debug)
            devLog(verbose)

            System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory::class.java.name)
            LoggerFactory.getLogger(LoggerFactory::class.java) // Required for Logback to work in Vertx

            val log = LoggerFactory.getLogger(this.javaClass.name)
            log.info("Starting application...")

            val vertx = Main.vertx
            try {
                if (lookup.isNotBlank()) {
                    val lookup = vertx.fileSystem().readFileBlocking(lookup).toString()
                    if (lookup.isNotBlank()) {
                        val j = JsonObject(lookup)
                        j.getJsonArray("sources")
                            .map { it as JsonObject }
                            .forEach {
                                val clazz = it.getString("class")
                                it.remove("class")
                                val cls = Class.forName(
                                    if (clazz.contains('.')) {
                                        // Full qualified class name
                                        clazz
                                    } else {
                                        // Default to internal package
                                        "com.azure.feathr.pipeline.lookup.$clazz"
                                    }
                                )
                                val src = it.mapTo(cls) as LookupSource
                                LookupSourceRepo.register(src)
                            }
                    }
                }
            } catch (e: io.vertx.core.file.FileSystemException) {
                log.warn("Lookup source definition file '$lookup' not found.")
            } catch (e: Throwable) {
                log.error("Failed to load lookup sources")
                exitProcess(1)
            }
            try {
                val conf = vertx.fileSystem().readFileBlocking(pipeline).toString()
                runBlocking {
                    val fp = vertx.deployVerticle(
                        PipelineVerticle::class.java,
                        DeploymentOptions()
                            .setConfig(Json.obj("conf" to conf))
                            .setWorker(true)
                            .setInstances(Runtime.getRuntime().availableProcessors())
                            .setWorkerPoolSize(Runtime.getRuntime().availableProcessors())
                    )
                    val fw = vertx.deployVerticle(WebVerticle(conf))
                    CompositeFuture.all(fp, fw).await()
                    log.info("Application started.")
                }
            } catch (e: io.vertx.core.file.FileSystemException) {
                log.warn("Pipeline definition file '$pipeline' not found.")
                exitProcess(1)
            } catch (e: Throwable) {
                log.error("Failed to load pipeline definitions")
                exitProcess(1)
            }
        }
    }
}
