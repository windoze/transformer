package com.azure.feathr

import ch.qos.logback.classic.Level
import com.azure.feathr.online.PipelineVerticle
import com.azure.feathr.online.WebVerticle
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import io.vertx.core.CompositeFuture
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.core.logging.LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME
import io.vertx.core.logging.SLF4JLogDelegateFactory
import io.vertx.kotlin.core.json.Json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class AppArgs(parser: ArgParser) {
    val conf by parser.storing("-c", "--conf", help = "config file name").default("pipeline.conf")
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

object GlobalState {
    val vertx = Vertx.vertx()
}

fun main(args: Array<String>) = mainBody {
    ArgParser(args).parseInto(::AppArgs).run {
        DatabindCodec.mapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)

        debugLog(debug)
        devLog(verbose)

        System.setProperty(LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory::class.java.name)
        LoggerFactory.getLogger(LoggerFactory::class.java) // Required for Logback to work in Vertx

        val log = LoggerFactory.getLogger(this.javaClass.name)
        log.info("Starting application...")

        val vertx = GlobalState.vertx
        try {
            val conf = vertx.fileSystem().readFileBlocking(conf).toString()
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
            log.warn("Config file '$conf' not found.")
            exitProcess(1)
        }
    }
}


//suspend fun main(args: Array<String>) {
//    val src =
//        """
//        t1(f1 as int, f2 as int, f3 as string, f4 as array)
//        | where f2>100
//        | project f5 = substring(f3, 2), f6 = "abc\txyz"
//        | top 3 by f2+10 desc
//        | mv-expand f4 as int;
//        """.trimIndent()
//    val pipelines = PipelineParser().parse(src)
//    println(pipelines.toList().joinToString("\n") { "${it.first}${it.second.dump()}" })
//
//    println(pipelines["t1"]!!.outputSchema.dump())
//
//    val ds = EagerDataSet(
//        listOf(
//            Column("f1", ColumnType.INT),
//            Column("f2", ColumnType.INT),
//            Column("f3", ColumnType.STRING),
//            Column("f4", ColumnType.ARRAY)
//        ),
//        listOf(
//            listOf(10, 100, "foo1", listOf(1, 2)),
//            listOf(20, 200, "foo2", listOf(1, 2, 3)),
//            listOf(30, 300, "foo3", listOf(1, 2, 3, 4)),
//            listOf(40, 400, "foo4", listOf(1, 2, 3, 4, 5)),
//            listOf(50, 500, "foo5", listOf(1, 2, 3, 4, 5, 6)),
//        )
//    )
//
//    val tds = pipelines["t1"]!!.process(ds).fetchAll().await()
//    tds.dump()
//}