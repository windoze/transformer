package com.azure.feathr.online

import com.azure.feathr.pipeline.Pipeline
import com.azure.feathr.pipeline.lookup.LookupSourceRepo
import com.azure.feathr.pipeline.parser.PipelineParser
import io.vertx.core.eventbus.EventBus
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import net.logstash.logback.argument.StructuredArguments
import org.slf4j.LoggerFactory
import io.vertx.core.json.JsonObject

open class WebVerticle(definition: String) : CoroutineVerticle() {
    @Transient
    private val log = LoggerFactory.getLogger(this.javaClass)

    @Transient
    lateinit var server: HttpServer

    private val pipelines: Map<String, Pipeline> = PipelineParser().parse(definition)

    private val router: Router by lazy {
        Router.router(vertx)
    }

    private val bus: EventBus by lazy {
        vertx.eventBus()
    }

    override suspend fun start() {
        log.info("Starting ${this.javaClass.name}...")

        server = vertx.createHttpServer()
        router.route()
            .handler(
                CorsHandler.create("*")
                    .allowedHeaders(
                        setOf(
                            "x-requested-with",
                            "Access-Control-Allow-Origin",
                            "origin",
                            "Content-Type",
                            "authorization",
                            "accept"
                        )
                    )
                    .allowedMethods(
                        setOf(
                            HttpMethod.GET,
                            HttpMethod.POST,
                            HttpMethod.PUT,
                            HttpMethod.DELETE,
                            HttpMethod.PATCH
                        )
                    )
            )
            .handler(BodyHandler.create())
            .handler(StaticHandler.create().setDefaultContentEncoding("UTF-8"))

        router.get("/healthz")
            .produces("application/json")
            .coroHandler {
                if (healthCheck())
                    "OK"
                else
                    throw InternalErrorException("Health check failed")
            }

        router
            .get("/pipelines")
            .produces("application/json")
            .coroHandler {
                pipelines.mapValues { pipelineToJson(it.value) }
            }

        router
            .get("/lookup-sources")
            .produces("application/json")
            .coroHandler {
                LookupSourceRepo.lookupSources
            }

        router
            .post("/process")
            .produces("application/json")
            .coroStrHandler {
                try {
                    bus.request<String>("pipeline", it.body().asString()).await().body()
                } catch (e: ReplyException) {
                    throw VertxWebException(e.message ?: "Unknown error", e.failureCode())
                }
            }


        getPort().let {
            server.requestHandler(router).listen(it)
            log.info(
                "WebVerticle started listening on port $it.",
                StructuredArguments.keyValue("listening_port", it)
            )
        }
    }

    private fun getPort(): Int {
        return System.getenv("HTTP_PLATFORM_PORT")?.toInt() ?: config.getInteger("http.port", 8000)
    }

    private fun pipelineToJson(pipeline: Pipeline): Map<String, Any> {
        return mapOf(
            "definition" to pipeline.dump(),
            "input" to pipeline.inputSchema.map {
                mapOf(
                    "name" to it.name,
                    "type" to it.type.toString().lowercase()
                )
            },
            "output" to pipeline.outputSchema.map {
                mapOf(
                    "name" to it.name,
                    "type" to it.type.toString().lowercase()
                )
            },
        )
    }

    private fun Route.coroStrHandler(handler: suspend (RoutingContext) -> String) {
        handler {
            launch(it.vertx().dispatcher()) {
                try {
                    it.response().endWithRawString(handler(it))
                } catch (e: VertxWebException) {
                    it.response().setStatusCode(e.statusCode).setStatusMessage(e.message).end()
                } catch (e: Throwable) {
                    it.response().internalError(e.message ?: "Internal error")
                }
            }
        }
    }

    private fun Route.coroHandler(handler: suspend (RoutingContext) -> Any) {
        handler {
            launch(it.vertx().dispatcher()) {
                try {
                    it.response().endWith(handler(it))
                } catch (e: VertxWebException) {
                    it.response().setStatusCode(e.statusCode).setStatusMessage(e.message).end()
                } catch (e: Throwable) {
                    it.response().internalError(e.message ?: "Internal error")
                }
            }
        }
    }

    open suspend fun healthCheck(): Boolean {
        // Check by calling %healthcheck pipeline, 57+42=99
        val req = """{"requests": [{ "pipeline": "%healthcheck", "data": { "n": 57 } }]}"""
        val resp = JsonObject(bus.request<String>("pipeline", req).await().body())
        val m = resp
            .getJsonArray("results")
            ?.getJsonObject(0)
            ?.getJsonArray("data")
            ?.getJsonObject(0)
            ?.getLong("m")
            ?: return false
        return m == 99L
    }
}