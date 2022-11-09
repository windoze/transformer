package com.azure.feathr.online

import com.azure.feathr.pipeline.Pipeline
import com.azure.feathr.pipeline.TransformerException
import com.azure.feathr.pipeline.parser.PipelineParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.toReceiveChannel
import kotlinx.coroutines.future.await

class PipelineVerticle : CoroutineVerticle() {
    private val mapper = ObjectMapper()
    private var pipelines: Map<String, Pipeline> = mutableMapOf()

    override suspend fun start() {
        super.start()

        pipelines = PipelineParser().parse(config.getString("conf"))

        val bus = vertx.eventBus()
        val consumer = bus.localConsumer<String>("pipeline")
        val channel = consumer.toReceiveChannel(vertx)
        while (!channel.isClosedForReceive) {
            val msg = channel.receive()
            val req = mapper.readValue<Request>(msg.body())
            try {
                val resp = process(req)
                msg.reply(mapper.writeValueAsString(resp))
            } catch (e: VertxWebException) {
                msg.fail(e.statusCode, e.message)
            }
        }
    }

    private suspend fun process(request: Request): Response {
        try {
            val entries = request.requests.map { req ->
                val start = System.currentTimeMillis()
                val data = pipelines[req.pipeline]?.let { p ->
                    val inputRow = p.inputSchema.map { col ->
                        req.data[col.name]
                    }
                    p.processSingle(inputRow, req.validate).fetchAll().await()?.map { row ->
                        p.outputSchema.zip(row.evaluate()).associate { (col, value) ->
                            col.name to value?.value
                        }
                    } ?: listOf()
                }
                val stop = System.currentTimeMillis()
                if (data == null) {
                    ResponseEntry(req.pipeline, "Pipeline not found")
                } else {
                    ResponseEntry(req.pipeline, "OK", data.size, stop - start, data)
                }
            }
            return Response(entries)
        } catch (e: TransformerException) {
            throw BadRequestException(e.message ?: "Bad request")
        } catch (e: Throwable) {
            throw InternalErrorException(e.message ?: "Internal error")
        }
    }
}