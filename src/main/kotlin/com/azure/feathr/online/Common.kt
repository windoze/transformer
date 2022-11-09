package com.azure.feathr.online

import com.fasterxml.jackson.annotation.JsonInclude
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import net.logstash.logback.argument.StructuredArgument

open class StructuralException(message: String, val args: Array<out StructuredArgument>) : Throwable(message)
open class VertxWebException(message: String, val statusCode: Int = 500, vararg args: StructuredArgument) :
    StructuralException(message, args)

class BadRequestException(message: String = "Bad Request", vararg args: StructuredArgument) :
    VertxWebException(message, 400, *args)

class UnauthorizedException(
    message: String = "Unauthorized",
    val schema: String,
    val realm: String,
    vararg args: StructuredArgument
) :
    VertxWebException(message, 401, *args)

class ForbiddenException(message: String = "Forbidden", vararg args: StructuredArgument) :
    VertxWebException(message, 403, *args)

class NotFoundException(message: String = "Not Found", vararg args: StructuredArgument) :
    VertxWebException(message, 404, *args)

class InternalErrorException(message: String = "Internal Server Error", vararg args: StructuredArgument) :
    VertxWebException(message, 500, *args)

fun HttpServerResponse.notFound(msg: String = "Not found") {
    setStatusCode(404).end(msg)
}

fun HttpServerResponse.badRequest(msg: String = "Bad request") {
    setStatusCode(400).end(msg)
}

fun HttpServerResponse.internalError(msg: String = "Internal Server Error") {
    setStatusCode(500).end(msg)
}

fun HttpServerResponse.endWithRawString(s: String) {
    putHeader("content-type", "application/json; charset=utf-8").end(s)
}

fun HttpServerResponse.endWith(o: Any) {
    putHeader("content-type", "application/json; charset=utf-8").end(Json.encodePrettily(o))
}

inline fun <reified T> RoutingContext.jsonBody(): T = body().asJsonObject().mapTo(T::class.java)

inline fun <reified T> JsonObject.mapAs(): T = mapTo(T::class.java)

data class RequestEntry(val pipeline: String = "", val data: Map<String, Any?> = mapOf(), val validate: Boolean = false)
data class Request(val requests: List<RequestEntry> = listOf())

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ResponseEntry(
    val pipeline: String,
    val status: String,
    val count: Int = 0,
    val time: Long = 0,
    val data: List<Map<String, Any?>> = listOf(),
)

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class Response(val results: List<ResponseEntry> = listOf())

