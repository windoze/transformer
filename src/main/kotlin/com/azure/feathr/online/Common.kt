package com.azure.feathr.online

import com.fasterxml.jackson.annotation.JsonInclude
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
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

suspend fun HttpServerResponse.notFound(msg: String = "Not found") {
    setStatusCode(404).end(msg).await()
}

suspend fun HttpServerResponse.badRequest(msg: String = "Bad request") {
    setStatusCode(400).end(msg).await()
}

suspend fun HttpServerResponse.internalError(msg: String = "Internal Server Error") {
    setStatusCode(500).end(msg).await()
}

suspend fun HttpServerResponse.endWithRawString(s: String) {
    putHeader("content-type", "application/json; charset=utf-8").end(s).await()
}

suspend fun HttpServerResponse.endWith(o: Any) {
    putHeader("content-type", "application/json; charset=utf-8").end(Json.encodePrettily(o)).await()
}

inline fun <reified T> RoutingContext.jsonBody(): T = body().asJsonObject().mapTo(T::class.java)

inline fun <reified T> JsonObject.mapAs(): T = mapTo(T::class.java)

enum class ErrorReportingMode {
    /**
     * Silently skip all rows contain errors without reporting
     */
    SKIP,

    /**
     * Silently convert all errors to null without reporting
     */
    OFF,

    /**
     * Convert all errors to null, and collect them into the `errors` field in the response object
     */
    ON,

    /**
     * Same as above, also collect stack traces of each error, for debugging purpose only
     */
    DEBUG,
}

data class RequestEntry(
    /**
     * Pipeline name
     */
    val pipeline: String = "",

    /**
     * Input data for the pipeline
     */
    val data: Map<String, Any?> = mapOf(),

    /**
     * True to check the type of each field, turn them into error if check fails
     * False to convert the field to required type, turn into error if the conversion fails
     */
    val validate: Boolean = false,

    /**
     * Error reporting mode
     */
    val error: ErrorReportingMode = ErrorReportingMode.ON
)

data class Request(val requests: List<RequestEntry> = listOf())

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class ErrorRecord(val row: Int, val column: String, val message: String, val trace: String = "")

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class ResponseEntry(
    /**
     * Requested pipeline name
     */
    val pipeline: String,
    /**
     * Status of this request, 'OK' if the request has been processed successfully, or other value if failed
     */
    val status: String,
    /**
     * Count of the rows in the output data set
     */
    val count: Int = 0,
    /**
     * Time to process this request, it doesn't contain the time to receive the request and send the response from/to the client
     */
    val time: Double = 0.0,
    /**
     * Result data set, may contain multiple rows, each row is a map of "field_name" to "field_value"
     */
    val data: List<Map<String, Any?>> = listOf(),
    /**
     * Collected errors in the result data et, this field may be omitted if there is no error
     */
    val errors: List<ErrorRecord> = listOf(),
)

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class Response(val results: List<ResponseEntry> = listOf())

