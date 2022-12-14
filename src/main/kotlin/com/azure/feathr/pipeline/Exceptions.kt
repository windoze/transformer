package com.azure.feathr.pipeline

open class TransformExternalException(message: String): Exception(message)

class HttpError(statusCode: Int, message: String): TransformExternalException("HTTP Error: $statusCode $message")

open class TransformerException(message: String) : Exception(message)

class TypeMismatch(message: String) : TransformerException(message)

class ColumnNotFound(column: String): TransformerException("Column '$column' is not found")

class IllegalType(type: ColumnType): TransformerException("Illegal type $type usage")

class IllegalValue(v: Any?): TransformerException("Value $v is illegal")

class KeyNotFound(k: String): TransformerException("Key '$k' is not found")

class IndexOutOfBound(idx: Int?): TransformerException ("Index $idx is out of bound")

class InvalidReference(name: String): TransformerException("Invalid reference to '$name'")

class IllegalArguments(message: String): TransformerException(message)

class InvalidDateFormatException(format: String, message: Any): TransformerException("$format: $message")

class LookupSourceError(private val src: Throwable) : TransformerException("$src")

class UnsupportedFeatureType(message: String): TransformerException(message)

class ArityError(message: String): TransformerException(message)