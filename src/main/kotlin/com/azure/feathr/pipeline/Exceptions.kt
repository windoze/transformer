package com.azure.feathr.pipeline

open class TransformerException(message: String) : Exception(message)

class TypeMismatch(message: String) : TransformerException(message)

class ColumnNotFound(column: String): TransformerException("Column '$column' is not found")

class IllegalType(type: ColumnType): TransformerException("Illegal type $type usage")

class IllegalValue(v: Any?): TransformerException("Value $v is illegal")

class KeyNotFound(k: String): TransformerException("Key '$k' is not found")

class IndexOutOfBound(idx: Int?): TransformerException ("Index $idx is out of bound")