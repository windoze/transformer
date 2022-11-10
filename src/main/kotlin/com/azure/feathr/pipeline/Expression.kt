package com.azure.feathr.pipeline

import com.azure.feathr.pipeline.operators.FunctionCall
import com.azure.feathr.pipeline.operators.Operator

interface Expression : Initializable {
    fun getResultType(columnTypes: List<ColumnType>): ColumnType
    fun evaluate(input: Row): Value

    fun dump(): String
}

class GetColumn(private val columnName: String) : Expression {
    var index: Int = -1

    override fun initialize(columns: List<Column>) {
        index = columns.indexOfFirst { it.name == columnName }
        if (index < 0) throw InvalidReference(columnName)
    }

    override fun getResultType(columnTypes: List<ColumnType>): ColumnType {
        return columnTypes[index]
    }

    override fun evaluate(input: Row): Value {
        return input.getColumn(index)
    }

    override fun dump(): String {
        return columnName
    }
}

class GetColumnByIndex(private val index: Int) : Expression {
    override fun initialize(columns: List<Column>) {
        if (index >= columns.size)
            throw InvalidReference("column$index")
    }

    override fun getResultType(columnTypes: List<ColumnType>): ColumnType {
        return columnTypes[index]
    }

    override fun evaluate(input: Row): Value {
        return input.getColumn(index)
    }

    override fun dump(): String {
        return ("row[$index]")
    }
}

class ConstantExpression(private val constant: Any?, private val type: ColumnType) : Expression {
    override fun getResultType(columnTypes: List<ColumnType>): ColumnType {
        return type
    }

    override fun evaluate(input: Row): Value {
        return Value(type, constant)
    }

    override fun dump(): String {
        if (constant is String) return "\"$constant\""     // TODO: Escape
        return constant.toString()
    }
}

class OperatorExpression(private val op: Operator, private val arguments: List<Expression>) : Expression {
    override fun getResultType(columnTypes: List<ColumnType>): ColumnType {
        return op.getResultType(arguments.map {
            it.getResultType(columnTypes)
        })
    }

    override fun evaluate(input: Row): Value {
        return try {
            op.apply(arguments.map {
                it.evaluate(input)
            })
        } catch (e: Throwable) {
            Value(ColumnType.DYNAMIC, null)
        }
    }

    override fun initialize(columns: List<Column>) {
        if ((op.arity >= 0) && op.arity != arguments.size)
            throw IllegalArguments("The number of arguments is incorrect")
        op.initialize(columns)
        arguments.forEach {
            it.initialize(columns)
        }
    }

    override fun dump(): String {
        return op.dump(arguments.map { it.dump() })
    }
}