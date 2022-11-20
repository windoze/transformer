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

class ConstantExpression(constant: Any?) : Expression {
    private val value: Value = Value(constant)
    override fun getResultType(columnTypes: List<ColumnType>): ColumnType {
        return value.getValueType()
    }

    override fun evaluate(input: Row): Value {
        return value
    }

    override fun dump(): String {
        if (value.getValueType() == ColumnType.STRING) return "\"${value.getString()}\""     // TODO: Escape
        return value.value.toString()
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
                val arg = it.evaluate(input)
                // Shortcut on error
                if (arg.isError()) return arg
                arg
            })
        } catch (e: TransformerException) {
            Value(e)
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