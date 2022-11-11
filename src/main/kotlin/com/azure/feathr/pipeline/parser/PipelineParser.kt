package com.azure.feathr.pipeline.parser

import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.TerminalNode
import com.azure.feathr.pipeline.*
import com.azure.feathr.pipeline.operators.*
import com.azure.feathr.pipeline.parser.PipelineDefinitionParser.*
import com.azure.feathr.pipeline.transformations.*

class PipelineParser {
    // Parse PT into Pipelines
    fun parse(src: String): Map<String, Pipeline> {
        val inputStream = CharStreams.fromString(src)
        val lexer = com.azure.feathr.pipeline.parser.PipelineDefinitionLexer(inputStream)
        val parser = com.azure.feathr.pipeline.parser.PipelineDefinitionParser(CommonTokenStream(lexer))
        return parser.prog().pipeline().associate { pipeline ->
            parsePipeline(pipeline)
        }
    }

    private fun parsePipeline(ctx: PipelineContext): Pair<String, Pipeline> {
        val name = ctx.ID().symbol.text!!
        val schema = parseSchema(ctx.schema())
        val transformations = ctx.transformation().map {
            parseTransformation(it)
        }
        return Pair(name, Pipeline(schema, transformations))
    }

    private fun parseSchema(ctx: SchemaContext): List<Column> {
        return ctx.column_list().ID().zip(ctx.column_list().TYPES()).map { (id, type) ->
            Column(id.symbol.text, ColumnType.valueOf(type.symbol.text!!.uppercase()))
        }
    }

    private fun parseTransformation(ctx: TransformationContext): Transformation {
        when (val t = ctx.children[0]) {
            is Explode_tranContext -> return Explode(
                t.ID().text,
                if (t.TYPES()?.text.isNullOrBlank()) ColumnType.DYNAMIC else parseType(t.TYPES())
            )

            is Where_tranContext -> return Where(parseExpression(t.expr()))

            is Project_tranContext -> return Project(t.ID().zip(t.expr()).map { (id, expr) ->
                id.text to parseExpression(expr)
            })

            is Project_rename_tranContext -> {
                return ProjectRename(t.ID().chunked(2).associate { (new, old) -> new.text to old.text })
            }

            is Project_remove_tranContext -> {
                return ProjectRemove(t.ID().map { it.text })
            }

            is Lookup_tranContext -> {
                val columns = t.ID().take(t.ID().size - 1).map { it.text }
                val srcName = t.ID().last().text
                val expr = parseExpression(t.expr())
                return Lookup(columns, expr, srcName)
            }

            is Take_tranContext -> return Take(parseInteger(t.number()).toInt())

            is Top_tranContext -> {
                val n = parseInteger(t.number()).toInt()
                val expr = parseExpression(t.expr())
                val asc = t.sort_dir().text == "asc"
                return Top(expr, asc, n)
            }

            else -> {
                TODO()
            }
        }
    }

    private fun parseExpression(ctx: ExprContext): Expression {
        return when (val t = ctx.children[0]) {
            is Unary_exprContext -> parseUnaryExpression(t)
            is Dot_memberContext -> parseDotMember(t)
            is FunctionContext -> parseFunction(t)
            is NumberContext -> parseNumber(t)
            is StrContext -> parseString(t)
            is BoolContext -> parseBool(t)
            else -> {
                // ( expr )
                if (ctx.childCount == 3 && ctx.getChild(0).text == "(") {
                    return parseExpression(ctx.expr(0))
                }
                // expr is [not] null
                if (ctx.childCount >= 3 && ctx.getChild(1).text == "is" && ctx.children.last().text == "null") {
                    val expr = parseExpression(ctx.expr(0))
                    val op: Operator = if (ctx.childCount == 3) IsNull() else IsNotNull()
                    return OperatorExpression(op, listOf(expr))
                }
                // expr [ expr ]
                if (ctx.childCount == 4 && ctx.getChild(1).text == "[" && ctx.getChild(3).text == "]") {
                    return OperatorExpression(ArrayIndex(), ctx.expr().map { parseExpression(it) })
                }
                // expr +-*/... expr
                if (ctx.childCount == 3) {
                    val l = parseExpression(ctx.expr(0))
                    val r = parseExpression(ctx.expr(1))
                    val op = when (ctx.getChild(1).text) {
                        "+" -> Plus()
                        "-" -> Minus()
                        "*" -> Multiply()
                        "/" -> Divide()
                        "%" -> Modular()
                        ">" -> GreaterThan()
                        "<" -> LessThan()
                        ">=" -> GreaterEqual()
                        "<=" -> LessEqual()
                        "==" -> Equal()
                        "!=" -> NotEqual()
                        "<>" -> NotEqual()
                        "and" -> And()
                        "or" -> Or()

                        else -> TODO()
                    }
                    return OperatorExpression(op, listOf(l, r))
                }
                TODO()
            }
        }
    }

    private fun parseFunction(ctx: FunctionContext): Expression {
        val op = FunctionCall(ctx.ID().text)
        val arguments = ctx.expr_list().expr().map { parseExpression(it) }
        return OperatorExpression(op, arguments)
    }

    private fun parseDotMember(ctx: Dot_memberContext): Expression {
        val init: Expression = GetColumn(ctx.ID().first().text)
        return ctx.ID().subList(1, ctx.ID().size).fold(init) { exp, id ->
            OperatorExpression(MapIndex(), listOf(exp, ConstantExpression(id.text, ColumnType.STRING)))
        }
    }

    private fun parseUnaryExpression(ctx: Unary_exprContext): Expression {
        val expr = parseExpression(ctx.expr())
        return when (ctx.children[0].text) {
            "not" -> {
                OperatorExpression(Not(), listOf(expr))
            }

            "+" -> {
                expr
            }

            "-" -> {
                TODO()
            }

            else -> {
                TODO()
            }
        }
    }

    private fun parseNumber(ctx: NumberContext): ConstantExpression {
        val text = ctx.getChild(0).text
        // Try int/long
        try {
            return ConstantExpression(text.toLong(), ColumnType.LONG)
        } catch (e: java.lang.NumberFormatException) {
            // pass
        }
        // Try float/double
        try {
            return ConstantExpression(text.toDouble(), ColumnType.DOUBLE)
        } catch (e: java.lang.NumberFormatException) {
            // pass
        }

        TODO()
    }

    private fun parseString(ctx: StrContext): ConstantExpression {
        val rawText = ctx.getChild(0).text
        val s = rawText
            .substring(1, rawText.length - 1)
            .replace("\\r", "\r")
            .replace("\\n", "\n")
            .replace("\\t", "\t")
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
        return ConstantExpression(s, ColumnType.STRING)
    }

    private fun parseBool(ctx: BoolContext): ConstantExpression {
        val rawText = ctx.getChild(0).text
        return ConstantExpression(rawText == "true", ColumnType.BOOL)
    }

    private fun parseInteger(ctx: NumberContext): Long {
        val text = ctx.getChild(0).text
        // Try int/long
        try {
            return text.toLong()
        } catch (e: java.lang.NumberFormatException) {
            // pass
        }
        // Try float/double
        try {
            return text.toDouble().toLong()
        } catch (e: java.lang.NumberFormatException) {
            // pass
        }

        TODO()
    }

    private fun parseType(ctx: TerminalNode): ColumnType {
        return ColumnType.valueOf(ctx.text.uppercase())
    }
}