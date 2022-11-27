# Generated from ExpressionOnly.g4 by ANTLR 4.11.1
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .ExpressionOnlyParser import ExpressionOnlyParser
else:
    from ExpressionOnlyParser import ExpressionOnlyParser

# This class defines a complete listener for a parse tree produced by ExpressionOnlyParser.
class ExpressionOnlyListener(ParseTreeListener):

    # Enter a parse tree produced by ExpressionOnlyParser#expr.
    def enterExpr(self, ctx:ExpressionOnlyParser.ExprContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#expr.
    def exitExpr(self, ctx:ExpressionOnlyParser.ExprContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#case_expr.
    def enterCase_expr(self, ctx:ExpressionOnlyParser.Case_exprContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#case_expr.
    def exitCase_expr(self, ctx:ExpressionOnlyParser.Case_exprContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#when_then.
    def enterWhen_then(self, ctx:ExpressionOnlyParser.When_thenContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#when_then.
    def exitWhen_then(self, ctx:ExpressionOnlyParser.When_thenContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#else_then.
    def enterElse_then(self, ctx:ExpressionOnlyParser.Else_thenContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#else_then.
    def exitElse_then(self, ctx:ExpressionOnlyParser.Else_thenContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#unary_expr.
    def enterUnary_expr(self, ctx:ExpressionOnlyParser.Unary_exprContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#unary_expr.
    def exitUnary_expr(self, ctx:ExpressionOnlyParser.Unary_exprContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#expr_list.
    def enterExpr_list(self, ctx:ExpressionOnlyParser.Expr_listContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#expr_list.
    def exitExpr_list(self, ctx:ExpressionOnlyParser.Expr_listContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#function.
    def enterFunction(self, ctx:ExpressionOnlyParser.FunctionContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#function.
    def exitFunction(self, ctx:ExpressionOnlyParser.FunctionContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#dot_member.
    def enterDot_member(self, ctx:ExpressionOnlyParser.Dot_memberContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#dot_member.
    def exitDot_member(self, ctx:ExpressionOnlyParser.Dot_memberContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#number.
    def enterNumber(self, ctx:ExpressionOnlyParser.NumberContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#number.
    def exitNumber(self, ctx:ExpressionOnlyParser.NumberContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#str.
    def enterStr(self, ctx:ExpressionOnlyParser.StrContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#str.
    def exitStr(self, ctx:ExpressionOnlyParser.StrContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#bool.
    def enterBool(self, ctx:ExpressionOnlyParser.BoolContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#bool.
    def exitBool(self, ctx:ExpressionOnlyParser.BoolContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#func_name.
    def enterFunc_name(self, ctx:ExpressionOnlyParser.Func_nameContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#func_name.
    def exitFunc_name(self, ctx:ExpressionOnlyParser.Func_nameContext):
        pass


    # Enter a parse tree produced by ExpressionOnlyParser#col_name.
    def enterCol_name(self, ctx:ExpressionOnlyParser.Col_nameContext):
        pass

    # Exit a parse tree produced by ExpressionOnlyParser#col_name.
    def exitCol_name(self, ctx:ExpressionOnlyParser.Col_nameContext):
        pass



del ExpressionOnlyParser