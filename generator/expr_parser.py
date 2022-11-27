from antlr4 import *
from generated.ExpressionOnlyLexer import ExpressionOnlyLexer
from generated.ExpressionOnlyListener import ExpressionOnlyListener
from generated.ExpressionOnlyParser import ExpressionOnlyParser
import sys

class ExpressionPrintListener(ExpressionOnlyListener):
    def __init__(self, functions: set, columns: set):
        self.functions = functions
        self.columns = columns
    def enterFunc_name(self, ctx):
        self.functions.add(ctx.ID().getText())
    def enterCol_name(self, ctx):
        self.columns.add(ctx.ID().getText())

def collect(expr: str, functions: set, columns: set):
    lexer = ExpressionOnlyLexer(InputStream(expr))
    stream = CommonTokenStream(lexer)
    parser = ExpressionOnlyParser(stream)
    tree = parser.expr()
    printer = ExpressionPrintListener(functions, columns)
    walker = ParseTreeWalker()
    walker.walk(printer, tree)
    return functions, columns

if __name__ == '__main__':
    functions = set()
    columns = set()
    collect("a+b-2*3", functions, columns)
    collect("f(2+c)", functions, columns)
    print("Functions:", functions)
    print("Columns:", columns)
