grammar ExpressionOnly;

expr
    : unary_expr
    | expr 'is' 'null'
    | expr 'is' 'not' 'null'
    | expr ('*'|'/'|'%'|'and') expr
    | expr ('+'|'-'|'or') expr
    | expr ('>'|'<'|'>='|'<='|'!='|'<>') expr
    | expr '[' expr ']'
    | case_expr
    | function
    | dot_member
    | number
    | str
    | bool
    | '(' expr ')'
    ;

case_expr
    : 'case' when_then (when_then)* else_then 'end'
    ;

when_then
    : 'when' expr 'then' expr
    ;

else_then
    : 'else' expr
    ;

unary_expr
    : ('not' | '+' | '-') expr
    ;

expr_list
    : expr (',' expr)*
    ;

function
    : func_name '(' expr_list? ')'
    ;

dot_member
    : col_name ('.' ID)*;

number
    : (FLOAT | DEC | HEX | BIN | CONSTANTS)
    ;

str
    : STRING_LIT
    ;

bool
    : BOOL_LIT
    ;

// We use separated rule as these 2 kinds of identifiers need to be collected
func_name : ID;
col_name: ID;

CONSTANTS
    : 'PI' | 'E'
    ;

TYPES
    : 'int' | 'long' | 'datetime' | 'float' | 'double' | 'string' | 'array' | 'object' | 'dynamic'
    ;

STRING_LIT : '"' (~('"' | '\\' | '\r' | '\n') | ('\\' ('"' | '\\' | 'r' | 'n' | 't')))* '"'
    ;

BOOL_LIT
    : 'true' | 'false'
    ;

ID
    : [a-zA-Z][a-zA-Z0-9_]*
    ;

FLOAT   : DIGIT+ '.' DIGIT*
        | '.' DIGIT+
        ;
DEC     : DIGIT+;
HEX     : '0' [xX] ([A-Fa-f] | DIGIT)+ ;
BIN     : '0' [bB] [01]+ ;

WS  : [ \t\r\n]+ -> skip ;    // toss out whitespace

LINE_COMMENT
    : '#' ~[\r\n]* -> channel(HIDDEN)
    ;

fragment DIGIT  : [0-9];
