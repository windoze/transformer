grammar PipelineDefinition;

prog
    : pipeline+
    ;

pipeline
    : ID schema ('|' transformation)* ';'
    ;

schema
    :  '(' (column_list) ')'
    ;

column_list
    : ID 'as' TYPES (',' ID 'as' TYPES)*
    ;

transformation
    : where_tran
    | top_tran
    | project_tran
    | project_rename_tran
    | project_remove_tran
    | explode_tran
    | lookup_tran
    | take_tran
    ;

where_tran
    : 'where' expr;

top_tran
    : 'top' number 'by' expr sort_dir? nulls_pos?
    ;

project_tran
    : 'project' ID '=' expr (',' ID '=' expr)*
    ;

project_rename_tran
    : 'project-rename' ID '=' ID (',' ID '=' ID)*
    ;

project_remove_tran
    : 'project-remove' ID (',' ID)*
    ;

explode_tran
    : ('mv-expand' | 'explode') ID ('as' TYPES)?
    ;

lookup_tran
    : 'lookup' ID (',' ID)* 'from' ID 'on' expr
    ;

take_tran
    : 'take' number
    ;

sort_dir
    : 'asc' | 'desc'
    ;

nulls_pos
    : 'nulls' ('first' | 'last')
    ;

expr
    : unary_expr
    | expr 'is' 'null'
    | expr 'is' 'not' 'null'
    | expr ('*'|'/'|'%'|'and') expr
    | expr ('+'|'-'|'or') expr
    | expr ('>'|'<'|'>='|'<='|'!='|'<>') expr
    | expr '[' expr ']'
    | function
    | dot_member
    | number
    | str
    | '(' expr ')'
    ;

unary_expr
    : ('not' | '+' | '-') expr
    ;

expr_list
    : expr (',' expr)*
    ;

function
    : ID '(' expr_list? ')'
    ;

dot_member
    : ID ('.' ID)*;

number
    : (FLOAT | DEC | HEX | BIN)
    ;

str
    : STRING_LIT
    ;

TYPES
    : 'int' | 'long' | 'float' | 'double' | 'string' | 'array' | 'object' | 'dynamic'
    ;

STRING_LIT : '"' (~('"' | '\\' | '\r' | '\n') | ('\\' ('"' | '\\' | 'r' | 'n' | 't')))* '"'
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


fragment DIGIT  : [0-9];
