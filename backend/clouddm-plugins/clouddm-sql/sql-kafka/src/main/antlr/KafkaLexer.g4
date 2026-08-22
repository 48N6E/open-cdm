/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

lexer grammar KafkaLexer;

WS      : [ \t\r\n]+ -> channel(HIDDEN);
SEMI    : ';';
SHOW    : [sS][hH][oO][wW];
DESCRIBE: [dD][eE][sS][cC][rR][iI][bB][eE];
ALTER   : [aA][lL][tT][eE][rR];
DELETE  : [dD][eE][lL][eE][tT][eE];
CONSUME : [cC][oO][nN][sS][uU][mM][eE];
TOPICS  : [tT][oO][pP][iI][cC][sS];
GROUPS  : [gG][rR][oO][uU][pP][sS];
GROUP   : [gG][rR][oO][uU][pP];
TOPIC   : [tT][oO][pP][iI][cC];
CLUSTER : [cC][lL][uU][sS][tT][eE][rR];
INFO    : [iI][nN][fF][oO];
SET     : [sS][eE][tT];
ADD     : [aA][dD][dD];
RESET   : [rR][eE][sS][eE][tT];
OFFSET  : [oO][fF][fF][sS][eE][tT];
TO      : [tT][oO];
TIMESTAMP: [tT][iI][mM][eE][sS][tT][aA][mM][pP];
PARTITIONS: [pP][aA][rR][tT][iI][tT][iI][oO][nN][sS];
FROM    : [fF][rR][oO][mM];
BEGINNING: [bB][eE][gG][iI][nN][nN][iI][nN][gG];
LATEST  : [lL][aA][tT][eE][sS][tT];
PARTITION: [pP][aA][rR][tT][iI][tT][iI][oO][nN];
LIMIT   : [lL][iI][mM][iI][tT];
EQ      : '=';
INTEGER : [0-9]+;
// Allow leading digits so unquoted topic names like 123abc still lex as one token (longest match beats INTEGER).
IDENT   : [A-Za-z0-9_][A-Za-z0-9._-]*;
STRING  : '""'   | ('"' (~["\r\n] | TRANS)* '"')
        | '\'\'' | '\'' (~['\r\n] | TRANS)* '\''
        ;
fragment TRANS  : '\\' (['"\\/bfnrt] | UNICODE);
fragment UNICODE: 'u' HEX HEX HEX HEX;
fragment HEX    : [0-9a-fA-F];
ARG     : '?';
