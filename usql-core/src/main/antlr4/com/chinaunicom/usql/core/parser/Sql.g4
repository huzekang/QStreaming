

grammar Sql;


@members {
  /**
   * When false, INTERSECT is given the greater precedence over the other set
   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
   */
  public boolean legacy_setops_precedence_enbled = false;

  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

sql
    : (dslStatement ';' )+
    ;

dslStatement
    : createTestStatement
    | createSourceTableStatement
    | createSinkTableStatement
    | createFunctionStatement
    | createViewStatement
    | insertStatement
    | commentStatement
    | sqlStatement
    ;



constraint
    :  'numRows()'     assertion                                                                                              #sizeConstraint
    |  'isUnique'  '(' column+=identifier (',' column+=identifier)* ')'  assertion?                                           #uniqueConstraint
    |  'isNotNull' '(' column=identifier ')'    assertion?                                                                    #completeConstraint
    |  'containsUrl' '(' column=identifier  ')'   assertion?                                                                  #containsUrlConstraint
    |  'containsEmail' '(' column=identifier  ')'  assertion?                                                                 #containsEmailConstraint
    |  'isContainedIn' '(' column=identifier ',' '[' value+=STRING (','  value+=STRING) ']'')' assertion?                     #containedInConstraint
    |  'isNonNegative'  '(' column=identifier  ')'    assertion?                                                              #isNonNegativeConstraint
    |  'isPositive'  '(' column=identifier  ')'    assertion?                                                                 #isPositiveConstraint
    |  'satisfy' '(' predicate=STRING ',' desc=STRING ')'      assertion?                                                     #satisfyConstraint
    |  'hasDataType' '(' column=identifier ',' dataType= ('NULL'|'INT'|'BOOL'|'FRACTIONAL'|'TEXT'|'NUMERIC')  ')' assertion?  #dataTypeConstraint
    |  'hasMinLength' '(' column=identifier ',' length=INTEGER_VALUE ')'  assertion?                                          #hasMinLengthConstraint
    |  'hasMaxLength' '(' column=identifier ',' length=INTEGER_VALUE ')'  assertion?                                          #hasMaxLengthConstraint
    |  'hasMin' '(' column=identifier ',' value = DECIMAL_VALUE ')'  assertion?                                               #hasMinConstraint
    |  'hasMax' '(' column=identifier ',' value = DECIMAL_VALUE ')'  assertion?                                               #hasMaxConstraint
    |  'hasSum' '(' column=identifier ',' value = DECIMAL_VALUE ')'  assertion?                                               #hasSumConstraint
    |  'hasMean' '(' column=identifier ',' value = DECIMAL_VALUE ')'  assertion?                                              #hasMeanConstraint
    |  'hasPattern' '('  column=identifier ',' pattern=STRING ')'     assertion?                                              #patternConstraint
    |  'hasDateFormat' '(' column=identifier ',' formatString=STRING ')'   assertion?                                         #dateFormatConstraint
    |  'hasApproxQuantile'  '(' column=identifier ',' quantile=DECIMAL_VALUE ')' assertion                                    #approxQuantileConstraint
    |  'hasApproxCountDistinct' '(' column=identifier  ')'  assertion                                                         #approxCountDistinctConstraint
    ;

assertion
    :assertionOperator value = (INTEGER_VALUE|DECIMAL_VALUE)
    ;

assertionOperator
    : ('>'|'<'|'>='|'<='|'==' |'!=' |'=')
    ;

createFunctionStatement
    : functionDataType? K_CREATE K_FUNCTION funcName=IDENTIFIER ( '(' funcParam  (',' funcParam)* ')')? '=' funcBody= statementBody
    | 'def' funcName=IDENTIFIER ( '(' funcParam  (',' funcParam)* ')')? '=' funcBody=statementBody
    ;

functionDataType
   : '@type(' structField (',' structField)* ')'
   ;

structField
   : STRING ':' fieldType
   ;

statementBody
    : '{' ~(';' )+ '}'
    |  ~(';' )+
    ;

funcParam
    : IDENTIFIER ':' ('String'|'Boolean'|'Int'|'Long'|'Double'|'BigInt'|'BigDecimal')
    ;

createSourceTableStatement
    : K_CREATE streamTable=(K_STREAM| K_BATCH) K_INPUT K_TABLE tableIdentifier schemaSpec? K_USING connectorSpec formatSpec? tableProperties?
    ;


createSinkTableStatement
    : K_CREATE streamTable=(K_STREAM| K_BATCH) K_OUTPUT K_TABLE tableIdentifier schemaSpec? K_USING (connectorSpec (',' connectorSpec)* ) formatSpec? partitionSpec? bucketSpec ? tableProperties?
    ;

createTestStatement
    : K_CREATE K_CHECK testName=identifier ('(' property (',' property)* ')')?  K_ON testDataset=tableIdentifier K_WITH  constraint (K_AND constraint)*
    ;

partitionSpec
    : K_PARTITIONED K_BY '(' columns+=IDENTIFIER (',' columns+=IDENTIFIER)* ')'
    ;

bucketSpec
    : K_CLUSTERED K_BY '(' columns+=IDENTIFIER (',' columns+=IDENTIFIER)* ')' K_INTO bucketNum=INTEGER_VALUE K_BUCKETS
    ;

tableProperties
    :  K_TBLPROPERTIES '(' property (',' property)* ')'
    ;

connectorSpec
    : connectorType=tableProvider ('(' connectProps+=property (','  connectProps+=property)* ')' )?
    ;

schemaSpec
    : '(' schemaField (',' schemaField)* (',' timeField )? ')'
    ;

formatSpec
    : K_ROW K_FORMAT rowFormat ( '(' property (',' property)* ')')?
    ;


timeField
    : fieldName=identifier K_AS 'PROCTIME()'                                                         #procTime
    | eventTime=identifier K_AS 'ROWTIME(' fromField=identifier ',' delayThreadsHold=identifier  ')' #rowTime
    ;

rowFormat
    :K_TEXT|K_AVRO|K_JSON|K_CSV|K_REGEX
    ;

schemaField
    : fieldName=identifier fieldType (K_COMMENT STRING)?
    ;

fieldType
    : K_INTEGER|K_LONG|K_TINYINT|K_SMALLINT|K_STRING|K_TIMESTAMP|K_DATE|K_TIME|K_DATETIME|K_BOOLEAN|K_DOUBLE|K_FLOAT|K_SHORT|K_BYTE|K_VARCHAR|( K_DECIMAL'(' precision=INTEGER_VALUE ','  scale=INTEGER_VALUE ')')
    ;

insertStatement
    : K_INSERT K_INTO tableIdentifier  selectStatement
    ;

createViewStatement
    : K_CREATE (K_OR K_REPLACE)? (K_GLOBAL ? K_TEMPORARY )?  K_VIEW viewName=tableIdentifier (K_WITH '('property ( ',' property )* ')' )? K_AS selectStatement
    ;

selectStatement
    : K_SELECT  ~(';' )+
    ;

property
    : propertyKey K_EQ propertyValue
    ;

propertyKey
    : identifier ('.' identifier)*
    | STRING
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | STRING
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : strictIdentifier
    ;

strictIdentifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

tableIdentifier
    : (db=identifier '.')? table=identifier
    ;

booleanValue
    : ('true' |'TRUE') | ('false' | 'FALSE')
    ;


//comment goes here
commentStatement
    : SIMPLE_COMMENT| BRACKETED_COMMENT | BRACKETED_EMPTY_COMMENT
    ;

sqlStatement
    :   ~(';' )+
    ;

tableProvider
    : multipartIdentifier
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier ('.' parts+=errorCapturingIdentifier)*
    ;

// this rule is used for explicitly capturing wrong identifiers such as test-table, which should actually be `test-table`
// replace identifier with errorCapturingIdentifier where the immediate follow symbol is not an expression, otherwise
// valid expressions such as "a-b" can be recognized as an identifier
errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : ('-' identifier)+    #errorIdent
    |                        #realIdent
    ;

//keyword goes here
K_CLUSTERED: C L U S T E R E D;
K_GLOBAL: G L O B A L;
K_TEMPORARY: T E M P;
K_PERSISTED: P E R S I S T E D;
K_BY: B Y ;
K_PARTITIONED: P A R T I T I O N E D ;
K_BUCKETS: B U C K E T S ;
K_AS: A S;
K_SELECT: S E L E C T;
K_INTO: I N T O;
K_CREATE: C R E A T E;
K_INSERT: I N S E R T;
K_VIEW: V I E W;
K_TABLE: T A B L E;
K_WITH: W I T H;
K_OPTIONS: O P T I O N S;
K_STREAM: S T R E A M;
K_BATCH: B A T C H;
K_INPUT: I N P U T;
K_OUTPUT: O U T P U T;
K_USING: U S I N G;
K_ROW: R O W;
K_FORMAT: F O R M A T ;
K_EQ: '=';
K_SET: S E T;
K_FUNCTION: F U N C T I O N;
K_COMMENT: C O M M E N T ;
K_TEST: T E S T;
K_CHECK: C H E C K;
K_OR: O R;
K_REPLACE: R E P L A C E;
K_AND: A N D;
K_ON: O N;

K_TBLPROPERTIES: T B L P R O P E R T I E S;
K_TEXT: T E X T;
K_AVRO: A V R O;
K_JSON: J S O N;
K_CSV: C S V;
K_REGEX: R E G E X;

K_INTEGER: I N T E G E R;
K_LONG :  L O N G;
K_TINYINT: T I N Y I N T;
K_SMALLINT: S M A L L I N T;
K_STRING: S T R I N G;
K_TIMESTAMP: T I M E S T A M P;
K_DATE: D A T E;
K_TIME: T I M E;
K_DATETIME: D A T E T I M E;
K_BOOLEAN: B O O L E A N;
K_DOUBLE: D O U B L E;
K_FLOAT: F L O A T;
K_SHORT: S H O R T;
K_BYTE: B Y T E;
K_VARCHAR: V A R C H A R;
K_DECIMAL: D E C I M A L;



STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z]
    ;

//a-z case insensitive
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_EMPTY_COMMENT
    : '/**/' -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' ~[+] .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;