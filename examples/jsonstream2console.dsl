/**定义流表**/
create stream input table raw_log(
 time STRING,
 domain STRING,
 traffic STRING
) using json(
  path="file:///Users/huzekang/study/bigdata-spark/data/json"
);


/**定义结果表**/
create stream output table outputtable using console;

insert into  outputtable
select * from  raw_log ;