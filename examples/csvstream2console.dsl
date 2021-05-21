/**定义流表**/
create stream input table raw_log(
 id STRING,
 username STRING,
 salary STRING,
 age STRING
) using csv(
  path="file:///Users/huzekang/study/bigdata-spark/data/csv"
);


/**定义结果表**/
create stream output table outputtable using console;

insert into  outputtable
select * from  raw_log ;