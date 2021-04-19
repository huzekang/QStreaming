/**定义流表**/
create stream input table raw_log(
 type STRING,
 username STRING
) using kafka(
  kafka.bootstrap.servers="10.93.6.167:6667",
  startingoffsets='{"t1":{"0":0}}',
  subscribe="t1",
  "group-id"="qstream-1"
) row format json;


/**定义结果表**/
create stream output table outputtable using console;

insert into  outputtable
select * from  raw_log ;