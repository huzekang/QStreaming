/**定义流表**/
create stream input table raw_log(
 `schema` string,
 `table` string,
 `type` string,
 `ts` long,
 `opTime` long,
 `before_create_time` string,
 `before_diag_code` string,
 `before_diag_name` string,
 `before_id` string,
 `after_create_time` string,
 `after_diag_code` string,
 `after_diag_name` string,
 `after_id` string
) using kafka(
  'kafka.bootstrap.servers'="hadoop004:6667",
  'subscribe'="blood_diag_up_01",
  'startingOffsets'="earliest",
  'minPartitions'=10
) row format json;

create view v_log with(coalesce=1) as  select * ,concat_ws("-",partition,offset) as pk from raw_log;

/**定义结果表**/
create stream output table outputtable using console;

insert into  outputtable
select * from  v_log ;