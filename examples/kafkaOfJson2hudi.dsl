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
  kafka.bootstrap.servers="hadoop004:6667",
  subscribe="blood_diag_up_02",
  startingOffsets="earliest",
  minPartitions=10
) row format json;

/**改变分区 **/
create view v_log with(coalesce=5) as  select * ,concat_ws("-",partition,offset) as pk from raw_log;

/**定义结果表**/
create stream output table outputTable USING hudi(
  'hoodie.datasource.write.precombine.field'="opTime",
  'hoodie.datasource.write.recordkey.field'="pk",
  'hoodie.upsert.shuffle.parallelism'="8",
  'hoodie.table.name'="outputTable",
  'path'="file:///tmp/hudi/outputTable",
  'checkpointLocation'="file:///tmp/hudi/chk2"
) TBLPROPERTIES(saveMode="append");

insert into  outputTable
select * from  v_log ;