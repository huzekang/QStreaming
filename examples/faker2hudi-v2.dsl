
/**定义更新的数据**/
CREATE BATCH INPUT TABLE raw_user_09_03 (id integer,name string ,isMan boolean, salary double,ts long)
USING faker(data='
1,pk33,false,4322.3,1000
4,mm,true,123.3,2000
');

/**定义hudi输出表**/
create batch output table outputTable USING hudi(
  hoodie.datasource.write.precombine.field="ts",
  hoodie.datasource.write.recordkey.field="id",
  hoodie.upsert.shuffle.parallelism="2",
  hoodie.table.name="ods_user",
  path="file:///tmp/hudi/ods_user"
) TBLPROPERTIES(saveMode="append");

insert into  outputTable SELECT * from  raw_user_09_03;