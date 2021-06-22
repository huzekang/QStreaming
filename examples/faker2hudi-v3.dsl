
/**定义删除的数据**/
CREATE BATCH INPUT TABLE raw_user_09_03 (id integer,name string ,isMan boolean, salary double,ts long)
USING faker(data='
4,mm,true,123.3,2000
');

/**定义hudi输出表**/
create batch output table outputTable USING hudi(
  hoodie.datasource.write.operation="delete",
  hoodie.datasource.write.precombine.field="ts",
  hoodie.datasource.write.recordkey.field="id",
  hoodie.table.name="ods_user",
  path="file:///tmp/hudi/ods_user"
) TBLPROPERTIES(saveMode="append");

insert into  outputTable SELECT * from  raw_user_09_03;