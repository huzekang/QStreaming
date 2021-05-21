/**注意：提交spark作业时要启动hive support **/
create batch output table outputTable using csv (path="/tmp/usql_t1") TBLPROPERTIES(saveMode="overwrite");

insert into  outputTable SELECT * from  kylin_country;