/**注意：提交spark作业时要启动hive support **/
create batch output table outputTable using console;

insert into  outputTable SELECT * from  kylin_country;