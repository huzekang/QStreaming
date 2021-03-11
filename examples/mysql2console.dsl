CREATE BATCH INPUT TABLE raw_log USING jdbc(url="jdbc:mysql://localhost/test",user="root",password="eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT",driver="com.mysql.jdbc.Driver",dbTable="yunfu_monitor");


create BATCH output table outputTable using console;

create view v_request_log with(coalesce=1) as SELECT count("性别") as cnts,"性别"  from  raw_log group by "性别";

insert into  outputTable select * from v_request_log;