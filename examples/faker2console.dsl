CREATE BATCH INPUT TABLE raw_log (id integer,name string ,isMan boolean, salary double)
USING faker(data='
1,pk,false,4322.3
1,pk,false,4322.3
');

create batch output table outputTable using console;

insert into  outputTable SELECT * from  raw_log;