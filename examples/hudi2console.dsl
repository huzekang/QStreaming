create batch input table raw_log using hudi(path="file:///tmp/hudi/outputTable/*/*") ;

create batch output table outputTable using console;

desc raw_log;

select * from  raw_log limit 10;

insert into  outputTable SELECT count(*) from  raw_log;