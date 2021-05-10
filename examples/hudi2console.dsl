create batch input table raw_log using hudi(path="file:///tmp/hudi/outputTable/*/*") ;

create batch output table outputTable using console;

@original desc raw_log;

@original select * from  raw_log limit 10;

insert into  outputTable SELECT count(*) from  raw_log;