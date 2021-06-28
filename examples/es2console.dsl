create batch input table raw_log using es
(resource='upuptop/stu2',  nodes= 'localhost',port='9200');

create batch output table outputTable using console;

insert into  outputTable SELECT * from  raw_log;