create batch input table raw_log using es
(resource='upuptop/stu2',  nodes= 'localhost',port='9200');

create batch output table dogs
using es (resource='index/dogs', nodes= 'localhost', port='9200');

insert into  dogs SELECT * from  raw_log;