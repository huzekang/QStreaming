CREATE BATCH INPUT TABLE raw_log USING parquet(path="file:////Volumes/Samsung_T5/dataset/product_reviews");

create batch output table outputTable using console;

insert into  outputTable SELECT * from  raw_log;