CREATE BATCH INPUT TABLE raw_log USING parquet(path="file:///Volumes/Samsung_T5/dataset/product_reviews");

create batch output table outputTable USING hudi(
  hoodie.datasource.write.precombine.field="review_date",
  hoodie.datasource.write.recordkey.field="review_id",
  hoodie.datasource.write.partitionpath.field="year",
  hoodie.upsert.shuffle.parallelism="2",
  hoodie.table.name="outputTable",
  path="file:///tmp/hudi/outputTable"
) TBLPROPERTIES(saveMode="append");

insert into  outputTable SELECT * from  raw_log;