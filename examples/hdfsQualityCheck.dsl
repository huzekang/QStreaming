
CREATE BATCH INPUT TABLE raw_log USING parquet(path="file:////Volumes/Samsung_T5/dataset/product_reviews");

create BATCH output table testResult1 using console;
create BATCH output table testResult2 using console;

CREATE CHECK TEST_SIZE (testLevel='Error',testOutput='testResult1') ON raw_log WITH numRows()=3120938;

CREATE CHECK test_id (testLevel='Error',testOutput='testResult2') on raw_log WITH isUnique(id) ;
