/**定义流表**/
CREATE Stream INPUT TABLE raw_log USING socket(host="hadoop001",port="9527");

/**定义维表**/
CREATE BATCH INPUT TABLE dim_customer USING jdbc(url="jdbc:mysql://localhost/test?useSSL=false",user="root",password="eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT",driver="com.mysql.jdbc.Driver",dbTable="customer");

/**定义结果表**/
create Stream output table outputTable using console;

insert into  outputTable SELECT * from  raw_log a join dim_customer b on a.value = b.customerId;