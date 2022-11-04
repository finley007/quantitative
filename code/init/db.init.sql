-- stock
CREATE DATABASE IF NOT EXISTS quantitative_transaction;
use quantitative_transaction;

-- test 测试表
CREATE TABLE IF NOT EXISTS test
(
varchar_column VARCHAR(10),
int_column  int(10),
date_column  date,
datetime_column  datetime,
time_column  time,
PRIMARY KEY(varchar_column)
);

-- 股票数据检查结果表
CREATE TABLE IF NOT EXISTS stock_validation_result
(
id VARCHAR(32),
validation_code VARCHAR(32),
tscode  VARCHAR(10),
date    VARCHAR(10),
result  int comment '0-成功，1-失败',
err_msg  VARCHAR(1024),
PRIMARY KEY(id)
);