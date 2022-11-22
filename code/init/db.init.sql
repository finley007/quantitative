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
id VARCHAR(40),
validation_code VARCHAR(40),
tscode  VARCHAR(10),
date    VARCHAR(10),
result  int comment '0-成功，1-失败',
err_msg  VARCHAR(1024),
PRIMARY KEY(id)
);

-- 期货数据处理结果表
CREATE TABLE IF NOT EXISTS future_process_record
(
id VARCHAR(40),
process_code VARCHAR(40),
instrument  VARCHAR(10),
date    VARCHAR(10),
status  int comment '0-已生成，1-已归并',
err_msg  VARCHAR(1024),
PRIMARY KEY(id)
);

-- 股票数据处理结果表
CREATE TABLE IF NOT EXISTS stock_process_record
(
id VARCHAR(40),
process_code VARCHAR(40),
tscode  VARCHAR(10),
date    VARCHAR(10),
status  int comment '0-合法，1-非法',
invalid_msg  VARCHAR(1024),
PRIMARY KEY(id)
);

-- 股指合约表
CREATE TABLE IF NOT EXISTS future_instrument_config
(
id VARCHAR(40),
product VARCHAR(2),
instrument  VARCHAR(8),
date    VARCHAR(10),
is_main  int comment '0-主力合约，1-非主力合约',
PRIMARY KEY(id)
);

-- 股指成分股表
CREATE TABLE IF NOT EXISTS index_constituent_config
(
id VARCHAR(40),
product VARCHAR(2),
date    VARCHAR(10),
tscode  VARCHAR(10),
PRIMARY KEY(id),
UNIQUE (product, date, tscode)
);