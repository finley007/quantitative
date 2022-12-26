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
created_time datetime,
modified_time datetime,
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
record_count int,
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);
alter table stock_validation_result add unique (validation_code, tscode, date);
alter table stock_validation_result add index stock_validation_result_index (result);

-- 期货数据处理结果表
CREATE TABLE IF NOT EXISTS future_process_record
(
id VARCHAR(40),
process_code VARCHAR(40),
instrument  VARCHAR(10),
date    VARCHAR(10),
status  int comment '0-已生成，1-已归并',
err_msg  VARCHAR(1024),
created_time datetime,
modified_time datetime,
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
created_time datetime,
modified_time datetime,
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
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);

-- 股指成分股表
CREATE TABLE IF NOT EXISTS index_constituent_config
(
id VARCHAR(40),
product VARCHAR(2),
date    VARCHAR(10),
tscode  VARCHAR(10),
status  int comment '0-正常，1-停牌',
created_time datetime,
modified_time datetime,
PRIMARY KEY(id),
UNIQUE (product, date, tscode)
);

-- 因子表
CREATE TABLE IF NOT EXISTS factor_config
(
code VARCHAR(200),
type  VARCHAR(10),
type_name VARCHAR(100),
number VARCHAR(10),
name    VARCHAR(100),
parameter  VARCHAR(100),
version  VARCHAR(10),
source  VARCHAR(20),
created_time datetime,
modified_time datetime,
PRIMARY KEY(code, version)
);

-- 因子操作表
CREATE TABLE IF NOT EXISTS factor_operation_history
(
id VARCHAR(40),
target_factor VARCHAR(2000),
operation int comment '1-生成因子文件, 2-生成统计信息，3-合并因子文件',
status  int comment '0-成功，1-失败',
time_cost long,
err_msg  VARCHAR(1024),
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);