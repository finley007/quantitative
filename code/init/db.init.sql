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
issue_count int,
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);
alter table stock_validation_result add unique (validation_code, tscode, date);
alter table stock_validation_result add index stock_validation_result_index (result);

-- 期货数据检查结果表
CREATE TABLE IF NOT EXISTS future_validation_result
(
id VARCHAR(40),
validation_code VARCHAR(40),
instrument  VARCHAR(10),
date    VARCHAR(10),
result  int comment '0-成功，1-失败',
err_msg  VARCHAR(1024),
record_count int,
issue_count int,
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);
alter table future_validation_result add unique (validation_code, instrument, date);
alter table future_validation_result add index future_validation_result_index (result);

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
status  int comment '0-正常，1-停牌，2-数据异常',
is_st   int comment '0-正常，1-st'
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

-- 缺失股票数据表
CREATE TABLE IF NOT EXISTS stock_missing_data
(
id VARCHAR(40),
date    VARCHAR(10),
tscode  VARCHAR(10),
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);

-- 因子处理表
CREATE TABLE IF NOT EXISTS factor_process_record
(
id VARCHAR(40),
process_code VARCHAR(40) comment '关联factor_operation_history表',
product  VARCHAR(2),
instrument  VARCHAR(10),
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);

-- 因子数据检查结果表
CREATE TABLE IF NOT EXISTS factor_validation_result
(
id VARCHAR(40),
validation_code VARCHAR(40),
instrument  VARCHAR(10),
date    VARCHAR(10),
result  int comment '0-成功，1-失败',
err_msg  VARCHAR(1024),
record_count int,
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);
alter table factor_validation_result add unique (validation_code, instrument, date);
alter table factor_validation_result add index factor_validation_result_index (result);

-- 复权因子表
CREATE TABLE IF NOT EXISTS stock_reversion_config
(
id VARCHAR(40),
date    VARCHAR(10),
tscode  VARCHAR(10),
value   DECIMAL(15,5),
type    int comment '0-后复权，1-前复权',
created_time datetime,
modified_time datetime,
PRIMARY KEY(id)
);


--初始化future_instrument_config，执行：factor_caculator->init_instrument_config
--初始化index_constituent_config，执行：tools->init_index_constituent_config, 再执行：tools->update_stock_suspension_status 更新停盘数据


insert into factor_config values ('FCT_01_001_WILLIAM','01','量价类','001','威廉因子','1000','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_01_002_CLOSE_MINUS_MOVING_AVERAGE','01','量价类','002','收盘价均值差','200|500|1000|1500','1.0','TSSB',SYSDATE(),SYSDATE());

insert into factor_config values ('FCT_AUXILIARY_FILE_GENERATION','02','现货类','002','辅助因子','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_002_10_GRADE_COMMISSION_RATIO','02','现货类','002','10档委比因子','','2.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_003_10_GRADE_WEIGHTED_COMMISSION_RATIO','02','现货类','003','10档加权委比因子','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_004_5_GRADE_COMMISSION_RATIO','02','现货类','004','5档委比因子','','2.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_005_5_GRADE_WEIGHTED_COMMISSION_RATIO','02','现货类','004','5档加权委比因子','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_007_SPREAD','02','现货类','007','价差因子','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_008_CALL_AUCTION_SECOND_STAGE_INCREASE','02','现货类','008','集合竞价二阶段涨幅','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_013_BID_LARGE_AMOUNT_BILL','02','现货类','013','5档大挂单委买因子','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_014_ASK_LARGE_AMOUNT_BILL','02','现货类','015','5档大挂单委卖因子','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_016_AMOUNT_AND_COMMISSION_RATIO','02','现货类','016','成交额和委买委卖相结合','','2.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_017_RISING_FALLING_AMOUNT_RATIO','02','现货类','017','涨跌成交额对比','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_018_UNTRADED_STOCK_RATIO','02','现货类','018','未成交股票占比','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_019_DAILY_ACCUMULATED_LARGE_ORDER_RATIO','02','现货类','019','日累计大单占比','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_020_ROLLING_ACCUMULATED_LARGE_ORDER_RATIO','02','现货类','020','滚动累计大单占比','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_021_DELTA_TOTAL_COMMISSION_RATIO','02','现货类','021','总委比增量','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_022_OVER_NIGHT_YIELD','02','现货类','022','隔夜收益率','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_023_AMOUNT_AND_1ST_COMMISSION_RATIO','02','现货类','023','成交额和1档委买委卖相结合','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_024_RISING_LIMIT_STOCK_PROPORTION','02','现货类','024','涨停股占比','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_025_FALLING_LIMIT_STOCK_PROPORTION','02','现货类','025','跌停股占比','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_028_AMOUNT_BID_10_GRADE_COMMISSION_RATIO','02','现货类','028','成交额10档委买比','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_029_AMOUNT_ASK_10_GRADE_COMMISSION_RATIO','02','现货类','029','成交额10档委卖比','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_030_AMOUNT_BID_5_GRADE_COMMISSION_RATIO','02','现货类','030','成交额5档委买比','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_031_AMOUNT_ASK_5_GRADE_COMMISSION_RATIO','02','现货类','031','成交额5档委卖比','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_033_10_GRADE_COMMISSION_VOLATILITY_RATIO','02','现货类','033','10档委买委卖波动率比例因子','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_034_5_GRADE_COMMISSION_VOLATILITY_RATIO','02','现货类','034','5档委买委卖波动率比例因子','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_035_TOTAL_COMMISSION_RATIO_DIFFERENCE','02','现货类','035','总委比差分','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_036_10_GRADE_COMMISSION_RATIO_DIFFERENCE','02','现货类','036','10档委比差分','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_037_5_GRADE_COMMISSION_RATIO_DIFFERENCE','02','现货类','037','5档委比差分','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_038_DAILY_RISING_STOCK_RATIO','02','现货类','038','当日上涨股票比例','','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_040_10_GRADE_COMMISSION_RATIO_MEAN','02','现货类','040','10档委比均值','20|50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_041_10_GRADE_COMMISSION_RATIO_STD','02','现货类','041','10档委比标准差','50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_042_5_GRADE_COMMISSION_RATIO_MEAN','02','现货类','042','5档委比均值','20|50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_043_5_GRADE_COMMISSION_RATIO_STD','02','现货类','043','5档委比标准差','50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_044_10_GRADE_WEIGHTED_COMMISSION_RATIO_DIFFERENCE','02','现货类','044','10档加权委比差分','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_045_10_GRADE_WEIGHTED_COMMISSION_RATIO_MEAN','02','现货类','045','10档加权委比均值','20|50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_046_10_GRADE_WEIGHTED_COMMISSION_RATIO_STD','02','现货类','046','10档加权委比标准差','50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_047_5_GRADE_WEIGHTED_COMMISSION_RATIO_DIFFERENCE','02','现货类','047','5档加权委比差分','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_048_5_GRADE_WEIGHTED_COMMISSION_RATIO_MEAN','02','现货类','048','5档加权委比均值','20|50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_049_5_GRADE_WEIGHTED_COMMISSION_RATIO_STD','02','现货类','049','5档加权委比标准差','50|100|300|500','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_050_AMOUNT_BID_TOTAL_COMMISSION_RATIO_STD','02','现货类','050','成交额总委买比标准差','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_051_AMOUNT_BID_10_GRADE_COMMISSION_RATIO_STD','02','现货类','051','成交额10档委买比标准差','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_052_AMOUNT_BID_5_GRADE_COMMISSION_RATIO_STD','02','现货类','052','成交额5档委买比标准差','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_053_AMOUNT_ASK_TOTAL_COMMISSION_RATIO_STD','02','现货类','053','成交额总委卖比标准差','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_054_AMOUNT_ASK_10_GRADE_COMMISSION_RATIO_STD','02','现货类','054','成交额10档委卖比标准差','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());
insert into factor_config values ('FCT_02_055_AMOUNT_ASK_5_GRADE_COMMISSION_RATIO_STD','02','现货类','055','成交额5档委卖比标准差','20|50|100|200','1.0','OWN',SYSDATE(),SYSDATE());





