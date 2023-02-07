--查询主力合约
select * from future_instrument_config where is_main = 0 order by instrument;
--查询主力合约总天数
select count(1) from (select distinct date from future_instrument_config where is_main = 0) t
--查询合约起始天
select instrument, min(date), max(date) from future_instrument_config group by instrument order by instrument;
--查询主力合约起始天
select instrument, min(date), max(date) from future_instrument_config where is_main = 0 group by instrument order by instrument;
--查询合约的股票日期
select t2.tscode, t2.date from future_instrument_config t1, index_constituent_config t2 where t1.date = t2.date and t1.product = t2.product and t2.status = 0 and t1.instrument = 'IF1712';

--查询股票数据处理执行记录
select count(1), process_code from stock_process_record group by process_code;
--查询问题股票数据
select count(1), status from stock_process_record where process_code = '20221111-finley-1' group by status;
+----------+--------+
| count(1) | status |
+----------+--------+
|  1045695 |      0 |
|    60000 |      1 |
--列出错误类别
select distinct invalid_msg from stock_process_record where process_code = '20221111-finley-1' and status = 1;
select distinct invalid_msg from stock_process_record where process_code = '20221111-finley-1' and status = 1 and invalid_msg not like '%suspended%'; 
--列出某一类具体错误
select id, date, tscode, invalid_msg from stock_process_record where process_code = '20221111-finley-1' and status = 1 and invalid_msg like '%Invalid OCHL value%' order by date, tscode limit 10;
--根据record id查询date和tscode
select date, tscode from stock_process_record where id = '';
--删除出错的记录
delete from stock_process_record where process_code = '20221111-finley-1' and status = 1;

--查询停牌信息
select distinct date, tscode from index_constituent_config where status = 1;
--查询股票交易日
select distinct date from index_constituent_config where tscode = '002642' and status = 0 order by date;

select date, tscode, invalid_msg from stock_process_record where process_code = '20221111-finley-1' and status = 1 order by date, tscode;
delete from stock_process_record where process_code = '20221111-finley-1' and status = 1;

ALTER TABLE index_constituent_config add column status  int comment '0-正常，1-停牌';
ALTER TABLE stock_validation_result add column record_count  int;


--查询检查执行纪录
select count(1), validation_code from stock_validation_result group by validation_code;
--查询股票检查记录总数
select count(1) from stock_validation_result where validation_code = '20221219-finley';
--查询股票检查结果分布
select count(1), result from stock_validation_result where validation_code = '20230105-finley' group by result;
--查询股票检查结果
select distinct err_msg from stock_validation_result where validation_code = '20230105-finley' and result = 1 order by err_msg; 
--查询已修复数据
select count(1) from stock_validation_result where validation_code = '20221219-finley' and result =2; 
--查询一个具体的股票检查结果记录
select * from stock_validation_result where tscode = '000027.SZ' and validation_code = '20221213-finley'  and date like '%2020-04%';
select * from stock_validation_result where err_msg like '%The redundant data for noon break exists%';

explain select * from stock_validation_result where validation_code = '20221219-finley' and tscode = '002458' and date = '20210106';
select * from (select count(1) as count, validation_code, tscode, date from stock_validation_result group by validation_code, tscode, date) t where count > 1; 
select * from stock_validation_result where concat(validation_code, tscode, date) = '';

