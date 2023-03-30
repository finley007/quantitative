select distinct err_msg from stock_validation_result where validation_code = '20230309-finley' and result = 1 order by err_msg; 
select count(1), result from stock_validation_result where validation_code = '20230320-finley' group by result;
select date from stock_validation_result where validation_code = '20230320-finley' group by date order by date;
select count(1) from stock_validation_result where validation_code = '20230320-finley';


| The validation result for stock: 000627 date: 20181226 is fail and error details: 数据未生成
| The validation result for stock: 002415 date: 20191008 is fail and error details: 数据未生成
| The validation result for stock: 603877 date: 20191128 is fail and error details: 数据未生成
| The validation result for stock: 000627 date: 20181226 is fail and error details: 数据未生成
| The validation result for stock: 600126 date: 20181225 is fail and error details: 数据未生成
| The validation result for stock: 603556 date: 20181207 is fail and error details: 数据未生成

RepeatRecord

=U19*AE19+V19*AF19+W19*AG19+X19*AH19+Y19*AI19+Z19*AJ19+AA19*AK19+AB19*AL19+AC19*AM19+AD19*AN19
=A19*K19+B19*L19+C19*M19+D19*N19+E19*O19+F19*P19+G19*Q19+H19*R19+I19*S19+J19*T19

select distinct date from future_instrument_config where date >= '2022-02-07' order by date

select date, tscode  into outfile 'G:\data\temp\aa.xls' from stock_validation_result where  validation_code = '20230309-finley' and result = 1;