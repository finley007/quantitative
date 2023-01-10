select count(1) from index_constituent_config;
select count(1) from stock_validation_result where validation_code = '20221213-finley';


select count(1), substring(date, 1 ,4) from index_constituent_config group by substring(date, 1 ,4);
select count(1), substring(date, 1 ,7) from index_constituent_config where substring(date, 1 ,4) = '2022' group by substring(date, 1 ,7);
select count(1), substring(date, 1 ,10) from index_constituent_config where substring(date, 1 ,7) = '2022-01' group by substring(date, 1 ,10);
+----------+-----------------------+
| count(1) | substring(date, 1 ,4) |
+----------+-----------------------+
|   207400 | 2017                  |
|   206550 | 2018                  |
|   207400 | 2019                  |
|   206550 | 2020                  |
|   206550 | 2021                  |
|   125800 | 2022                  |
+----------+-----------------------+
+----------+-----------------------+
| count(1) | substring(date, 1 ,7) |
+----------+-----------------------+
|    16150 | 2022-01               |
|    13600 | 2022-02               |
|    19550 | 2022-03               |
|    16150 | 2022-04               |
|    16150 | 2022-05               |
|    17850 | 2022-06               |
|    17850 | 2022-07               |
|     8500 | 2022-08               |
+----------+-----------------------+
+----------+------------------------+
| count(1) | substring(date, 1 ,10) |
+----------+------------------------+
|      850 | 2022-01-04             |
|      850 | 2022-01-05             |
|      850 | 2022-01-06             |
|      850 | 2022-01-07             |
|      850 | 2022-01-10             |
|      850 | 2022-01-11             |
|      850 | 2022-01-12             |
|      850 | 2022-01-13             |
|      850 | 2022-01-14             |
|      850 | 2022-01-17             |
|      850 | 2022-01-18             |
|      850 | 2022-01-19             |
|      850 | 2022-01-20             |
|      850 | 2022-01-21             |
|      850 | 2022-01-24             |
|      850 | 2022-01-25             |
|      850 | 2022-01-26             |
|      850 | 2022-01-27             |
|      850 | 2022-01-28             |
+----------+------------------------+
select count(1), substring(date, 1 ,4) from stock_validation_result where validation_code = '20221213-finley' group by substring(date, 1 ,4);
select count(1), substring(date, 1 ,7) from stock_validation_result where validation_code = '20221213-finley' and substring(date, 1 ,4) = '2022' group by substring(date, 1 ,7);
select count(1), substring(date, 1 ,10) from stock_validation_result where validation_code = '20221213-finley' and substring(date, 1 ,7) = '2022-01' group by substring(date, 1 ,10);
select count(1), tscode from stock_validation_result where validation_code = '20221213-finley' and substring(date, 1 ,10) = '2022-01-07' group by tscode;
select * from stock_validation_result where validation_code = '20221213-finley' and substring(date, 1 ,10) = '2022-01-07' and tscode = '002382.SZ';
+----------+-----------------------+
| count(1) | substring(date, 1 ,4) |
+----------+-----------------------+
|   463715 | 2019                  |
|   226116 | 2021                  |
|   362997 | 2018                  |
|   287157 | 2020                  |
|   386542 | 2017                  |
|   137290 | 2022                  |
+----------+-----------------------+
+----------+-----------------------+
| count(1) | substring(date, 1 ,7) |
+----------+-----------------------+
|    18395 | 2022-01               |
|    18179 | 2022-06               |
|    21708 | 2022-03               |
|    18190 | 2022-04               |
|     2211 | 2022-08               |
|    18730 | 2022-05               |
|    17464 | 2022-02               |
|    22413 | 2022-07               |
+----------+-----------------------+
+----------+------------------------+
| count(1) | substring(date, 1 ,10) |
+----------+------------------------+
|      847 | 2022-01-21             |
|     1491 | 2022-01-07             |
|      846 | 2022-01-05             |
|      848 | 2022-01-26             |
|      845 | 2022-01-04             |
|      847 | 2022-01-19             |
|     1401 | 2022-01-18             |
|      848 | 2022-01-28             |
|      847 | 2022-01-20             |
|      847 | 2022-01-17             |
|      846 | 2022-01-14             |
|      848 | 2022-01-25             |
|      846 | 2022-01-11             |
|     1108 | 2022-01-10             |
|      846 | 2022-01-06             |
|      848 | 2022-01-27             |
|      846 | 2022-01-12             |
|     1692 | 2022-01-13             |
|      848 | 2022-01-24             |
+----------+------------------------+