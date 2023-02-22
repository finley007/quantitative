大单成交额占比（大单用均值和一倍标准差判断）
import Funcs.Formula as Fla

def main(df1, df):
    new_df = Fla.pd.DataFrame()
    df['money'] = df['volume'] * df['price']
    df['money'] = df['money'].astype('float')
    buy_money = df[df['bs'] == 'B'][['money']]
    all_money = buy_money['money'].groupby('code').sum()
    big_trader = buy_money.groupby('code').mean() + buy_money.groupby('code').std()
    big_trader.columns = ['big_trader']
    buy_money = Fla.pd.merge(buy_money.reset_index().set_index('code'), big_trader, how = 'left', on = 'code')
    buy_money = buy_money[buy_money['money'] >= buy_money['big_trader']]
    new_df['fdata'] = buy_money['money'].groupby('code').sum() / all_money
    return new_df

日内最大成交量出现的时间
import Funcs.Formula as Fla

def main(df, N = 1):
    new_df = Fla.pd.DataFrame()
    volume = df['volume_min'].unstack('date')
    volume = volume.reset_index().drop(columns = ['time'])
    new_df['fdata'] = volume.apply(Fla.np.argmax)
    new_df = Fla.sma(new_df / 239, N)
    return new_df

def main_date(df_, N = 1):
    volume = df_['volume_min']
    new_df = volume.groupby('date').apply(lambda x: x.reset_index().drop(columns = ['date','time']).apply(Fla.np.argmax))
    new_df = Fla.sma(new_df / 239, N)
    return new_df

分钟平均单笔成交额q分位数
import Funcs.Formula as Fla

def main(df1, df, q = 0.1, N = 1):
    new_df = Fla.pd.DataFrame()
    data = df[['dealnum', 'money']]
    data = data.reset_index()
    data['time'] = data['time'] // 100 * 100
    data = data.set_index(['code', 'time'])
    data_min = data.groupby(['code', 'time']).last()
    data_min = data_min.query("time > 92500")
    min_data = data_min.groupby('code').diff()
    min_data = min_data.fillna(data_min)
    money_per_deal = (min_data['money'] / min_data['dealnum']).fillna(0).unstack('code')
    rank = money_per_deal.rank(ascending = False)
    money_per_deal = money_per_deal.where(rank > 10)
    new_df['fdata'] = (money_per_deal.quantile(q) - money_per_deal.min()) / (money_per_deal.max() - money_per_deal.min())
    return new_df

大成交量对应的收益率标准差(分钟成交量前1/3)
import Funcs.Formula as Fla

def main(df , N = 1):
    new_df = Fla.pd.DataFrame()
    volume = df['volume_min'].unstack('date')
    close = df['close_min'].unstack('date')
    pre_close = df['pre_close_min'].unstack('date')
    volume_rank = volume.rank(pct = True)
    ret = close / pre_close - 1
    ret = ret.where(volume_rank > 2/3)
    new_df['fdata'] = ret.std()
    new_df = Fla.sma(new_df , N)
    return new_df

def main_date(df , N = 1):
    ret = df['close_min'] / df['pre_close_min'] - 1 
    volume_rank = df['volume_min'].groupby('date').rank(pct=True)
    ret_rank = ret.where(volume_rank > 2/3)
    ret_std = ret_rank.groupby('date').std()
    new_df = Fla.sma(ret_std , N)
    return new_df

当日隔夜涨幅
集合竞价量比（与前N日集合竞价均量相比）
集合竞价第二阶段累计挂单量/第一阶段累计挂单量
当日隔夜涨幅（将涨幅排序值最大的10%股票排序值变为0）
剔除了N个tick的当日隔夜涨幅
剔除了N个tick的当日隔夜涨幅
剔除了N个tick的当日隔夜涨幅
剔除了N个tick的阶段 2 （9:20-9:25，该阶段无法撤单）涨幅
剔除了N个tick的阶段 2 （9:20-9:25，该阶段无法撤单）涨幅
剔除了N个tick的阶段 2 （9:20-9:25，该阶段无法撤单）涨幅
剔除了N个tick的阶段 2 价格是否平稳上升。相关系数
剔除了N个tick的阶段 2 价格是否平稳上升。相关系数
剔除了N个tick的阶段 2 价格是否平稳上升。相关系数
剔除了N个tick的集合竞价量比（240×集合竞价成交额/过去五日日均成交额）
剔除了N个tick的集合竞价量比（240×集合竞价成交额/过去五日日均成交额）
剔除了N个tick的集合竞价量比（240×集合竞价成交额/过去五日日均成交额）
阶段 1（9:15-9:20，该阶段可以撤单）涨幅
阶段 2 （9:20-9:25，该阶段无法撤单）涨幅
阶段 1 是否涨停
阶段 1 是否跌停
阶段 2 价格是否平稳上升。比率类型
阶段 2 价格是否平稳上升。相关系数
集合竞价量比（240×集合竞价成交额/过去五日日均成交额）
隔夜收益率与第二阶段涨幅的差

def main(df1, df2, df3, drop_tick = 1):
    pre_close = df2['pre_close'] / df2['factor']
    latest_price = df1['price'].groupby(['date', 'code']).apply(lambda x: x.tail(drop_tick + 1).iloc[0])
    latest_price = latest_price.unstack('code')
    new_df = latest_price / pre_close - 1
    return new_df