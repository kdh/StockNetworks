from datetime import datetime
from utils import get_pg_engine
import pandas as pd
import numpy as np
from tabulate import tabulate
from matplotlib import pyplot as plt
from matplotlib import font_manager, rc
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)

import pdb

def print_df(df):
    # pip install tabulate
    # from tabulate import tabulate
    print(tabulate(df, headers='keys', tablefmt='psql'))

def clean_and_format(start_date = '2019-01-01', end_date = datetime.now().strftime('%Y-%m-%d'), num_stocks = 1000, use_pct_change=True):
    
    
    assert num_stocks > 20, 'To build an interesting analysis, make sure the number of stocks to use is at least 20'

    start_date_fmt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_fmt = datetime.strptime(end_date, '%Y-%m-%d')
    diff = end_date_fmt - start_date_fmt
    assert int(diff.days/7) > 12, 'For more meaningful correlations increase the window between the start_date and end_date (at least 12 weeks)'

    start_date = int(start_date.replace('-', ''))
    end_date = int(end_date.replace('-', ''))

    engine = get_pg_engine()

    '''
    stocks = pd.read_sql(f'select symbol, sum(volume) as volume \
                       from price \
                       where timestamp between \'{start_date}\' and \'{end_date}\' \
                       group by symbol \
                       order by sum(volume) desc \
                       limit {num_stocks}', engine)
    '''
    
    stocks = pd.read_sql(f'select top ({num_stocks}) stockCode as symbol, sum(volume) as volume \
                       from logDay \
                       where logDate between \'{start_date}\' and \'{end_date}\' \
                       group by stockCode \
                       order by sum(volume) desc \
                       ', engine)
                    
    print(stocks)
    
    relevant_stocks = ','.join([f"'{stock}'" for stock in stocks['symbol'].tolist()])

    #print('relevant_stocks')
    #print(relevant_stocks)

    '''
    price_data = pd.read_sql(f"select timestamp, symbol, close as price\
                            from price\
                            where \"timestamp\" between \'{start_date}\' and \'{end_date}\'\
                            and symbol in ({relevant_stocks})", engine)
    '''
    
    price_data = pd.read_sql(f"select logDate as timestamp, stockCode as symbol, priceClose as price\
                            from logDay\
                            where \"logDate\" between \'{start_date}\' and \'{end_date}\'\
                            and stockCode in ({relevant_stocks})", engine)

    # Calculating coefficient of variation to keep only stocks with more price movement
    stdevs = price_data.groupby('symbol')['price'].apply(lambda x: np.std(x)/x.mean()).to_frame().rename(columns={'price':'var_coef'})
    keep = stdevs[(stdevs<stdevs.quantile(0.95)) & (stdevs>stdevs.quantile(0.05))].index
    print(f"Dropping stocks with very high or very low coefficients of variation Keeping {len(keep)} out of {len(price_data['symbol'].unique())}")
    
    price_data = price_data[price_data['symbol'].isin(keep)]

    price_data = price_data.pivot(index='timestamp', columns='symbol', values='price')
    print('price_data pivot')
    print(price_data.head())

    drop_stocks = price_data.isna().apply('mean').sort_values(ascending=False).reset_index().rename(columns={0:'nas'}).query('nas > 0.65')
    #print(f"Will drop {len(drop_stocks)} stocks from the total list, dropping high missing values")
    
    price_data = price_data.loc[:,~price_data.columns.isin(drop_stocks['symbol'])]
    
    #print(f"Now cleaning dates")
    price_data = price_data.fillna(method='ffill')
    
    drop_stocks = price_data.isna().apply('mean').reset_index().rename(columns = {0:'nas'}).query('nas > 0.1')
    #print(f"Will additionally drop {len(drop_stocks)} due to high missingness at start of period")
    if len(drop_stocks) >0:
        price_data = price_data.loc[:,~price_data.columns.isin(drop_stocks['symbol'])]

    # Drop dates with high percentage og missing values
    drop_dates = price_data.isna().apply('mean', axis=1).to_frame().assign(drop = lambda df: df[0] > 0).query('drop').index
    price_data = price_data[~price_data.index.isin(drop_dates)]


    if use_pct_change:
        price_data = price_data.pct_change(fill_method='ffill')
        price_data = price_data.iloc[1:]

        print('use_pct_change')
        print(price_data.head())


    return price_data, stdevs



def correlate(df):
    print("Deduping pairings")
    df_corr = df.corr().reset_index()
    print('df_corr')
    print(df_corr.head())

    df_out = df_corr.melt(id_vars='symbol', var_name='cor').query('symbol != cor')

    print('df_melt')
    print(df_out.head())

    df_out = df_out[pd.DataFrame(np.sort(df_out[['symbol','cor']].values,1)).duplicated().values]
    df_out = df_out.rename(columns={'symbol':'symbol1', 'cor':'symbol2','value':'cor'})
    return df_out

def build_correlations(start_date = '2019-01-01', end_date = datetime.now().strftime('%Y-%m-%d'), num_stocks = 1000):
    engine = get_pg_engine()
    df, stdevs =  clean_and_format(start_date, end_date, num_stocks)
    df = correlate(df)

    a_id = f"{start_date.replace('-','_')}_{end_date.replace('-','_')}_{num_stocks}"
    df['id'] = a_id

    print('\n\nbuild_corr df')
    print(df.head())
    
    #df.to_sql('correlations_'+a_id, engine, if_exists='replace')
    
    #print(stdevs.head())
    #stdevs.to_sql('coef_variation', engine, if_exists='replace')
    #stdevs.to_sql('coef_variation', engine, if_exists='append')
       
# ------------------------- theme ---------------------------

def clean_and_format_theme(start_date = '2019-01-01', end_date = datetime.now().strftime('%Y-%m-%d'), num_stocks = 1000, use_pct_change=True):
        
    assert num_stocks > 20, 'To build an interesting analysis, make sure the number of stocks to use is at least 20'

    start_date_fmt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_fmt = datetime.strptime(end_date, '%Y-%m-%d')
    diff = end_date_fmt - start_date_fmt
    assert int(diff.days/7) > 12, 'For more meaningful correlations increase the window between the start_date and end_date (at least 12 weeks)'

    start_date = int(start_date.replace('-', ''))
    end_date = int(end_date.replace('-', ''))

    engine = get_pg_engine()
    
    #themes = pd.read_sql(f'select theme from theme_search where ratio > 10 group by theme order by theme', engine)
    themes = pd.read_sql(f'select theme from theme_search group by theme order by theme', engine)
                    
    print(themes)
    
    relevant_theme = ','.join([f"'{theme}'" for theme in themes['theme'].tolist()])
    #print(relevant_theme)
   
    price_data = pd.read_sql(f"select logDate as timestamp, theme, ratio \
                            from theme_search ", engine)
                            #where \"logDate\" between \'{start_date}\' and \'{end_date}\'\
                            #and stockCode in ({relevant_theme})", engine)

    print(price_data.head())
    # Calculating coefficient of variation to keep only stocks with more price movement
    stdevs = price_data.groupby('theme')['ratio'].apply(lambda x: np.std(x)/x.mean()).to_frame().rename(columns={'ratio':'var_coef'})
    keep = stdevs[(stdevs<stdevs.quantile(0.95)) & (stdevs>stdevs.quantile(0.05))].index
    print(f"Dropping stocks with very high or very low coefficients of variation\n\
        Keeping {len(keep)} out of {len(price_data['theme'].unique())}")
    
    #price_data = price_data[price_data['theme'].isin(keep)]

    #price_data = price_data.pivot(index='timestamp', columns='theme', values='ratio')
    price_data = price_data.pivot_table(index='timestamp', columns='theme', values='ratio', aggfunc=np.mean)
    drop_stocks = price_data.isna().apply('mean').sort_values(ascending=False).reset_index()\
    .rename(columns={0:'nas'}).query('nas > 0.65')
    print(f"Will drop {len(drop_stocks)} stocks from the total list, dropping high missing values")
    
    #price_data = price_data.loc[:,~price_data.columns.isin(drop_stocks['theme'])]
    
    print(f"Now cleaning dates")
    price_data = price_data.fillna(method='ffill')
    
    drop_stocks = price_data.isna().apply('mean').reset_index().rename(columns = {0:'nas'}).query('nas > 0.1')
    print(f"Will additionally drop {len(drop_stocks)} due to high missingness at start of period")
    if len(drop_stocks) >0:
        price_data = price_data.loc[:,~price_data.columns.isin(drop_stocks['theme'])]

    # Drop dates with high percentage og missing values
    drop_dates = price_data.isna().apply('mean', axis=1).to_frame().assign(drop = lambda df: df[0] > 0).query('drop').index
    price_data = price_data[~price_data.index.isin(drop_dates)]


    if use_pct_change:
        price_data = price_data.pct_change(fill_method='ffill')
        price_data = price_data.iloc[1:]

    return price_data, stdevs

def correlate_theme(df):
    print("Deduping pairings")
    df_out = df.corr().reset_index()
    print('corr matrix')
    print(df_out)

    df_out = df_out.melt(id_vars='theme', var_name='cor').query('theme != cor')
    df_out = df_out[pd.DataFrame(np.sort(df_out[['theme','cor']].values,1)).duplicated().values]
    df_out = df_out.rename(columns={'theme':'theme1', 'cor':'theme2','value':'cor'})
    return df_out
       
def build_correlations_theme(start_date = '2019-01-01', end_date = datetime.now().strftime('%Y-%m-%d'), num_stocks = 1000):
    engine = get_pg_engine()

    if False:
        df = pd.read_sql(f'select [dbo].getDate2(logDate) logDate, theme, ratio from theme_search where ratio > 20 and logDate > 20190101 order by theme, logDate', engine)
        df = df.set_index(['logDate'])
        df = df.pivot_table(index='logDate', columns='theme', values='ratio', aggfunc=np.mean)
        df = df.fillna(0)


        print(df.head(50))
        df.plot()
        plt.title("테마 검색")
        plt.xlabel("날짜")
        plt.ylabel("검색")
        plt.show()
        return


    df, stdevs =  clean_and_format_theme(start_date, end_date, num_stocks)
    df = correlate_theme(df)
    df = df.sort_values(['cor'], ascending=[False])
    a_id = f"{start_date.replace('-','_')}_{end_date.replace('-','_')}_{num_stocks}"
    df['id'] = a_id

    print_df(df.head(1000))
    
    #df.to_sql('correlations_'+a_id, engine, if_exists='replace')
    
    #print(stdevs.head())
    #stdevs.to_sql('coef_variation', engine, if_exists='replace')
    #stdevs.to_sql('coef_variation', engine, if_exists='append')
       
    
    