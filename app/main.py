from price import fetch_price
from symbol import get_symbols, get_nonexisting
from correlations import build_correlations, build_correlations_theme
from neo4j import NeoGraph
from utils import get_pg_engine, read_table, read_symbols
import utils
import argparse
from tqdm import tqdm
from datetime import datetime

def symbols(namespace):
    get_symbols(namespace.exchange)

def prices(namespace):
    ne = get_nonexisting()
    if namespace.stock != '*':
        fetch_price(namespace.stock)
    else:
        for s in tqdm(ne):
            fetch_price(s)


def correlate(namespace):
    #build_correlations(namespace.start_date, namespace.end_date, namespace.num_stocks)
    build_correlations_theme(namespace.start_date, namespace.end_date, namespace.num_stocks)


def to_neo(namespace):

    #symbols = read_table('symbols')
    symbols = read_symbols()
    print(symbols)
    var_coef = read_table('coef_variation')
    print('corr_id', namespace.corr_id)
    cor = read_table(namespace.corr_id)
    print(cor)
    #cor = cor.query('cor == cor')
    symbols = symbols[(symbols['symbol'].isin(cor['symbol1'])) | (symbols['symbol'].isin(cor['symbol2']))]
    print('isin cor')
    print(symbols.head())
    symbols = symbols.merge(var_coef,on='symbol', how='left')

    ng = NeoGraph()
    if not namespace.dont_truncate:
        ng.truncate()

    ng.add_companies(symbols)
    ng.create_links(cor)


def to_neo_stock(namespace):

    df_stock = utils.read_stock()
    df_corr = utils.read_corr()

    print(df_stock.head())
    print(df_corr.head())

    #return

    ng = NeoGraph()
    #if not namespace.dont_truncate:
    
    ng.truncate()

    ng.add_stock(df_stock)
    ng.create_links_stock(df_corr)

def to_neo_company():

    df_company = utils.read_company()
    df_person = utils.read_person()
    df_holder = utils.read_holder()

    print(df_company.head())
    print(df_person.head())
    print(df_holder.head())

    #return 
    ng = NeoGraph()

    ng.add_company(df_company)
    ng.add_person(df_person)
    ng.create_links_holder(df_holder)

    pass

if __name__ == "__main__":

    #to_neo_stock(None)
    #to_neo_company()
    build_correlations('2019-10-01', '2020-03-31', 100)
    exit(0)

    parser = argparse.ArgumentParser(description='Stock exploration using Graph Networks')

    subparsers = parser.add_subparsers(help='Action type to perform')

    parser_gs = subparsers.add_parser('gs', help = "'gs' for getting Symbols from the API")
    parser_gs.add_argument('-e','--exchange', help="Stock Exchange identifier, by default it's LON for London Stock Exchange")
    parser_gs.set_defaults(func=symbols)

    parser_f = subparsers.add_parser('p', help = "'p' for fetching prices from the API")
    parser_f.add_argument('-s','--stock', default='*', help='Stock symbol to fetch, if not given, it will pull price for all stocks')
    parser_f.set_defaults(func=prices)


    help_text="""
                + 'cor' for calculating the pairwise correlations between the stocks, this command has 3 arguments:\n
                    \t+ '-sd' : start date for price data (YYYY-MM-DD)
                    \t+ '-ed' : end data for price data
                    \t+ '-ns' : number of stocks to correlate (these are ordered by transaction volume over the previous period)
              """
    parser_corr = subparsers.add_parser('cor', help = help_text)
    parser_corr.add_argument('-sd','--start-date', default = '2019-11-01', help='Format is YYYY-MM-DD')
    parser_corr.add_argument('-ed','--end-date', default = datetime.now().strftime('%Y-%m-%d'),help='Format is YYYY-MM-DD')
    parser_corr.add_argument('-ns','--num-stocks', default = 1000, type=int)
    parser_corr.set_defaults(func=correlate)

    parser_neo = subparsers.add_parser('neo', help = 'Add stock data to Neo4j database')
    parser_neo.add_argument('-dt','--dont-truncate', action='store_true')
    parser_neo.add_argument('-c','--corr-id', default = 'correlations_2019_10_01_2020_03_31_100', help='Name of the correlation table to use')
    parser_neo.set_defaults(func=to_neo)

    args = parser.parse_args()
    args.func(args)
    