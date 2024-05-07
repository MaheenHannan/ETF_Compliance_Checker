import pandas as pd
import json
import urllib.request
import urllib.parse
from time import sleep
import numpy as np
import warnings
import requests
from time import sleep
import pycountry_convert as pc
import math
from currency_converter import CurrencyConverter

def country_to_continent(country_name):
    country_alpha2 = pc.country_name_to_country_alpha2(country_name)
    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
    country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
    return country_continent_name

def get_fixed_assets(asset):
    match asset:
        case "openfigi_apikey":
            return '' #OpenFIGI API Key (free to use)
        
        case "exchange_codes":
            return pd.read_csv("OpenFIGI_Exchange_Codes.csv", encoding="unicode_escape", keep_default_na=False)
        
        case "zoya_url":
            return "https://api.zoya.finance/graphql"

        case "zoya_headers":
            return {'Authorization': '', 'Content-Type': 'application/json'} #Zoya API Key  (purchase to use)

def map_jobs(jobs):
    openfigi_apikey = get_fixed_assets('openfigi_apikey')
    handler = urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(handler)
    openfigi_url = 'https://api.openfigi.com/v3/mapping'
    request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(jobs), encoding='utf-8'))
    request.add_header('Content-Type','application/json')
    if openfigi_apikey:
        request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
    request.get_method = lambda: 'POST'
    while True:
        try:
            connection = opener.open(request)
            break
        except Exception as error:
            print("OpenFigi error, retrying in 6 seconds ", error)
            sleep(7)
    return json.loads(connection.read().decode('utf-8'))

def init_fund(holdings, custom_columns=[]):
    
    #ISIN, Shares, Market_Value, Country, Local_Currency, Weighting

    columns = ['ISIN',
               'Ticker',
               'Name',
               'Continent',
               'Country',
               'Stock_Exchange',
               'Business_Screening',
               'Finance_Screening',
               'Shares',
               'Local_Currency',
               'Market_Value',
               'Impure_Market_Value',
               'Weighting',
               'Impure_Weighting']

    index = 'ISIN'
    columns.extend(custom_columns)

    fund = pd.DataFrame(columns=columns)
    
    
    for column in columns:
        if column in holdings.columns:
            fund[column] = holdings[column]
        else:
            fund[column] = ''

    fund = fund.set_index(index)
    
    return fund

def get_zoya_stock(ticker):

    url = get_fixed_assets("zoya_url")
    headers = get_fixed_assets("zoya_headers")

    business = "Unrated"
    financial = "Unrated"
    haram_percent = 0
    
    data = {"query": "query GetAdvancedReport($input: AdvancedReportInput!) { advancedCompliance {  report(input: $input) {businessScreen financialScreen nonCompliantRevenue} } }",
        "variables": {"input": {"symbol": ticker, "methodology": "AAOIFI"}}}

    with warnings.catch_warnings(action="ignore"):
        while True:
            try:
                response = requests.post(url=url, json=data, headers=headers, verify=False) #SSL Verification turned off
                business = response.json().get('data').get('advancedCompliance').get('report').get('businessScreen')
                financial = response.json().get('data').get('advancedCompliance').get('report').get('financialScreen')
                haram_percent = float(response.json().get('data').get('advancedCompliance').get('report').get('nonCompliantRevenue'))/100
                break
            except Exception as error:
                if "max retries" in str(error).lower():
                    print("Zoya error, retrying in 5 seconds")
                else:
                    break

    return business, financial, haram_percent

def get_zoya_regions():

    url = get_fixed_assets("zoya_url")
    headers = get_fixed_assets("zoya_headers")
    data = {"query": "query ListRegions {advancedCompliance {regions}}"}

    with warnings.catch_warnings(action="ignore"):
        while True:
            try:
                regions = requests.post(url=url, json=data, headers=headers, verify=False).json().get('data').get('advancedCompliance').get('regions') #SSL Verification turned off
                break
            except Exception as error:
                if "max retries" in str(error).lower():
                    print("Zoya error, retrying in 5 seconds")
                else:
                    print(error)
                    break

    return regions

def get_ticker(isin, exchCode, need_name=True):
    try:
        result = map_jobs([{'idType': 'ID_ISIN', 'idValue': isin, 'exchCode': exchCode}])
    except Exception as error:
        print(error)
        ticker = error

    ticker = ','.join([d['ticker'] for d in result[0].get('data', [])])
    name = ','.join([d['name'] for d in result[0].get('data', [])])

    if need_name:
        return ticker, name
    else:
        return ticker

def set_stock_data(fund, exchCodes_list):

    usd_value = CurrencyConverter()
    
    for fund_index, fund_row in fund.iterrows():        

        ticker = ''
        z_ticker = ''
        exchange = ''
        
        country = fund_row['Country']
        continent = country_to_continent(country)
        exchCodes = exchCodes_list.loc[exchCodes_list['Composite Name'].str.contains(country, case=False)]
        if exchCodes.empty:
            exchCodes = exchCodes_list.loc[exchCodes_list['ISO Country Code (where applicable)'].str.contains(fund_index[:2], case=False)]
        
        def wildcard_search():
            for exchCodes_index, exchCodes_row in exchCodes.iterrows():
                ticker, name = get_ticker(fund_index, exchCodes_row['Exchange Code'])
                if not ticker == '':
                    break
            return ticker, name, exchCodes_row['Exchange Code']

        match country:
            case "United States":
                exchange = 'UA'
                ticker, name = get_ticker(fund_index, exchange)
                if ticker == '':
                    ticker, name, exchange = wildcard_search()                
                    
            case "United Kingdom":
                exchange = 'LO'
                ticker, name = get_ticker(fund_index, exchange)
                if ticker == '':
                    exchange = 'LN'
                    ticker, name = get_ticker(fund_index, exchange)
                   
            case _:
                ticker, name, exchange = wildcard_search()
                if ticker == '':
                    exchange = 'UA'
                    ticker, name = get_ticker(fund_index, 'UA')

        business, financial, haram_percent = get_zoya_stock(ticker)

        z_ticker = ''
        if business == 'Unrated':
            match continent:
                case "Europe":
                    z_ticker = get_ticker(fund_index, 'LO', False) + "-LN"
                    if z_ticker == '-LN':
                        z_ticker = get_ticker(fund_index, 'LN', False) + "-LN"
                case "Oceania":
                    z_ticker = get_ticker(fund_index, 'AU', False) + "-AU"
                case "Asia":
                    if country == "India":
                        z_ticker = get_ticker(fund_index, 'IB', False) + "-IB"
                    elif country == "China":
                        z_ticker = get_ticker(fund_index, 'TT', False) + "-TT"
                        
            business, financial, haram_percent = get_zoya_stock(z_ticker)
            
            if business == 'Unrated':
                z_ticker = get_ticker(fund_index, 'US', False)
                business, financial, haram_percent = get_zoya_stock(z_ticker)

        
        fund.loc[fund.index == fund_index, ['Ticker']] = ticker
        fund.loc[fund.index == fund_index, ['Stock_Exchange']] = exchange
        fund.loc[fund.index == fund_index, ['Name']] = name
        fund.loc[fund.index == fund_index, ['Continent']] = continent
        fund.loc[fund.index == fund_index, ['Business_Screening']] = business.capitalize()
        fund.loc[fund.index == fund_index, ['Finance_Screening']] = financial.capitalize()
        fund.loc[fund.index == fund_index, ['Impure_Market_Value']] = round(float(fund_row['Market_Value'])*haram_percent,6)
        fund.loc[fund.index == fund_index, ['Impure_Weighting']] = round(float(fund_row['Weighting'])*haram_percent,6)

        #stock = "ISIN:" + fund_index + "\t Ticker/Name:" + ticker + " / " + name + "\t" + " SE: " + exchCodes_row['Full Exchange Name'] + " " + exchCodes_row['Composite Name']
        #compliancy = "Business: " + business + " Financial: " + financial

        print(fund_index)
        
    return fund


def main():
    exchange_codes = get_fixed_assets('exchange_codes')
    holdings_list = ['']
    for i in holdings_list:
        holding = i + ".csv"
        checked = i + "_CHECKED" + ".csv"
        holdings = pd.read_csv(holding)
        try:
            fund = set_stock_data(init_fund(holdings), exchange_codes)
            fund.to_csv(checked)
            print("done", i)
        except Exception as error:
            print(error)
            print("failed", i)

main()
