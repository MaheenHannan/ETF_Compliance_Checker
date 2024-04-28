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

def country_to_continent(country_name):
    country_alpha2 = pc.country_name_to_country_alpha2(country_name)
    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
    country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
    return country_continent_name

def get_fixed_assets(asset):
    match asset:
        case "openfigi_apikey":
            return 'a8c36abd-ad1a-4e5d-862e-92697a275bb8' #OpenFIGI API Key (free to use)
        
        case "exchange_codes":
            return pd.read_csv("OpenFIGI_Exchange_Codes.csv", encoding="unicode_escape", keep_default_na=False)

        case "holdings":
            return pd.read_csv("HSBC_World_Islamic.csv", encoding="unicode_escape")
        
        case "zoya_url":
            return "https://api.zoya.finance/graphql"

        case "zoya_headers":
            return {'Authorization': 'live-98324407-8f79-40b9-97cb-36704b2e7a4c', 'Content-Type': 'application/json'} #Zoya API Key  (purchase to use)

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
            print("OpenFigi error, retrying in 6 seconds")
            sleep(6)
    return json.loads(connection.read().decode('utf-8'))

def init_fund(index, rows, holdings):
    fund = pd.DataFrame()
    fund.insert(0, index, holdings[index], False)
    
    for row in rows:
        fund.loc[:, row] = holdings[row]

    fund = fund.set_index(index)

    fund.insert(1, 'Ticker', '', False)
    fund.insert(2, 'Name', '', False)
    fund.insert(3, 'Continent', '', True)
    fund.insert(4, 'Business_screening', '', True)
    fund.insert(5, 'Finance_screening', '', True)

    return fund

def get_zoya_stock(ticker):

    url = get_fixed_assets("zoya_url")
    headers = get_fixed_assets("zoya_headers")

    business = "Unrated"
    financial = "Unrated"
    
    data = {"query": "query GetAdvancedReport($input: AdvancedReportInput!) { advancedCompliance {  report(input: $input) {businessScreen financialScreen} } }",
        "variables": {"input": {"symbol": ticker, "methodology": "AAOIFI"}}}

    with warnings.catch_warnings(action="ignore"):
        while True:
            try:
                response = requests.post(url=url, json=data, headers=headers, verify=False) #SSL Verification turned off
                business = response.json().get('data').get('advancedCompliance').get('report').get('businessScreen')
                financial = response.json().get('data').get('advancedCompliance').get('report').get('financialScreen')
                break
            except Exception as error:
                if "max retries" in str(error).lower():
                    print("Zoya error, retrying in 5 seconds")
                else:
                    break

    return business, financial

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
    
    for fund_index, fund_row in fund.iterrows():
        continent = country_to_continent(fund_row['Country'])
        exchCodes = exchCodes_list.loc[exchCodes_list['Composite Name'].str.contains(fund_row['Country'], case=False)]
        for exchCodes_index, exchCodes_row in exchCodes.iterrows():
            ticker, name = get_ticker(fund_index, exchCodes_row['Exchange Code'])
            if not ticker == '':
                break      

        ticker, name = get_ticker(fund_index, 'US')
        business, financial = get_zoya_stock(ticker)

        z_ticker = ''
        if business == 'Unrated':
            match continent:
                case "Europe":
                    z_ticker = get_ticker(fund_index, 'LO', False) + "-LN"
                    if z_ticker == '-LN':
                        z_ticker = get_ticker(fund_index, 'LN', False) + "-LN"
                case "Oceania":
                    z_ticker = get_ticker(fund_index, 'AU', False) + "-AU"
                    

            business, financial = get_zoya_stock(z_ticker)
        
        fund.loc[fund.index == fund_index, ['Ticker']] = ticker
        fund.loc[fund.index == fund_index, ['Name']] = name
        fund.loc[fund.index == fund_index, ['Continent']] = continent
        fund.loc[fund.index == fund_index, ['Business_screening']] = business.capitalize()
        fund.loc[fund.index == fund_index, ['Finance_screening']] = financial.capitalize()
        
        print(fund_index)
        stock = "ISIN:" + fund_index + "\t Ticker/Name:" + ticker + " / " + name + "\t" + " SE: " + exchCodes_row['Full Exchange Name'] + " " + exchCodes_row['Composite Name']
        compliancy = "Business: " + business + " Financial: " + financial

    return fund


def main():
    holdings_rows = ['Country','NumberOfShare','MarketValue','LocalCurrencyCode','Weighting']
    exchange_codes = get_fixed_assets('exchange_codes')
    holdings = get_fixed_assets('holdings')
    fund = set_stock_data(init_fund('ISIN', holdings_rows, holdings), exchange_codes)
    fund.to_csv("fund.csv")

main()
