import pandas as pd
import json
import urllib.request
import urllib.parse
from time import sleep
import numpy as np
import warnings
import requests

def get_fixed_assets(asset):
    match asset:
        case "openfigi_apikey":
            return '' #OpenFIGI API Key (free to use)
        
        case "exchange_codes":
            return pd.read_csv("OpenFIGI_Exchange_Codes.csv", encoding="unicode_escape")

        case "holdings":
            return pd.read_csv("HSBC_World_Islamic.csv", encoding="unicode_escape")
        
        case "zoya_url":
            return "https://api.zoya.finance/graphql"

        case "zoya_headers":
            return {'Authorization': '###', 'Content-Type': 'application/json'} #Zoya API Key  (purchase to use)

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
    connection = opener.open(request)
    if connection.code != 200:
        raise Exception('Bad response code {}'.format(str(response.status_code)))
    return json.loads(connection.read().decode('utf-8'))

def init_fund(index, rows, holdings):
    fund = pd.DataFrame()
    fund.insert(0, index, holdings[index], False)
    
    for row in rows:
        fund.loc[:, row] = holdings[row]

    fund = fund.set_index(index)

    fund.insert(1, 'Ticker', '', False)
    fund.insert(1, 'NSYE_Ticker', '', False)
    fund.insert(3, 'Name', '', False)
    fund.insert(4, 'Business_screening', '', True)
    fund.insert(4, 'Finance_screening', '', True)

    return fund

def get_zoya(url, headers, ticker):
    
    data = {"query": "query GetAdvancedReport($input: AdvancedReportInput!) { advancedCompliance {  report(input: $input) {businessScreen financialScreen} } }",
        "variables": {"input": {"symbol": ticker, "methodology": "AAOIFI"}}}

    with warnings.catch_warnings(action="ignore"):
        response = requests.post(url=url, json=data, headers=headers, verify=False) #SSL Verification turned off
        
    business = response.json().get('data').get('advancedCompliance').get('report').get('businessScreen')
    financial = response.json().get('data').get('advancedCompliance').get('report').get('financialScreen')

    return business, financial

def set_stock_data(fund, exchCodes_list):
    
    for fund_index, fund_row in fund.iterrows():
        exchCodes = exchCodes_list.loc[exchCodes_list['Composite Name'].str.contains(fund_row['Country'], case=False)]

        for exchCodes_index, exchCodes_row in exchCodes.iterrows():
            us_result = map_jobs([{'idType': 'ID_ISIN', 'idValue': fund_index, 'exchCode': 'US'}])
            try:
                
                result = map_jobs([{'idType': 'ID_ISIN', 'idValue': fund_index,
                                    'exchCode': exchCodes_row['Exchange Code']}])
                
                if not result == [{'warning': 'No identifier found.'}]:
                    break
                
            except Exception as error:
                print(error)

            result = us_result

        ticker = ','.join([d['ticker'] for d in result[0].get('data', [])])
        us_ticker = ','.join([d['ticker'] for d in us_result[0].get('data', [])])
        name = ','.join([d['name'] for d in result[0].get('data', [])])
        business, financial = get_zoya(get_fixed_assets("zoya_url"), get_fixed_assets("zoya_headers"), ticker)
                
        fund.loc[fund.index == fund_index, ['Ticker']] = ticker
        fund.loc[fund.index == fund_index, ['US_Ticker']] = us_ticker
        fund.loc[fund.index == fund_index, ['Name']] = name
        fund.loc[fund.index == fund_index, ['Business_screening']] = business.capitalize()
        fund.loc[fund.index == fund_index, ['Finance_screening']] = financial.capitalize()
        
        
        stock = "ISIN:" + fund_index + "\t Ticker/Name:" + ticker + " / " + us_ticker + " / " + name + "\t" + " SE: " + exchCodes_row['Full Exchange Name'] + " " + exchCodes_row['Composite Name']
        compliancy = "Business: " + business + " Financial: " + financial

        print(stock, compliancy)

    return fund


def main():
    holdings_rows = ['NumberOfShare','MarketValue','Country','LocalCurrencyCode','Weighting']
    exchange_codes = get_fixed_assets('exchange_codes')
    holdings = get_fixed_assets('holdings')
    fund = set_stock_data(init_fund('ISIN', holdings_rows, holdings), exchange_codes)

main()
