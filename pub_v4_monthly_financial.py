import pandas as pd
import requests
import pdfplumber
import re
import numpy as np
import gspread
import csv
import time
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# obtain these credentials with your own google account.
credentials = {
  "type": "service_account",
  "project_id": "",
  "private_key_id": "",
  "private_key": "",
  # if you encounter "gspread.exceptions.SpreadsheetNotFound" error, you have to share the spreadsheet with the client email listed below
  "client_email": "",
  "client_id": "",
  "auth_uri": "",
  "token_uri": "",
  "auth_provider_x509_cert_url": "",
  "client_x509_cert_url": ""
}

sa = gspread.service_account_from_dict(credentials)
sh = sa.open('<sheetname>')

combine = pd.DataFrame()

def extract_dbs(filename1):
    # must use sep='\t' when importing csv else you will get error tokenising the data
    dbs_statement = pd.read_csv(filename1, skiprows=17,sep='\t')
    df = dbs_statement['Transaction Date,Reference,Debit Amount,Credit Amount,Transaction Ref1,Transaction Ref2,Transaction Ref3'].str.split(',',expand=True)

    df['Description'] = df[4] + df[5] + df[6]
    df.drop([1,4,5,6,7],inplace=True, axis=1)
    df.rename({
        0:'Date',
        2:'Debit',
        3:'Credit'
    },inplace=True,axis=1)
    
    df['part1'] = df['Date'].str.split(' ').str.get(0)
    df['part2'] = df['Date'].str.split(' ').str.get(1)
    df['Date'] = df['part1']+' '+df['part2']
    
    #any row on debit side should be negative
    df['Debit'] = '('+df['Debit']+')'

    #fill empty debit cells with credit values
    df['Debit'] = np.where(df['Debit'] == '( )', df['Credit'], df['Debit'])
    df.drop('Credit',inplace=True,axis=1)
    df.rename({'Debit':'Amt'},inplace=True, axis=1)

    df.drop(['part1','part2'],inplace=True,axis=1)
    df['Bank'] = 'DBS'

    df['Date'] = df['Date'].str.replace(' ', '')


    ###################

    currentYear = datetime.now().year
    dates = df["Date"] + str(currentYear)

    # add a space after the day in the series
    datetime_series = pd.Series([None] * len(dates))
    for i, date_str in dates.items():
        try:
            if len(date_str) == 5:
                # add a space after the day for dates like '26dec'
                date_str = date_str[:-3] + ' ' + date_str[-3:]
                datetime_series[i] = datetime.strptime(date_str, '%d %b')
            elif len(date_str) == 9:
                # parse dates like '01mar2023'
                datetime_series[i] = datetime.strptime(date_str, '%d%b%Y')
        except ValueError:
            pass

    # format the datetime series as a string without the timestamp
    dates = datetime_series.apply(lambda x: x.strftime('%Y-%m-%d') if x is not None else None)
    df = pd.concat([df,dates],axis=1,ignore_index=False)
    df.drop('Date', axis=1, inplace=True)
    df.rename(columns = {0:"Date"},inplace=True)

    ###################
    
    # rearrange the columns
    df = df[['Date','Description','Amt','Bank']]
    
    
    return df

def extract_citi(filename2):
    global table
    
    trans = pd.DataFrame()

    with pdfplumber.open(filename2) as pdf:
        pages = pdf.pages

        for i,pg in enumerate(pages):    
            text = pages[i].extract_text()
            # print(f'{i} --- {text}')

            new_re = re.compile(r'(?<!\d)(\d{2})[a-zA-Z]{3} [a-zA-Z]')

            for line in text.split('\n'):
                if new_re.match(line):
                    line_item = pd.Series(line, dtype=pd.StringDtype())
                    trans = pd.concat([trans,line_item])

    trans = pd.DataFrame(trans)
    trans.reset_index(inplace=True)
    trans.drop('index',inplace=True,axis=1)

    date = trans[0].apply(lambda x:x.split()[0])

    ### take out of a list
    description = trans[0].apply(lambda x:x.split()[1:len(x)])

    output_des= list()
    i=0

    for x in description:
        split = ' '.join(map(str, x))
        output_des.insert(i,split)
        i=i+1

    description = pd.DataFrame(output_des)

    amount = trans[0].apply(lambda x:x.split()[-1])
    
    table = pd.concat([date,description,amount],axis=1)
    table.columns=["1","2","3"]
    table.rename(columns = {'1':'Date','2':'Description','3':'Amt',}, inplace = True)

    ####################

    currentYear = datetime.now().year
    dates = table["Date"] + str(currentYear)

    # add a space after the day in the series
    datetime_series = pd.Series([None] * len(dates))
    for i, date_str in dates.items():
        try:
            if len(date_str) == 5:
                # add a space after the day for dates like '26dec'
                date_str = date_str[:-3] + ' ' + date_str[-3:]
                datetime_series[i] = datetime.strptime(date_str, '%d %b')
            elif len(date_str) == 9:
                # parse dates like '01mar2023'
                datetime_series[i] = datetime.strptime(date_str, '%d%b%Y')
        except ValueError:
            pass

    # format the datetime series as a string without the timestamp
    dates = datetime_series.apply(lambda x: x.strftime('%Y-%m-%d') if x is not None else None)
    table = pd.concat([table,dates],axis=1,ignore_index=False)
    table.drop('Date', axis=1, inplace=True)
    table.rename(columns = {0:"Date"},inplace=True)

    ####################

    table['Bank'] = 'Citi'
    
    # rearrange the columns
    table = table[['Date','Description','Amt','Bank']]

    
    # remove the brackets in values replace with negative sign 
    # so that i can convert positive to negative and negative to positive

    table['Amt'].astype(str)

    for index, amt in enumerate(table['Amt']):
        fullstring = "{x}".format(x=table['Amt'])
        substring = "("

        if substring in fullstring:
            table['Amt'][index] = table['Amt'][index].replace("(","-")
            table['Amt'][index] = table['Amt'][index].replace(")","")

    # convert to float and also, any phrases that are present are turned into nan
    table['Amt']
    table['Amt'] = pd.to_numeric(table['Amt'], errors='coerce')

    table['Amt'].astype(float)
    table['Amt']= -table['Amt']
    
    #have to convert nan to empty '' else will throw error
    table['Amt'] = table['Amt'].fillna('')



    return table

def extract_hsbc(filename3):
    trans = pd.DataFrame()

    with pdfplumber.open(filename3) as pdf:

        # Loop through each page of the PDF file
        for page in pdf.pages:

            # Extract the text content of the current page
            text = page.extract_text()

            if text:
                # Split the text into lines
                lines = text.split('\n')

                # Loop through each line and run multiple regex matches
                for line in lines:
                    # Run regex matches on the line
                    match1 = re.search(r"^“\d", line)
                    match2 = re.search(r"^\d{2}\s*[a-zA-Z]{3}", line)

                    # Print the line if it matches any of the regex patterns
                    if match1 or match2:
                        line_item = pd.Series(line, dtype=pd.StringDtype())
                        trans = pd.concat([trans,line_item])

    # gives the dataframe proper index for ease of ref downstream transformation
    trans = pd.DataFrame(trans) 
    trans.reset_index(inplace=True)
    trans.drop('index',inplace=True,axis=1)

    # split strings in the cells into columns
    dateNum = trans[0].apply(lambda x:x.split()[0])
    dateMonth = trans[0].apply(lambda x:x.split()[1])

    # output the series into same df and rename
    df1 = pd.concat([dateNum, dateMonth], axis=1,keys=['dateNum', 'dateMonth'])
    df1.rename(columns={0:'dateNum',0:'dateMonth'}, inplace=True)

    # concat the two columns to form proper date formatting
    df1['dateFinal'] = df1['dateNum'] + df1['dateMonth']

    # to pick out edge cases in date formatting like : DDMmmDDMmm
    split_text_date = pd.DataFrame() 

    # iterate over each row of the DataFrame
    pattern = re.compile(r"^\d{2}\s*[a-zA-Z]{3}\d{2}[A-Z]{1}[a-z]{2}")

    for row in df1["dateFinal"]:
        if not pattern.match(row):
            split_text = row.split(r"^\d{2}")
            split_text = split_text.pop()
            split_item = pd.Series(split_text, dtype=pd.StringDtype())
            split_text_date = pd.concat([split_text_date,split_item])
            
    split_text_date.reset_index(inplace =True)
    split_item = pd.DataFrame(split_text_date[0])
    trans_split_text_date = trans.merge(split_text_date, left_index=True, right_index=True)
    trans_split_text_date.drop('index', axis=1,inplace=True)

    # identify all extra strings that i'd like to omit from my txn text description
    pattern_extract_date_from_txn = re.compile(
        r"^\d{2}\s[A-Z]{1}[a-z]{2}\s*\d{2}[A-Z][a-z]{2}\s*|"
        r"^\d{2}[A-Z][a-z]{2}\s{1}\d{2}[A-Z][a-z]{2}\s|"
        r"^\d{2}\s{1}[A-Z][a-z]{2}\s{1}[a-zA-Z]\d{2}[A-Z][a-z]{2}|"
        r"^\d{2}\s{1}[A-Z]{1}[a-z]{2}\s{1}\d{2}\s{1}[A-Z]{1}[a-z]{2}\s|"
        r"^“\d{2}\s{1}[A-Z]\s{1}\d{2}\s{1}[A-Z]\s")

    # implement via lambda expression the extraction of the str that i don't need
    row_str_print = trans_split_text_date['0_x'].apply(lambda x:re.sub(pattern_extract_date_from_txn,'',x))
    row_str_print_df = row_str_print.to_frame()

    # name this new column so that it's easier to identify columns after concating with main df
    row_str_print_df.rename(columns={'0_x':'txnExtracted'}, inplace=True)

    # concat with main df with trans and date
    trans_split_text_date = pd.concat([trans_split_text_date,row_str_print_df],axis=1)

    # rename columns for ez dropping later
    trans_split_text_date.rename(columns={'0_x':'rawTable','0_y':'dateExtracted', 'txnExtracted':'txnExtracted' }, inplace=True)

    # drop raw column
    trans_split_text_date.drop('rawTable', axis=1, inplace=True)

    # primary key identifier
    trans_split_text_date = trans_split_text_date.assign(Bank='HSBC')

    currentYear = datetime.now().year
    dates = trans_split_text_date["dateExtracted"] + str(currentYear)

    # add a space after the day in the series
    datetime_series = pd.Series([None] * len(dates))
    for i, date_str in dates.items():
        try:
            if len(date_str) == 5:
                # add a space after the day for dates like '26dec'
                date_str = date_str[:-3] + ' ' + date_str[-3:]
                datetime_series[i] = datetime.strptime(date_str, '%d %b')
            elif len(date_str) == 9:
                # parse dates like '01mar2023'
                datetime_series[i] = datetime.strptime(date_str, '%d%b%Y')
        except ValueError:
            pass

    # format the datetime series as a string without the timestamp
    dates = datetime_series.apply(lambda x: x.strftime('%Y-%m-%d') if x is not None else None)
    trans_split_text_date = pd.concat([trans_split_text_date,dates],axis=1,ignore_index=False)
    trans_split_text_date.drop('dateExtracted', axis=1, inplace=True)
    trans_split_text_date.rename(columns = {0:"dateExtracted"},inplace=True)

    # define the regular expression pattern
    pattern = re.compile(r'\d{1}\.\d+|\d{2}\.\d+|\d{3}\.\d+|\d{1,2},\d+\.\d+')

    # loop through the strings and find matches
    df_for_amt = pd.DataFrame()


    for index, row_value in trans_split_text_date["txnExtracted"].items():
        matches = re.findall(pattern, row_value)
        if matches:
            df_to_append = pd.DataFrame({'Amt': [matches[0]]})
            df_for_amt = pd.concat([df_for_amt,df_to_append],ignore_index=True)


    df_for_amt = pd.concat([df_for_amt,trans_split_text_date],ignore_index=False,axis=1)
    df_for_amt = df_for_amt[['dateExtracted', 'txnExtracted', 'Amt','Bank']]
    df_for_amt = df_for_amt.rename(columns={'dateExtracted': 'Date', 'txnExtracted': 'Description', 'Amt': 'Amt', 'Bank':'Bank'})
    df_for_amt['Amt'] = pd.to_numeric(df_for_amt['Amt'], errors='coerce')
    df_for_amt['Amt'].astype(float)
    df_for_amt['Amt']= -df_for_amt['Amt']

    return df_for_amt



def combine_statements(filename1,filename2,filename3):
    
    global combine
    
    dbs = extract_dbs(filename1)
    citi = extract_citi(filename2)
    hsbc = extract_hsbc(filename3)
    combine = pd.concat([dbs,citi,hsbc],axis=0)
    
    
    category = [
    #transport
    combine.Description.str.lower().str.contains('parking|bus'), 
    #eating out
    combine.Description.str.lower().str.contains("bluelabelpizzaandw|guzmanygomez"), 
    #phone bill
    combine.Description.str.lower().str.contains('giga'), 
    #groceries
    combine.Description.str.lower().str.contains('fairpricefinest|coldstorage'),
    #health and wellness
    combine.Description.str.lower().str.contains('qbhouse|guardian|watson'), 
    #entertainment
    combine.Description.str.lower().str.contains('goldenvillage'), 
    #shopping
    combine.Description.str.lower().str.contains('muji|uniqlo|shopee'), 
    #fee
    combine.Description.str.lower().str.contains('ccyconversionfee|giropayment|interest'), 
    #misc
    combine.Description.str.lower().str.contains('hardwarestore'),
    #transfers
    combine.Description.str.lower().str.contains('top-up to paylah!|maxed out from paylah!|i-bank')
    ]

    category_values = [
        'Transport',
        'Eating out',
        'Phone bill',
        'Groceries',
        'Health & wellness',
        'Entertainment',
        'Shopping',
        'Fees',
        'Misc',
        'Transfers'
    ]
    
    combine['Category'] = np.select(category, category_values, default='?')
    combine['Include Txn?'] = 'Yes'
    
    new_index = ['Date', 'Description', 'Amt', 'Bank', 'Category','Include Txn?']

    combine = combine.reindex(new_index, axis=1)
    
    # 10 june i had an error InvalidJSONError: Out of range float values are not JSON compliant 
    # this was due to combine['Amt'] column having a NaN. and this was further validated with my googling that
    # NaN values would cause that error. The below fillna will solve this issue for all columns.
    combine = combine.fillna('') 
    
    return combine

def export_to_sheet(month, combine):
    
    #select worksheet
    wks = sh.worksheet(f'{month}')
    
    #export data from pandas to google sheets
    # you need raw=False else you will see ' infront of your values and you cant do sum or anything
    
    wks.update('A6',[combine.columns.values.tolist()] + combine.values.tolist(),raw=False)

    #format cells
    wks.format('1:100', {'textFormat': {'bold': False}})
    wks.format('6', {'textFormat': {'bold': True}})
    wks.format("A1", {
        "backgroundColor": {
          "red": 0.0,
          "green": 0.0,
          "blue": 0.0
        },
        "horizontalAlignment": "CENTER",
        "textFormat": {
          "foregroundColor": {
            "red": 1.0,
            "green": 1.0,
            "blue": 1.0
          },
          "fontSize": 12,
          "bold": True
        }
    })
    wks.format("A6:F6", {
        "backgroundColor": {
          "red": 0.0,
          "green": 0.0,
          "blue": 0.0
        },
        "horizontalAlignment": "CENTER",
        "textFormat": {
          "foregroundColor": {
            "red": 1.0,
            "green": 1.0,
            "blue": 1.0
          },
          "fontSize": 12,
          "bold": True
        }
    })
    
    print('data exported to sheets')
    
combine_statements('DBS_june23.csv','CITI_june23.pdf','HSBC_june23.pdf')
export_to_sheet('june',combine)