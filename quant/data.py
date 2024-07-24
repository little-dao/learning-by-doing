from tqdm import tqdm
import requests
import pandas as pd
import IPython.core.display as display
# View Dataframe as an interactive table in Jupyter Notebook
from itables import init_notebook_mode

# init_notebook_mode(all_interactive=True)

headers = {'User-Agent': "YOUR@EMAIL.HERE"}


def get_all_companies(headers):
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers)
    company_dct = companyTickers.json()
    cik_lst = [str(company_dct[x]['cik_str']).zfill(10) for x in company_dct]

    return cik_lst


def get_filing_data_by_cik(cik, headers):
    companyFacts = requests.get(
        f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
        headers=headers)
    cFacts = companyFacts.json()
    facts = cFacts['facts']

    date_dict = {'Q1': '03-30-',
                 'Q2': '06-30-',
                 'Q3': '09-30-',
                 'FY': '12-31-'}

    Q10K10_tags_values = [(f"{t}_{u}",
                           [(f"{date_dict[i['fp']]}{i['fy']}", i['val'])
                            for i in facts[c][t]['units'][u] if i['form']
                            in ['10-Q', '10-K']
                            ]
                           )
                          for c in facts.keys()
                          for t in facts[c].keys()
                          for u in facts[c][t]['units'].keys()
                          ]

    K10_tags_values = [(f"{t}_{u}",
                        [(f"{date_dict[i['fp']]}{i['fy']}", i['val'])
                         for i in facts[c][t]['units'][u] if i['form']
                         in ['10-K']
                         ]
                        )
                       for c in facts.keys()
                       for t in facts[c].keys()
                       for u in facts[c][t]['units'].keys()
                       ]

    Q10_tags_values = [(f"{t}_{u}",
                        [(f"{date_dict[i['fp']]}{i['fy']}", i['val'])
                         for i in facts[c][t]['units'][u] if i['form']
                         in ['10-Q']
                         ]
                        )
                       for c in facts.keys()
                       for t in facts[c].keys()
                       for u in facts[c][t]['units'].keys()
                       ]

    filing_data = {'name': cFacts['entityName'],
                   'cik': cFacts['cik'],
                   '10Q10K': {k: v for k, v in Q10K10_tags_values},
                   '10K': {k: v for k, v in K10_tags_values},
                   '10Q': {k: v for k, v in Q10_tags_values}
                   }
    # print(filing_data['10K'])
    return filing_data


def convert_dict_to_df(filing_data, form, nan_tolerance=20):
    '''
    filing_data: dict
    form: lst
          ['10K'], ['10Q'], or ['10K', '10Q']
    nan_tolerance: int
                    a integer from 1 - 100
    '''
    form_df = pd.DataFrame()

    if form == ['10K']:

        tenk = filing_data['10K']

        for i in tqdm(tenk.keys()):

            if len(tenk[i]) > 0:

                df = pd.DataFrame(tenk[i], columns=['date', i])
                df.index = pd.to_datetime(df.date)
                df = df.drop(columns=['date'])
                df = df.resample('Y').last()
                length = len(df)
                percent_nan = df.isnull().sum() * 100 / length

                if percent_nan[0] < nan_tolerance:

                    if form_df.empty:
                        form_df = df
                    else:
                        form_df = pd.merge(form_df,
                                           df,
                                           on=['date'],
                                           how='outer')

    elif form == ['10Q']:

        tenq = filing_data['10Q']

        for i in tqdm(tenq.keys()):

            if len(tenq[i]) > 0:

                df = pd.DataFrame(tenq[i], columns=['date', i])
                df.index = pd.to_datetime(df.date)
                df = df.drop(columns=['date'])
                df = df.resample('Q').last()
                length = len(df)
                percent_nan = df.isnull().sum() * 100 / length

                if percent_nan[0] < nan_tolerance:

                    if form_df.empty:
                        form_df = df
                    else:
                        form_df = pd.merge(form_df,
                                           df,
                                           on=['date'],
                                           how='outer')

    elif form == ['10Q', '10K'] or form == ['10K', '10Q']:

        tenqk = filing_data['10Q10K']

        for i in tqdm(tenqk.keys()):

            if len(tenqk[i]) > 0:
                df = pd.DataFrame(tenqk[i], columns=['date', i])
                df.index = pd.to_datetime(df.date)
                df = df.drop(columns=['date'])
                df = df.resample('Q').last()
                length = len(df)
                percent_nan = df.isnull().sum() * 100 / length

                if length > 30 and percent_nan[0] < nan_tolerance:

                    if form_df.empty:
                        form_df = df
                    else:
                        form_df = pd.merge(form_df,
                                           df,
                                           on=['date'],
                                           how='outer')

    else:
        print("Invalid form.")
        print("Vaild inputs are ['10K'], ['10Q'], or ['10K', '10Q']")

    form_df.dropna(axis=1, how='all', inplace=True)
    form_df.dropna(axis=0, how='all', inplace=True)
    form_df = form_df.fillna(0)

    return form_df


cik_lst = get_all_companies(headers)

filing_data = get_filing_data_by_cik(cik_lst[1], headers)

df = convert_dict_to_df(filing_data, form=['10Q', '10K'], nan_tolerance=20)

print(df.head())
