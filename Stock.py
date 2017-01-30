import requests
import csv
import StringIO
"""We now represent stocks as a dictionary due to spark quirks"""
def create_new_stock(symb, ind):
  return {'PE': None, 'PB' : None, 'PEG' : None, 'Cap' : None, 'Vol' : None, 'Class' : None, 'Symb': symb, 'Ind' : ind, 'ROE' : None, 'DE' : None}
def fetch_data_yahoo(stock, params = "vrr5p6j1", fields = ['Vol', 'PE', 'PEG', 'PB', 'Cap']):
    """This fetches the specified data from yahoo finance."""
    url = "http://finance.yahoo.com/d/quotes.csv?s=" + stock['Symb'] + "&f=" + params
    res = requests.get(url)
    vals = res.text.split(',')
    for field, val in zip(fields, vals):
      stock[field] = val
def update(stock):
  """This updates a stock. It gets all data from yahoo, then classifies it by Market capitalization."""
  fetch_data_yahoo(stock)    
  if stock['Cap'] != None:
      calc_class(stock)
def calc_class(stock):
  """This method calulates if the company is small, mid, or large cap based on market value"""
  if 'M' in stock['Cap']:
    val = float(stock['Cap'].replace('M', ""))
    if val >= 250:
      stock['Class'] = 'Small'
    return
  if 'B' in stock['Cap']:
    val = float(stock['Cap'].replace('B', ""))
    if val >= 10:
      stock['Class'] = 'Large'
    elif val >= 1:
      stock['Class'] = 'Mid'
"""From Python2.7 docs"""
def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]
"""From Python2.7 docs"""
def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')
def get_morningstar_ratios(stock):
  res = requests.get('http://financials.morningstar.com/ajax/exportKR2CSV.html?t=' + stock['Symb'])
  f = StringIO.StringIO(res.text)
  csvData = unicode_csv_reader(f, delimiter=',')
  for line in csvData:
    if 'Return on Equity %' in line:
      """This gets average ROE data over the past two years"""
      stock['ROE'] = float(line[-1])
    if 'Debt/Equity' in line:
      stock['DE'] = float(line[-1])
def mapper(stock):
  update(stock)
  return stock
def filter_de(stock, thresh = .5):
  """This filters for stocks with a low debt to equity"""
  """ASSUMPTION: FILTER_ROE WAS RUN BEFORE THIS, OR GET_MORINGSTAR_RATIOS WAS CALLED"""
  """TODO: ABSTRACT USING HOF"""
  return stock['DE'] <= thresh
def filter_cap(stock):
  """This filters for small cap only"""
  return stock['Class'] == 'Small'
def filter_roe(stock, thresh = 15):
  """This filters for stocks with a high ROE"""
  try:
    get_morningstar_ratios(stock)
  except:
    #No ROE available
    pass
  return stock['ROE'] >= thresh
