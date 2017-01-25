#Change these parameters
X_YEARS = 3 #THIS CHANGES HOW MANY YEARS BACK IT LOOKS FOR STOCK DATA, BUT NOT EARNINGS DATA
DAYS_SINCE_MARKET_DAY = 1 #DON'T TOUCH UNLESS IT GIVES YOU AN ERROR, THEN ENTER RANDOM NUMBERS 0-6 UNTIL IT WORKS. AFTER TODAY 0 SHOULD WORK EXCEPT ON WEEKENDS

#Don't change anything below this line
#-----------------------------------------

import requests
from datetime import date

old = (str(date.today().month - 1), str(date.today().day - DAYS_SINCE_MARKET_DAY), str(date.today().year - X_YEARS))

def get_change_ni(symb):
  url = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symb + "&reportType=is&period=12&dataType=A&order=asc&columnYear=5&number=1"
  IS = requests.get(url).text.split(',')
  i = 1
  for line in IS:
    if "Net income" in line:
      break
    i += 1
  if i >= len(IS):
    return
  if IS[i] != None:
    curr = ""
    for char in IS[i + 5]:
      if char in "0123456789":
        curr += char
      else:
        break
    return (IS[i + 2], curr)
  
def mapper(symb):
  "Given a stock symbol, return a list containing SYMB, x_years performance, and PE"
  quote = requests.get("http://finance.yahoo.com/d/quotes.csv?s=" + symb + "&f=l1r")
  price, PE = quote.text.split(',')
  if price != "N/A":
    price = float(price)
  if PE != "N/A\n":
    PE = float(PE)
  else:
    PE = "N/A"
  #MAKE IT WORK FOR NON-MARKET DAYS
  old_price_url = "http://ichart.finance.yahoo.com/table.csv?g=d&ignore=.csv&f=" + old[2] + "&e=" + old[1] + "&c=" + old[2] + "&b=" + old[1] + "&a=" + old[0] + "&d=" + old[0] + "&s=" + symb
  old_price = requests.get(old_price_url)
  try:
    assert type(PE) is not str
    old_price_num = float(old_price.text.split(',')[-1]) #10 for close, -1 for adj close
  except:
    #For some reason, this is not returning an entry. Leading causes are a none market day and the company did not exist back then
    return symb + ',' + 'NA' + ',' + 'NA'
  eps = get_change_ni(symb)
  if eps == None or eps[0] == "" or eps[1] == "":
    return symb + ',NA,NA'
  change_pe = int(eps[0]) - int(eps[1]) #- PE
  change = old_price_num - price
  return symb + ',' + str(change_pe) + ',' + str(change)

def filt(stats):
  _, pe, price = stats.split(',')
  if price != 'NA' and pe != 'NA':
    if float(price) > 0 and int(pe) < 0:
      return True 
  return False
nasdaq = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
nyse = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download"
exchanges = {"nasdaq" : nasdaq, "nyse" : nyse}

dbutils.fs.mkdirs("dbfs:/tmp")

for name, exchange in exchanges.items():
  csv_data = requests.get(exchange)  
  localpath="/tmp/" + name + "_raw.csv"
  file = open(localpath, 'w')
  file.write(csv_data.text)
  file.close()
  df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('file:' + localpath)
  symbs = df.select('Symbol').collect()
  data = []
  for symb in symbs:
    data.append(symb['Symbol'])
  rdd = sc.parallelize(data)
  mapRDD = rdd.map(mapper).filter(filt).collect()
  for row in mapRDD:
    print(row.split(',')[0])
