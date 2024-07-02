import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
import time
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery

#basic setting
looptimes = random.randint(1,3)
waittimes = random.uniform(2,3)
asecond = random.uniform(0.9,1.3)
table_data = []
table_head = []
table_streaming = []
account = 'abc@gmail.com'
password = '123abc'
amounts = 2000 #調整提取table數量
times = int(amounts/100)
yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
url = "https://app.respond.io/reports/conversations"
Path = '/your_path/chromedriver'
export_path = '/your_path/Downloads/conversation_list.csv'
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")
service = webdriver.ChromeService(executable_path=Path)
driver = webdriver.Chrome(service=service, options=options)
service_account_json = "/your_path/key.json" 
client = bigquery.Client.from_service_account_json(service_account_json)
# 指定目標資料集和表格
table_id = 'project.dataset.table'

#functions
def login():
    try:
        findaccount = driver.find_element(By.XPATH, '//*[@id="input-7"]')
        findaccount.send_keys(account)
        findpassword = driver.find_element(By.XPATH, '//*[@id="input-9"]')
        findpassword.send_keys(password)
        findsubmit = driver.find_element(By.XPATH, '//*[@id="authContainer"]/div[1]/div[2]/div/div/form/div[4]/button')
        findsubmit.click()
        time.sleep(asecond)

    except Exception as e:
        # 異常發生時的處理
        print(f"發生錯誤: {e}")
        print("login error!")
        time.sleep(2)

def display_num():
    try:
        findperpage = driver.find_element(By.XPATH, '//div[@class="dls-flex dls-flex-col dls-items-start dls-space-y-4 dls-txt-body"]')
        ActionChains(driver).scroll_to_element(findperpage).perform()
        findperpage.click()
        time.sleep(1)
        find100 = driver.find_elements(By.XPATH, '//div[@class="v-list-item v-list-item--link v-theme--light v-list-item--density-default v-list-item--one-line v-list-item--variant-text"]')
        click100 = find100[2]
        driver.execute_script("arguments[0].click();", click100)
        time.sleep(asecond)
    except Exception as e:
        # 異常發生時的處理
        print(f"發生錯誤: {e}")
        print("perpage_click error!")
        time.sleep(2)

def retrieve_table():
    global table_streaming
    try:
        findtable = driver.find_element(By.TAG_NAME, 'table')
        rows = findtable.find_elements(By.TAG_NAME, 'tr')
        table_streaming = len(rows)
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'th')
            cols_text = [col.text for col in cols]
            table_head.append(cols_text)

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            cols_text = [col.text for col in cols]
            table_data.append(cols_text)
        return table_streaming
    except Exception as e:
        print(f"發生錯誤: {e}")
        print("retrieve_table error!")
        time.sleep(2)

def retrieve_loop():
    try:
        for i in range(1, times):
            print(f"Progression: {i*100}/{amounts}...")
            findtable = driver.find_element(By.TAG_NAME, 'table')
            rows = findtable.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, 'td')
                cols_text = [col.text for col in cols]
                table_data.append(cols_text)
            next()
            time.sleep(asecond)
        print("End of retrieval!!!\n")
        print(f'retrieved data lenth:{len(table_data)}')
    except Exception as e:
        print(f"發生錯誤: {e}")
        print("retrieve_tables error!")
        time.sleep(2)
        return 0

def next():
    try:
        findarrow = driver.find_element(By.XPATH, '//li[@class="v-pagination__next"]')
        findarrow.click()
        #clickarrow = findarrow[len(findarrow)-1]
        #driver.execute_script("arguments[0].click();", clickarrow)

    except Exception as e:
        # 異常發生時的處理
        print(f"發生錯誤: {e}")
        print("next error!")
        time.sleep(2)

def display_missing():
    for col in df1.columns.tolist():          
        print('{} column missing values: {}'.format(col, df1[col].isnull().sum()))
    print('\n')

def data_cleaning():
    global df
    global df1
    global df2
    print(f"Today is {datetime.now()}")
    print(f"Format of yesterday: {yesterday}")
    df = pd.DataFrame(data = table_data[1:], columns = table_head[0])
    print(f'orginal_lenth:{len(df)}')
    df1 = df.dropna(subset=["Closed Timestamp"]).copy()
    print(f'cleaned_lenth:{len(df1)}')
    display_missing()
    df1['Closed Timestamp'] = pd.to_datetime(df['Closed Timestamp'], format='%B %d, %Y %I:%M %p')
    df2 = df1[(df1['Closed Timestamp'] >= yesterday) & (df1['Closed Timestamp'] < yesterday + timedelta(days=1))]
    print(f"table of all: {df1.head(2)}")
    print(f"table of yesterday: {df2.head(2)}")
    print("Data of list have been saved")

def upload():# 將DataFrame寫入BigQuery
    try:
        job = client.load_table_from_dataframe(df2, table_id)
        job.result()
        print("上傳至Big query 已完成!!")
        query_check = f"SELECT * FROM `{table_id}` ORDER BY `Closed Timestamp` DESC LIMIT 10"
        query_job = client.query(query_check)
        for row in query_job:
            print(row)
    except Exception as e:
        print("Error while uploading to Big Query", e)


#main
driver.get(url)
login()
time.sleep(3)
display_num()
retrieve_table()
retrieve_loop()
data_cleaning()
upload()



