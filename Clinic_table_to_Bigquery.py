#need to install following modules: mysql, mysql-connector-python, pandas, google-cloud-bigquery
import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import date, timedelta
from google.cloud import bigquery

service_account_json = "/your_path/key.json"
client = bigquery.Client.from_service_account_json(service_account_json)
table_id = 'project.dataset.table' #目標Bigquery資料集和表格
yesterday = str(date.today() - timedelta(days=2))
query = f"select d.name as '診所', ddate as '看診日期',case when isnp = '1' then 'y' else '' end as '新客', c.sfname as '醫師姓名' , b.cusname as '患者姓名' , b.cusbirthday as '出生日期' ,a.sch_note as '看診備註' from (select clinic_id ,ddate ,isnp, drno1 ,cussn ,cusno ,seqno,sch_note from headquarters.registration where ddate ='{yesterday}' and seqno != '000' and deldate is null)a left join(select * from headquarters.customer)b on a.clinic_id = b.clinic_id and a.cussn = b.cussn left join(select * from headquarters.staff where deleted_at is null) c on a.clinic_id = c.clinic_id and a.drno1 = sfsn left join(select * from lesson.Clinic)d on a.clinic_id = d.clinic_id order by a.ddate , a.clinic_id ,a.seqno"
table_head = ['診所','看診日期', '新客', '醫師姓名', '患者姓名', '出生日期', '看診備註']
export_path = "/your_path/output/"

#connect to mysql & query data
def sql_proc():
    global record
    try:
        # 建立連接
        print(f"Query clinic list of {yesterday}")
        connection = mysql.connector.connect(
            host='IP',
            database='database',
            user='user',
            password='user123456',
            port='3306'
        )

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute(query)
            #cursor.execute("select database();")
            record = cursor.fetchall()
            print("You're connected to database.")

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        # 關閉數據庫連接
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

#data processing
def data_proc():
    global df
    df = pd.DataFrame(data = record, columns = table_head)
    df.to_csv(export_path+f"/診所名單{yesterday}.csv")
    df1 = df[0:4]
    print(f"診所名單輸出完成！\n名單長度:{len(df)}\n\n前五筆資料:\n{df1.to_string(header=False)}")

#upload to google sheet
def upload_sheet():
    try:
        # 將DataFrame寫入BigQuery
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        query_check = f"SELECT * FROM `{table_id} LIMIT 10`"
        query_job = client.query(query_check)
        for row in query_job:
            print(row)
        print("上傳至Big query 已完成!!")
    except Exception as e:
        print("Error while uploading to Big Query", e)

#main
sql_proc()
data_proc()
upload_sheet()
