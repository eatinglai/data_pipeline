# data_pipeline

Clinic table to Bigquery：從診所系統提取資料並上傳至BigQuery，主要從診所端的顧客系統資料庫提取數據，經過清洗和篩選後上傳至BigQuery資料集中的特定表格。

>資料提取：從診所顧客系統資料庫使用SQL語句篩選多張表的特定欄位。
>
>資料清洗：使用Python Pandas進行數據清洗和篩選。
>
>資料匯入：將清洗過的數據上傳至BigQuery資料集中的特定表格。

Appointment table to Bigquery：從客服系統網站提取資料並上傳至BigQuery，主要從客服系統網站提取數據，經過清洗和篩選後上傳至BigQuery資料集中的特定表格。


>資料提取：使用selenium 爬蟲從客服系統網站提取數據。
>
>資料清洗：使用Python Pandas進行數據清洗和篩選。
>
>資料匯入：將清洗過的數據上傳至BigQuery資料集中的特定表格。

這兩個腳本分別處理不同來源的數據，經過清洗後將其匯入至BigQuery，解決數據流程自動化及集中管理的情境需求。

# data_flow
![Flowchart-3](https://github.com/eatinglai/data_pipeline/assets/139863864/1431487f-4811-4117-830e-fd06d8eb175a)





