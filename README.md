# data_pipeline

Clinic table to Bigquery：從診所系統提取資料並上傳至BigQuery
這個腳本主要從診所端的顧客系統資料庫提取數據，經過清洗和篩選後上傳至BigQuery資料集中的特定表格。

資料提取：從診所顧客系統資料庫使用SQL語句篩選多張表的特定欄位。
資料清洗：使用Python Pandas進行數據清洗和篩選。
資料匯入：將清洗過的數據上傳至BigQuery資料集中的特定表格。

Appointment table to Bigquery：從客服系統網站提取資料並上傳至BigQuery
這個腳本主要從客服系統網站提取數據，經過清洗和篩選後上傳至BigQuery資料集中的特定表格。

資料提取：使用爬蟲技術從客服系統網站提取數據。
資料清洗：使用Python Pandas進行數據清洗和篩選。
資料匯入：將清洗過的數據上傳至BigQuery資料集中的特定表格。

這兩個腳本分別處理不同來源的數據，經過清洗後將其匯入至BigQuery，從而實現數據的集中管理和分析。

# data_flow

![Flowchart-2](https://github.com/eatinglai/data_pipeline/assets/139863864/3c06469e-554f-4046-8b11-94d26fc7ab0d)


