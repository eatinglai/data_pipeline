# data_pipeline

使用Python和Selenium自動化一系列網頁操作，並將數據導入Google BigQuery。

模組： random、selenium、datetime、pandas 和 google.cloud.bigquery ，用於網頁操作、時間處理和數據操作。

基本設定：定義各種變量，如執行次數、等待時間、賬戶信息、數據表等，並設置Chrome瀏覽器的選項。

登錄功能：自動填寫並提交登錄表單，以訪問指定的網頁。

顯示每頁數據：調整網頁設置以顯示更多數據，每頁顯示100條記錄。

提取表格數據：從網頁表格中提取數據，存儲在列表中。

循環提取數據：多次翻頁以提取更多數據，直到達到預設的數據量。

數據清理：清理和處理提取的數據，將其轉換為DataFrame格式，並篩選特定日期的數據。

上傳數據：將清理後的數據上傳至Google BigQuery，並檢查上傳結果。

用於自動化數據收集和處理，實現需要定期從網頁提取數據並存儲到BigQuery中的情境。

# data_flow
