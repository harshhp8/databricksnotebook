-- Databricks notebook source
create or refresh streaming live table bronze_tab
comment "bronze table"
as select * from cloud_files("/harsh", "csv")

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_tab(
  CONSTRAINT valid_order_number EXPECT (Min_Order_Amount IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Silver table"
AS
  SELECT f.Unnamed, f.Area, f.Zone_Name,f.Restaurant_Id,f.Restaurant_Name,
         f.Address,f.Latitude,f.Longitude,f.Cuisines,f.Delivery_Time,f.Details,
         f.Link,f.Ratings,f.Reviews,f.Timings,f.Delivery_Fees,f.Min_Order_Amount,
         f.Area_Lat,f.Area_Long,f.Api_Lat,f.Api_Long,f.id
  FROM STREAM(LIVE.bronze_tab) f

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_tab
COMMENT "Gold table"
AS
  SELECT Zone_Name FROM LIVE.silver_tab GROUP BY Zone_Name

-- COMMAND ----------


