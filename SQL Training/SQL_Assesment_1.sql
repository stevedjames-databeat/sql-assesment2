SELECT * FROM NewsNation2022Goals order by [visit date]

SELECT [visit date], [platform], [Device Category], [Channel Grouping], [page views] INTO source FROM NewsNation2022Goals 
WHERE [visit Date] IS NOT NULL ORDER BY [visit date]  

DROP TABLE SOURCE
CREATE VIEW source_1 AS SELECT [visit date], [platform], [Device Category], [Channel Grouping], [page views] FROM NewsNation2022Goals 
WHERE [visit Date] IS NOT NULL ORDER BY [visit date] OFFSET 0 ROWS
SELECT * INTO source FROM source_1
DROP VIEW source_1
SELECT * FROM SOURCE

ALTER TABLE source ADD [goal date] date
ALTER TABLE source ADD [goal weekday] int
ALTER TABLE source ADD [month+weekday] varchar(30)
ALTER TABLE source ADD [visitmonth+weekday] varchar(30)
ALTER TABLE source ADD [goal match] varchar(30)
ALTER TABLE source ADD [% PVs Per Month] varchar(20)
ALTER TABLE source ADD [Avg % PVs for Month by Dimensions] varchar(20)
ALTER TABLE source ADD [Avg % PVs per Month by Day of Week and dimensions] varchar(20)
ALTER TABLE source ADD [Avg % PVs per Month by Day of Week and dimensions1] varchar(20)
ALTER TABLE source ADD [goal month] char(2)
ALTER TABLE source ADD [goal PVs] int
ALTER TABLE source ADD id int identity(1,1)

--Column M
WITH goal_date AS
( SELECT id,CONVERT(DATE, CASE WHEN YEAR([visit date])=2021 THEN DATEADD(MONTH, 12, [visit date]) 
ELSE [visit date] 
END ) AS [goal date] FROM source)
UPDATE source SET [goal date] = (SELECT [goal date] from goal_date where goal_date.id = source.id)

--Column N
WITH month as
( SELECT id, CONVERT(char(2), [goal date], 101)  as Month from source)
Update source SET [goal month] = (SELECT month from month where month.id = source.id)

--Colmun O
SET DATEFIRST 7

WITH goal_weekday as
(SELECT id, DATEPART(dw,[goal date]) as [goal weekday] FROM source)
Update source SET [goal weekday] = (SELECT [goal weekday] from goal_weekday where goal_weekday.id = source.id)

--Column P
WITH month_weekday as
(SELECT id,CONCAT([goal month],[goal weekday],platform,[device category],[channel grouping]) as [month+weekday] from source)
UPDATE source SET [month+weekday] = (SELECT [month+weekday] from month_weekday WHERE month_weekday.id = source.id)

--Column S
WITH goal_match as
(SELECT id,CONCAT([goal month],platform,[device category]) as [goal match] from source)
UPDATE source SET [goal match] = (SELECT [goal match] from goal_match WHERE [goal_match].id = source.id)

--COlumn K
WITH [%_PVs_per_month] as
(SELECT id,CONCAT(CONVERT(decimal(7,6),[page views]*100/sum([page views]) over(partition by month([visit date]))),'%') AS [% PVs Per Month]  from source )
UPDATE source SET [% PVs Per Month] = (SELECT [% PVs Per Month] from [%_PVs_Per_Month] where [%_PVs_Per_Month].id = source.id)

--COlumn L
WITH [Avg % PVs per Month by Day of Week and dimensions] as
(SELECT id, CONCAT(AVG(CONVERT(decimal(7,6),TRIM('%' FROM [% PVs Per Month]))) OVER(partition by [month+weekday]),'%') as [Avg % PVs per Month by Day of Week and dimensions] from source)
UPDATE source SET [Avg % PVs per Month by Day of Week and dimensions] = (
SELECT [Avg % PVs per Month by Day of Week and dimensions] FROM [Avg % PVs per Month by Day of Week and dimensions] 
WHERE
[Avg % PVs per Month by Day of Week and dimensions].id = source.id)

--Column E
WITH [visitmonth+weekday] as
(SELECT id, CONCAT([goal month],DATEPART(dw,[visit date]),[platform],[device category],[channel grouping]) as[visitmonth+weekday] from source)
UPDATE source SET [visitmonth+weekday] = (SELECT [visitmonth+weekday] from [visitmonth+weekday] where [visitmonth+weekday].id = source.id)

SELECT * FROM source

--Column Q
WITH [Avg % PVs per Month by Day of Week and dimensions1] AS
(
SELECT DISTINCT s2.id,s2.[visit date], AVG(CONVERT(decimal(7,6),TRIM('%' FROM s1.[% PVs Per Month]))) OVER (PARTITION BY s1.[month+weekday]) 
AS [Avg % PVs per Month by Day of Week and dimensions1] 
from source s1 join source s2 on s1.[visitmonth+weekday] = s2.[month+weekday] 
order by s2.[visit date] offset 0 rows
)
SELECT * FROM [Avg % PVs per Month by Day of Week and dimensions1]
UPDATE source SET [Avg % PVs per Month by Day of Week and dimensions1] = (SELECT CONCAT([Avg % PVs per Month by Day of Week and dimensions1],'%') 
from [Avg % PVs per Month by Day of Week and dimensions1] where [Avg % PVs per Month by Day of Week and dimensions1].id = source.id)

--COlumn R
WITH [Avg % PVs for Month by Dimensions] as (
SELECT id,Convert(decimal(7,6),TRIM('%' FROM [Avg % PVs per Month by Day of Week and dimensions1]))*100/sum(convert(decimal(7,6),TRIM('%' FROM [Avg % PVs per Month by Day of Week and dimensions1]))) over(partition by [goal month]) as [Avg % PVs for Month by Dimensions]
from source order by [visit date] offset 0 rows
)
UPDATE source SET [Avg % PVs for Month by Dimensions] = (SELECT CONCAT(CONVERT(decimal(7,6),[Avg % PVs for Month by Dimensions].[Avg % PVs for Month by Dimensions]),'%') 
FROM [Avg % PVs for Month by Dimensions] WHERE [Avg % PVs for Month by Dimensions].id = source.id)

SELECT * FROM NewsNation2022Goals
	
SELECT F21, F22, F23, F24, F25 INTO mapping_table FROM NewsNation2022Goals WHERE F21 IS NOT NULL order by F21 offset 2 rows
EXEC sp_rename 'mapping_table.F21','Month'
EXEC sp_rename 'mapping_table.F22','Platform'
EXEC sp_rename 'mapping_table.F23','Device Category'
EXEC sp_rename 'mapping_table.F24','Match'
EXEC sp_rename 'mapping_table.F25','PVs'

ALTER TABLE mapping_table ALTER COLUMN Match varchar(30)

DROP table mapping_table
DELETE FROM mapping_table where PVs IS NULL

SELECT * FROM mapping_table
UPDATE mapping_table SET [Match] = CONCAT([Month],[platform],[Device Category])

--COlumn T
WITH [Goal PVs] AS(
SELECT source.id,[goal match],round(convert(decimal(7,6),trim('%'from source.[Avg % PVs for Month by Dimensions]))*mt.[PVs]/sum(convert(decimal(7,6),trim('%' from source.[Avg % PVs for Month by Dimensions])))
Over(partition by [goal match]),0) AS [Goal PVs] from source left join mapping_table mt on mt.[match]=source.[goal match] order by [visit date] offset 0 rows
)
UPDATE source SET [Goal PVs] = (SELECT [Goal PVs] from [Goal PVs] WHERE [Goal PVs].id = source.id)

--2
CREATE PROCEDURE GetGoalPageViews
AS
BEGIN
SELECT [Goal PVs] FROM source
END

EXEC GetGoalPageViews