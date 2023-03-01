--1a)
	CREATE VIEW duplicate_task AS 
	SELECT * FROM data WHERE taskname 
		IN (SELECT taskname  FROM data
	GROUP BY TaskName
	HAVING COUNT(taskname)>1)
--1b)
	-- get the row number where there is duplicate task name
	SELECT *,
         ROW_NUMBER() OVER (PARTITION BY taskname ORDER BY lastupdatedat DESC) AS rn into newData
  	FROM data


	--split table into two with rn=1 and rn=2	
	SELECT id, taskname, duedate,lastupdatedat, ModifiedDueDateStatus, rn into new1
	FROM newData  where rn=1
	SELECT id, taskname, duedate,lastupdatedat, ModifiedDueDateStatus, rn into new2
	FROM newData  where rn=2

	--Join new tables and based on duedate give modifiedduedatestatus as yes or no
	SELECT n1.id,n1.taskname,n1.duedate,n1.lastupdatedat,n1.rn, 
	CASE WHEN n1.duedate = n2.duedate  THEN 'No' else 'Yes' END AS ModifiedDueDateStatus 
	INTO finalData 
	FROM new1 n1 JOIN new2 n2 ON 
	n1.taskname = n2.taskname

	--join the result with main table
	SELECT c1.id, c1.taskname, c1.duedate, c1.lastupdatedat, c1.rn, fd.ModifiedDueDateStatus 
	INTO resultData 
	FROM newData c1 FULL OUTER JOIN finalData fd ON c1.taskname = fd.taskname WHERE c1.rn=1

	--Join with data from sheet2
	SELECT * FROM resultData LEFT JOIN subdata ON resultData.id = ParentId


--1c)
	
	ALTER TABLE data 
	ADD [site-department] varchar(50)

	UPDATE data SET [site-department] = CONCAT(site,'-',department)
	
--1d)
	SELECT 
	substring(CONVERT(nvarchar, Duedate, 113),1,(len(CONVERT(nvarchar, Duedate, 113))-4)) as duedate,
	substring(CONVERT(nvarchar, closedat, 13),1,(len(CONVERT(nvarchar, closedat, 13))-4)) as closedat,
	substring(CONVERT(nvarchar, lastupdatedat, 13),1,(len(CONVERT(nvarchar, lastupdatedat, 13))-4)) as lastupdatedat,
	substring(CONVERT(nvarchar, createdat, 13),1,(len(CONVERT(nvarchar, createdat, 13))-4)) as createdat
	FROM data

--1e)
	UPDATE data SET SOURCE = 'NA' 
	WHERE SOURCE = 'NULL'

--1f)
	SELECT * FROM duplicate_task ORDER BY LastUpdatedAt DESC

--1g)
	ALTER TABLE Data
	ADD RiskLevel INT

	UPDATE Data
	SET RiskLevel =
	CASE 
  		WHEN RiskMagnitude = '1 - Trivial' THEN 1
  		WHEN RiskMagnitude = '2 - Tolerable / Tolerável' THEN 2
  		WHEN RiskMagnitude = '3 - Moderada' THEN 3
  		WHEN RiskMagnitude = '4 - Importante / Significativo' THEN 4
	END

--2)
	CREATE PROCEDURE GetDataByName (@Name VARCHAR(50))
	AS
	BEGIN
    		SELECT ROW_NUMBER() OVER (ORDER BY date DESC) AS SUB_ID, *
    		FROM SubData
    		WHERE Name = @Name
	END

--3)
	WITH cte AS(
	SELECT id,created_by_name, created_at, questions, options
	FROM
	(
	SELECT  id,created_by_name, created_at, carts_working_condition,
	aisles_free_of_obstacles, pallet_storage_guidelines, fire_extinguishers_signage, 
	pallets_damage_identification, fire_ext_inspect_hydraulic_test, evacuation_routes_signage_access,
	rk_aisle_blocking_system_working, rk_no_coexistence_of_mobile_equip
	FROM sheet3) OrigTable
	UNPIVOT
	(options FOR questions IN (carts_working_condition, aisles_free_of_obstacles ,
	pallet_storage_guidelines , fire_extinguishers_signage ,
	pallets_damage_identification , fire_ext_inspect_hydraulic_test ,
	evacuation_routes_signage_access , rk_aisle_blocking_system_working,
	rk_no_coexistence_of_mobile_equip)
	) 
	AS Unpivot_sheet3 
	)
	SELECT * FROM cte WHERE options <> 'NULL'

--4)
	SELECT Name from subdata WHERE Name like '%M%M%'
	--or
	SELECT Name FROM subdata
	WHERE CHARINDEX('M', Name) <> 0 AND CHARINDEX('M', Name, CHARINDEX('M', Name) + 1) <> 0

--5a)
	ALTER TABLE visitdata
	ADD Date varchar(50)

	UPDATE visitdata
	SET Date = CONCAT(day, ' ' ,LEFT(month, 3), ' ',year)

	//check for invalid dates

	update visitdata
	SET date = NULL where isdate(date)=0

	ALTER TABLE visitdata
	ALTER COLUMN Date DATE

	UPDATE visitdata
	SET Date = CONVERT(datetime, Date, 106)

--5b)
	ALTER TABLE visitdata
	ADD NewDate AS DATEADD(dd, 10, Date)

--5c)
	//created a view containing day of week as number
	create view longest_time_spent as 
	SELECT time_spent, DATEPART(dw, date) AS DayOfWeek 
	FROM visitdata

	//cte for partitioning by day of week
	WITH CTE AS (
    		SELECT time_spent, dayofweek,
           	ROW_NUMBER() OVER (PARTITION BY dayofweek ORDER BY time_spent DESC) AS rn
    		FROM longest_time_spent
	)
	
	//return max time spend during weekdays
	SELECT time_spent, dayofweek
	FROM CTE
	WHERE dayofweek IN (1, 7) AND rn <= 10
	ORDER BY time_spent DESC;

--5d)
	SELECT * FROM visitdata 
	WHERE  DATEPART(month, date) = 1 AND Clicked = 'yes'

--5e)
	SELECT DISTINCT(vistId) FROM visitdata 
	SELECT TOP 3 vistId, Internet_Usage FROM VisitData order by Internet_Usage 

--5f)
	SELECT DATEPART(MONTH, date) AS Month, 
    	COUNT(CASE WHEN clicked = 'Yes' THEN 1 END) AS Clicks
	FROM visitdata
	GROUP BY DATEPART(MONTH, date);

--5g)
	SELECT *, DATENAME(WEEKDAY, date) AS DayofWeek
	FROM visitdata
	WHERE internet_usage > 230 AND time_spent > 60 and Date IS NOT NULL

--5e)
	WITH CTE AS (
    	SELECT *, ROW_NUMBER() OVER (ORDER BY time_spent DESC) AS rn
    	FROM visitdata
	)
	SELECT * FROM CTE
	WHERE rn > 5 ORDER BY time_spent DESC;
--5f)
	SELECT DATEPART(MONTH, date) AS Month, 
    	COUNT(CASE WHEN clicked = 'Yes' THEN 1 END) AS Clicks
	FROM visitdata
	GROUP BY DATEPART(MONTH, date);

--5g)
	SELECT *, DATENAME(WEEKDAY, date) AS DayofWeek
	FROM visitdata
	WHERE internet_usage > 230 AND time_spent > 60 and Date IS NOT NULL

--5h)
	WITH CTE AS (
    	SELECT *, ROW_NUMBER() OVER (ORDER BY time_spent DESC) AS rn
    	FROM visitdata
	)
	SELECT * FROM CTE
	WHERE rn > 5 ORDER BY time_spent DESC;