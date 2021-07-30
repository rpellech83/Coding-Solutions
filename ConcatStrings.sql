--Used for SSRS Report for monthly reporting
--Created temp table for the massive monthly batch of repair reciepts
CREATE TABLE #RepairDetail
(
Lot_no NVARCHAR(255),
LIN NVARCHAR(255),
SIZE NVARCHAR(255),
REPAIR NVARCHAR(255)
)
INSERT INTO #RepairDetail
--select statement for sending data to input into temp table
SELECT LOT_NO, LIN, SIZE, REPAIR From RepairDetail_2
Where Month(Date) = @Month and YEAR(Date) = @Year--variables for SSRS
--CTE to use windows function for concat of strings
;WITH Partitioned AS
(
    SELECT 
        Lot_no,
		LIN,
		SIZE,
        REPAIR,
        --this part allow a partition to be created to put all the lots together
		--and order the repairs done by lot
		ROW_NUMBER() OVER (PARTITION BY Lot_no ORDER BY Repair) AS RepairNumber,
        COUNT(*) OVER (PARTITION BY Lot_no) AS RepairCount
    FROM #RepairDetail
	
),
Concatenated AS--this sets up the joining of the CTE to produce final result
(
    SELECT 
        LOT_NO,
		LIN,
		SIZE, 
        CAST(REPAIR AS nvarchar(1000)) AS FullRepair, 
        REPAIR, 
        RepairNumber, 
        RepairCount 
    FROM Partitioned 
    WHERE RepairNumber = 1

    UNION ALL

    SELECT 
        P.LOT_NO,
		P.LIN,
		P.Size,
		--the following creates the concatenated strings as a 
		--field separated by commas for the rpeort layout 
        CAST(C.FullRepair + ', ' + P.REPAIR AS nvarchar(1000)), 
        P.REPAIR, 
        P.RepairNumber, 
        P.RepairCount
    FROM Partitioned AS P
        INNER JOIN Concatenated AS C 
                ON P.LOT_NO = C.LOT_NO 
                AND P.RepairNumber = C.RepairNumber + 1
)
SELECT 
    cast(LOT_NO as NVARCHAR(25)) as LOT_NO
	,LIN
	,SIZE
    ,FullRepair--th final results selected show all the repairs as needed
FROM Concatenated
WHERE RepairNumber = RepairCount
--had to add maxrecursion do to query running over long and terminating before finish
OPTION (MAXRECURSION 5000)
DROP TABLE #RepairDetail--best practice even though this will end with session