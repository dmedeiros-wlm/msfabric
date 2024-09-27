CREATE   VIEW [Silver].[vCalendar] 
    AS (
        SELECT
            [date] as Date,
            [daynum] as DayNum,
            [dayofweekname] as DayOfWeek,
            [dayofweeknum] as DayOfWeekNum,
            [monthname] as Month,
            [monthnum] as MonthNum,
            [monthyear] as MonthYear,
            [quartername] as Quarter,
            [quarternum] as QuarterNum,
            [year] as Year
        FROM
            [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Calendar]
    )