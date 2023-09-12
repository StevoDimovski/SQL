
--------------------------------Kreiranje na tabela Date---------------------------------------------

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Date]') AND type in (N'U'))
		DROP TABLE [dbo].[Date]

		CREATE TABLE [dbo].[Date](
			[DateKey] [int] NOT NULL,
			[Date] [date] NOT NULL,
			[Day] [tinyint] NOT NULL,
			[DaySuffix] [char](2) NOT NULL,
			[Weekday] [tinyint] NOT NULL,
			[WeekDayName] [varchar](10) NOT NULL,
			[IsWeekend] [bit] NOT NULL,
			[IsHoliday] [bit] NOT NULL,
			[HolidayText] [varchar](64) SPARSE  NULL,
			[DOWInMonth] [tinyint] NOT NULL,
			[DayOfYear] [smallint] NOT NULL,
			[WeekOfMonth] [tinyint] NOT NULL,
			[WeekOfYear] [tinyint] NOT NULL,
			[ISOWeekOfYear] [tinyint] NOT NULL,
			[Month] [tinyint] NOT NULL,
			[MonthName] [varchar](10) NOT NULL,
			[Quarter] [tinyint] NOT NULL,
			[QuarterName] [varchar](6) NOT NULL,
			[Year] [int] NOT NULL,
			[MMYYYY] [char](6) NOT NULL,
			[MonthYear] [char](7) NOT NULL,
			[FirstDayOfMonth] [date] NOT NULL,
			[LastDayOfMonth] [date] NOT NULL,
			[FirstDayOfQuarter] [date] NOT NULL,
			[LastDayOfQuarter] [date] NOT NULL,
			[FirstDayOfYear] [date] NOT NULL,
			[LastDayOfYear] [date] NOT NULL,
			[FirstDayOfNextMonth] [date] NOT NULL,
			[FirstDayOfNextYear] [date] NOT NULL,
		PRIMARY KEY CLUSTERED 
		(
			[DateKey] ASC
		)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
		) ON [PRIMARY]

--------------------------------Kreiranje na procedura za popolnuvanje na tabelata Date ---------------------------------------------
CREATE PROCEDURE [dbo].[GenerateTableDate]

	@StartDate date,
	@NumberOfYears int
AS
BEGIN
			-- SET NOCOUNT ON added to prevent extra result sets from
			-- interfering with SELECT statements.
			SET NOCOUNT ON;
			drop table if exists #dim
				DECLARE
				@CutoffDate DATE = (select DATEADD(YEAR, @NumberOfYears, @StartDate))

			-- prevent set or regional settings from interfering with 
			-- interpretation of dates / literals
			SET DATEFIRST 7;
			SET DATEFORMAT mdy;
			SET LANGUAGE US_ENGLISH;

			-- this is just a holding table for intermediate calculations:
			CREATE TABLE #dim
			(
				[Date]       DATE        NOT NULL, 
				[day]        AS DATEPART(DAY,      [date]),
				[month]      AS DATEPART(MONTH,    [date]),
				FirstOfMonth AS CONVERT(DATE, DATEADD(MONTH, DATEDIFF(MONTH, 0, [date]), 0)),
				[MonthName]  AS DATENAME(MONTH,    [date]),
				[week]       AS DATEPART(WEEK,     [date]),
				[ISOweek]    AS DATEPART(ISO_WEEK, [date]),
				[DayOfWeek]  AS DATEPART(WEEKDAY,  [date]),
				[quarter]    AS DATEPART(QUARTER,  [date]),
				[year]       AS DATEPART(YEAR,     [date]),
				FirstOfYear  AS CONVERT(DATE, DATEADD(YEAR,  DATEDIFF(YEAR,  0, [date]), 0)),
				Style112     AS CONVERT(CHAR(8),   [date], 112),
				Style101     AS CONVERT(CHAR(10),  [date], 101)
			);

			-- use the catalog views to generate as many rows as we need
			INSERT INTO #dim ([date]) 
			SELECT
				DATEADD(DAY, rn - 1, @StartDate) as [date]
			FROM 
			(
				SELECT TOP (DATEDIFF(DAY, @StartDate, @CutoffDate)) 
					rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
				FROM
					-- on my system this would support > 5 million days
					sys.all_objects AS s1
					CROSS JOIN sys.all_objects AS s2
				ORDER BY
					s1.[object_id]
			) AS x;
			-- select * from #dim

			INSERT dbo.[Date] ([DateKey], [Date], [Day], [DaySuffix], [Weekday], [WeekDayName], [IsWeekend], [IsHoliday], [HolidayText], [DOWInMonth], [DayOfYear], [WeekOfMonth], [WeekOfYear], [ISOWeekOfYear], [Month], [MonthName], [Quarter], [QuarterName], [Year], [MMYYYY], [MonthYear], [FirstDayOfMonth], [LastDayOfMonth], [FirstDayOfQuarter], [LastDayOfQuarter], [FirstDayOfYear], [LastDayOfYear], [FirstDayOfNextMonth], [FirstDayOfNextYear])
			SELECT
				DateKey       = convert(int, convert(varchar(4),year([date])) + right('0' + convert(varchar(2),month([date])),2) + right('0' + convert(varchar(2),day([date])),2)),
				[Date]        = [date],
				[Day]         = CONVERT(TINYINT, [day]),
				DaySuffix     = CONVERT(CHAR(2), CASE WHEN [day] / 10 = 1 THEN 'th' ELSE 
								CASE RIGHT([day], 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
								WHEN '3' THEN 'rd' ELSE 'th' END END),
				[Weekday]     = CONVERT(TINYINT, [DayOfWeek]),
				[WeekDayName] = CONVERT(VARCHAR(10), DATENAME(WEEKDAY, [date])),
				[IsWeekend]   = CONVERT(BIT, CASE WHEN [DayOfWeek] IN (1,7) THEN 1 ELSE 0 END),
				[IsHoliday]   = CONVERT(BIT, 0),
				HolidayText   = CONVERT(VARCHAR(64), NULL),
				[DOWInMonth]  = CONVERT(TINYINT, ROW_NUMBER() OVER 
								(PARTITION BY FirstOfMonth, [DayOfWeek] ORDER BY [date])),
				[DayOfYear]   = CONVERT(SMALLINT, DATEPART(DAYOFYEAR, [date])),
				WeekOfMonth   = CONVERT(TINYINT, DENSE_RANK() OVER 
								(PARTITION BY [year], [month] ORDER BY [week])),
				WeekOfYear    = CONVERT(TINYINT, [week]),
				ISOWeekOfYear = CONVERT(TINYINT, ISOWeek),
				[Month]       = CONVERT(TINYINT, [month]),
				[MonthName]   = CONVERT(VARCHAR(10), [MonthName]),
				[Quarter]     = CONVERT(TINYINT, [quarter]),
				QuarterName   = CONVERT(VARCHAR(6), CASE [quarter] WHEN 1 THEN 'First' 
								WHEN 2 THEN 'Second' WHEN 3 THEN 'Third' WHEN 4 THEN 'Fourth' END), 
				[Year]        = [year],
				MMYYYY        = CONVERT(CHAR(6), LEFT(Style101, 2)    + LEFT(Style112, 4)),
				MonthYear     = CONVERT(CHAR(7), LEFT([MonthName], 3) + LEFT(Style112, 4)),
				FirstDayOfMonth     = FirstOfMonth,
				LastDayOfMonth      = MAX([date]) OVER (PARTITION BY [year], [month]),
				FirstDayOfQuarter   = MIN([date]) OVER (PARTITION BY [year], [quarter]),
				LastDayOfQuarter    = MAX([date]) OVER (PARTITION BY [year], [quarter]),
				FirstDayOfYear      = FirstOfYear,
				LastDayOfYear       = MAX([date]) OVER (PARTITION BY [year]),
				FirstDayOfNextMonth = DATEADD(MONTH, 1, FirstOfMonth),
				FirstDayOfNextYear  = DATEADD(YEAR,  1, FirstOfYear)
			FROM #dim
			select * from [dbo].[Date]

END
GO
--------------------------------Izvrshuvanje na procedurata---------------------------------------------
EXEC [dbo].[GenerateTableDate] @StartDate='2000-01-01',@NumberOfYears=35
select * from dbo.Date

--------------------------------Kreiranje na tabela [SeniorityLevel]---------------------------------------------

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SeniorityLevel]') AND type in (N'U'))
DROP TABLE [dbo].[SeniorityLevel]
create table [dbo].[SeniorityLevel]
(
[ID] int identity (1,1) primary key not null,
[Name] nvarchar(100) not null
)
insert into [dbo].[SeniorityLevel] (Name)
values
		('Junior'),
		('Intermediate'),
		('Senior'),
		('Lead'),
		('Project Manager'),
		('Division Manager'),
		('Office Manager'),
		('CEO'),
		('CTO'),
		('CIO')
select * from [dbo].[SeniorityLevel]


--------------------------------Kreiranje na tabela [Location]---------------------------------------------


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Location]') AND type in (N'U'))
DROP TABLE [dbo].[Location]
create table [dbo].[Location]
(
[ID] int identity (1,1) primary key not null,
[CountryName] nvarchar(100),
[Continent] nvarchar(100),
[Region] nvarchar(100)
)
insert into [dbo].[Location] ([CountryName], [Continent], [Region])
select CountryName,Continent,Region
from WideWorldImporters.[Application].[Countries]

select * from [dbo].[Location]

--------------------------------Kreiranje na tabela [Department]---------------------------------------------

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Department]') AND type in (N'U'))
DROP TABLE [dbo].[Department]
create table [dbo].[Department]
(
[ID] int identity (1,1) primary key not null,
[Name] nvarchar(100) not null
)


insert into [dbo].[Department]
values
		('Personal Banking & Operations'),
		('Digital Banking Department'),
		('Retail Banking & Marketing Department'),
		('Wealth Management & Third Party Products'),
		('International Banking Division & DFB'),
		('Treasury'),
		('Information Technology'),
		('Corporate Communications'),
		('Support Services & Branch Expansion'),
		('Human Resources')
select * from [dbo].[Department]


--------------------------------Kreiranje na tabela [Employee]---------------------------------------------
/*
List of employees should be imported from Application.People table in WideWorldImporters database.
Table should contain 1111 records after import.
How to populate Location, Seniority and Department data: 
-	Seniority level:
o	We have 10 different seniority levels, so all employees should be divided in almost equal groups and ~10% of employees should have ‘Junior’ seniority, 10% “Intermediate” and so on.
-	Departments:
o	We have 10 different departments, so all employees should be divided in almost equal groups and ~10% of employees should belong to ‘Personal Banking & Operations’ department, ~10% “Treasury” department and and so on.
-	Location
o	We have 190 different departments, so all employees should be divided in almost equal groups and we need to have approx. 5-6 employees on each location.
o	Example: Employee 1,2,3,4,5,6 should be on location 1, Employees 7,8,9,10,11,12 should be on location 2 etc.
*/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Employee]') AND type in (N'U'))
DROP TABLE [dbo].[Employee]
CREATE TABLE [dbo].[Employee]
(
	[ID]               int IDENTITY(1,1) primary key NOT NULL,
	[FirstName]        nvarchar(100) NOT NULL,
	[LastName]         nvarchar (100) NOT NULL,
	[LocationID]       int not NULL,
	[SeniorityLevelID] int not null,
	[DepartmentID]     int not null
)

;with cte as 
(
select 
case 
	when CharIndex(' ',trim(FullName),CharIndex(' ',trim(FullName)) + 1) = 0 --ovde proveruvam dali ima samo edno prazno mesto, ako e 0 togash ima samo edno prazno mesto 
	then trim(substring(trim(FullName),1,charindex(' ',trim(FullName))-1))   -- ovde gi zema prvite n karakteri do praznoto mesto kako ime
	else trim(SubString(trim(FullName),1,CharIndex(' ', trim(FullName), CharIndex(' ', trim(FullName)) + 1))) END As FirstName,-- ako ima i vtoro prazno mesto a fullname e sostaveno od tri zbora togash da gi zeme prvite dva zbora kako ime
case 
	when CharIndex(' ',trim(FullName),CharIndex(' ',trim(FullName)) + 1)= 0 --ovde proveruvam dali ima samo edno prazno mesto, ako e 0 togash ima samo edno prazno mesto
	then trim(substring(trim(FullName),charindex(' ',trim(FullName)),len(trim(FullName))-charindex(' ',trim(FullName))+1))-- ovde gi zema site karakteri posle praznoto mesto kako Prezime 
	else trim(substring(trim(FullName),CharIndex(' ',trim(FullName),CharIndex(' ',trim(FullName)) + 1) ,(len(trim(FullName))-(CharIndex(' ',trim(FullName),CharIndex(' ',trim(FullName)) + 1))+1))) end as LastName
	--gore gi zima site karakteri posle vtoroto prazno mesto kako prezime
,	NTILE(190) OVER (order by PersonID) as LN1
,   Ntile(10) over (order by PreferredName)  as DN2
,   Ntile(10) over (order by trim(substring(trim(FullName),charindex(' ',trim(FullName)),len(trim(FullName))-charindex(' ',trim(FullName))+1)) desc)  as SLN3
from WideWorldImporters.Application.People
)
insert into Employee ([FirstName], [LastName], [LocationID], [SeniorityLevelID], [DepartmentID])
select c.FirstName,c.LastName,l.ID as LocationID,sl.ID as SeniorityLevelID, d.ID as DepartmentID 
from cte c
join Department d on d.ID=c.DN2
join Location l on l.ID=c.LN1
join SeniorityLevel sl on sl.ID=c.SLN3
order by LocationID

select * from [dbo].[Employee]

--------------------------------Kreiranje na tabela [Salary]---------------------------------------------

/*
Salary data should be generated with SQL Script.
Following data should be inserted:
-	Salary data for the past 20 years, starting from 01.2001 to 12.2020
-	Gross amount should be random data between 30.000 and 60.000 
-	Net amount should be 90% of the gross amount
-	RegularWorkAmount sould be 80% of the total Net amount for all employees and months
-	Bonus amount should be the difference between the NetAmount and RegularWorkAmount for every Odd month (January,March,..)
-	OvertimeAmount  should be the difference between the NetAmount and RegularWorkAmount for every Even month (February,April,…)
-	All employees use 10 vacation days in July and 10 Vacation days in December
-	Additionally random vacation days and sickLeaveDays should be generated with the following script:
update dbo.salary set vacationDays = vacationDays + (EmployeeId % 2)
where  (employeeId + MONTH+ year)%5 = 1
GO
update dbo.salary set SickLeaveDays = EmployeeId%8, vacationDays = vacationDays + (EmployeeId % 3)
where  (employeeId + MONTH+ year)%5 = 2
GO

*/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Salary]') AND type in (N'U'))
DROP TABLE [dbo].[Salary]
create table [dbo].[Salary]
(
Id bigint identity (1,1) primary key not null,
EmployeeId int not null,
Month smallint not null ,
Year smallint not null ,
GrossAmount decimal(18,2) not null,
NetAmount decimal(18,2) not null,
RegularWorkAmount decimal(18,2) not null,
BonusAmount decimal(18,2) not null,
OvertimeAmount decimal(18,2) not null,
VacationDays smallint not null ,
SickLeaveDays smallint not null 
)

;with cte as 
(
select e.ID as EmployeeId,d.Month,d.Year
,Round(CAST(25000 + RAND(CHECKSUM(NEWID())) * 35000 AS Money),0) as GrossAmount
from Employee e
cross join [dbo].[Date] d
where d.Year between '2001' and '2020'
group by e.ID,d.Month,d.Year
)
insert into [dbo].[Salary] ([EmployeeId], [Month], [Year], [GrossAmount], [NetAmount], [RegularWorkAmount], [BonusAmount], [OvertimeAmount], [VacationDays], [SickLeaveDays])
select *
,Round(CAST(GrossAmount*0.9 AS Money),0) as NetAmount
,ROUND(CAST((GrossAmount*0.9)*0.8 as money),0) as RegularWorkAmount
,case when Month % 2 <>0 then Round(CAST(GrossAmount*0.9 AS Money),0)-ROUND(CAST(GrossAmount*0.9*0.8 as money),0)
 else 0 end as BonusAmount
,case when month % 2=0 then Round(CAST(GrossAmount*0.9 AS Money),0)-ROUND(CAST(GrossAmount*0.9*0.8 as money),0)
else 0 end as OvertimeAmount
,case when Month=7 or Month=12 then 10 else 0 end as VacationDays
,0 as SickLeaveDays
from cte c
order by YEAR,Month
select * from [dbo].[Salary]
--------------------------------Update na denovite za odmor i boleduvanje---------------------------------------------
update dbo.salary set vacationDays = vacationDays + (EmployeeId % 2)
where  (employeeId + MONTH+ year)%5 = 1
GO
update dbo.salary set SickLeaveDays = EmployeeId%8, vacationDays = vacationDays + (EmployeeId % 3)
where  (employeeId + MONTH+ year)%5 = 2
GO
--------------------------------proverka dali e ispolnet baraniot uslov ---------------------------------------------
select * from dbo.salary 
where NetAmount <> (regularWorkAmount + BonusAmount + OverTimeAmount)

--------------------------------Proverka dali denovite za odmor kaj site vraboteni e pomegju 20 i 30 dena godishno---------------------------------------------

select EmployeeId,Year,sum(VacationDays)
from Salary
group by EmployeeId,Year
having sum(VacationDays) between 20 and 30
order by EmployeeId,Year

--------------------------------Dodavanje na Foreign keys---------------------------------------------

ALTER TABLE Salary WITH CHECK
ADD CONSTRAINT FK_Salary_Employee FOREIGN
KEY (EmployeeID)
REFERENCES Employee (ID)

ALTER TABLE Employee WITH CHECK
ADD CONSTRAINT FK_Employee_Department FOREIGN
KEY (DepartmentID)
REFERENCES Department (ID)


ALTER TABLE Employee WITH CHECK
ADD CONSTRAINT FK_Employee_Location FOREIGN
KEY (LocationID)
REFERENCES Location (ID)


ALTER TABLE Employee WITH CHECK
ADD CONSTRAINT FK_Employee_SeniorityLevel FOREIGN
KEY (SeniorityLevelID)
REFERENCES SeniorityLevel (ID)
