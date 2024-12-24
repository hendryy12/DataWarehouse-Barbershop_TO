CREATE DATABASE Barbershop_OLAP
GO
USE Barbershop_OLAP
GO

--Table Dimension

CREATE TABLE ProductDimension (
ProductCode INT PRIMARY KEY IDENTITY,
ProductID VARCHAR(7),
ProductSellingPrice BIGINT,
ProductBuyingPrice BIGINT,
ValidFrom DATETIME,
ValidTo DATETIME
)

CREATE TABLE BarberDimension (
BarberCode INT PRIMARY KEY IDENTITY,
BarberID VARCHAR(7),
BarberDOB DATE,
BarberSalary BIGINT,
BarberAddress VARCHAR(100),
ValidFrom DATETIME,
ValidTo DATETIME
)

CREATE TABLE StaffDimension (
StaffCode INT PRIMARY KEY IDENTITY,
StaffID VARCHAR(7),
StaffDOB DATE,
StaffSalary BIGINT,
StaffAddress VARCHAR(100),
ValidFrom DATETIME,
ValidTo DATETIME
)

CREATE TABLE CustomerDimension (
CustomerCode INT PRIMARY KEY IDENTITY,
CustomerID VARCHAR(7),
CustomerAddress VARCHAR(100),
CustomerGender VARCHAR(10)
)

CREATE TABLE BenefitDimension (
BenefitCode INT PRIMARY KEY IDENTITY,
BenefitID VARCHAR(7),
BenefitPrice BIGINT,
ValidFrom DATETIME,
ValidTo DATETIME
)

CREATE TABLE PackageDimension (
PackageCode INT PRIMARY KEY IDENTITY,
PackageID VARCHAR(7),
PackagePrice BIGINT,
ValidFrom DATETIME,
ValidTo DATETIME
)

CREATE TABLE SupplierDimension (
SupplierCode INT PRIMARY KEY IDENTITY,
SupplierID VARCHAR(7),
SupplierAddress VARCHAR(100),
SupplierPhone VARCHAR(20)
)

CREATE TABLE TimeDimensionSales(
TimeCode INT PRIMARY KEY IDENTITY,
[Date] DATE,
[Day] INT,
[Month] INT,
[Quarter] INT,
[Year] INT
)

CREATE TABLE TimeDimensionPurchase(
TimeCode INT PRIMARY KEY IDENTITY,
[Date] DATE,
[Day] INT,
[Month] INT,
[Quarter] INT,
[Year] INT
)

CREATE TABLE TimeDimensionSubscription(
TimeCode INT PRIMARY KEY IDENTITY,
[Date] DATE,
[Day] INT,
[Month] INT,
[Quarter] INT,
[Year] INT
)

CREATE TABLE TimeDimensionHaircut(
TimeCode INT PRIMARY KEY IDENTITY,
[Date] DATE,
[Day] INT,
[Month] INT,
[Quarter] INT,
[Year] INT
)

--FilterTimeStamp

CREATE TABLE FilterTimeStampSales(
TableName VARCHAR(50) PRIMARY KEY,
LastETL DATETIME
)

CREATE TABLE FilterTimeStampPurchase(
TableName VARCHAR(50) PRIMARY KEY,
LastETL DATETIME
)

CREATE TABLE FilterTimeStampSubscription(
TableName VARCHAR(50) PRIMARY KEY,
LastETL DATETIME
)

CREATE TABLE FilterTimeStampHaircut(
TableName VARCHAR(50) PRIMARY KEY,
LastETL DATETIME
)



--Fact Table

CREATE TABLE SalesTransaction (
ProductCode INT,
StaffCode INT,
CustomerCode INT,
TimeCode INT,
TotalSalesEarnings BIGINT,
TotalHaircareProductsSold BIGINT
)

CREATE TABLE PurchaseTransaction (
ProductCode INT,
StaffCode INT,
SupplierCode INT,
TimeCode INT,
TotalPurchaseExpense BIGINT,
TotalMaterialPurchased BIGINT
)

CREATE TABLE SubscriptionTransaction (
CustomerCode INT,
StaffCode INT,
BenefitCode INT,
TimeCode INT,
TotalSubscriptionEarning BIGINT,
TotalSubscriber BIGINT
)

CREATE TABLE HaircutTransaction (
CustomerCode INT,
PackageCode INT,
BarberCode INT,
TimeCode INT,
TotalHaircutEarning BIGINT,
TotalBarber BIGINT
)
