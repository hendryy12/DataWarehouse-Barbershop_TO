-- SELECT FOR ETL 
--ProductDimension
SELECT ProductID, ProductSellingPrice, ProductBuyingPrice FROM OLTP_BarbershopTO..MsProduct
--BarberDimension
SELECT BarberID, BarberDOB, BarberSalary, BarberAddress FROM OLTP_BarbershopTO..MsBarber
--StaffDimension
SELECT StaffID, StaffDOB, StaffSalary, StaffAddress FROM OLTP_BarbershopTO..MsStaff
--CustomerDimension
SELECT CustomerID, CustomerAddress, CustomerGender FROM OLTP_BarbershopTO..MsCustomer
--BenefitDimension
SELECT BenefitID, BenefitPrice FROM OLTP_BarbershopTO..MsBenefit
--PackageDimension
SELECT PackageID, PackagePrice FROM OLTP_BarbershopTO..MsPackage
--SupplierDimension
SELECT SupplierID, SupplierAddress, SupplierPhone FROM OLTP_BarbershopTO..MsSupplier

--SELECT FOR DISPLAY OLAP TABLE
SELECT * FROM Barbershop_OLAP..ProductDimension
SELECT * FROM Barbershop_OLAP..BarberDimension
SELECT * FROM Barbershop_OLAP..StaffDimension
SELECT * FROM Barbershop_OLAP..CustomerDimension
SELECT * FROM Barbershop_OLAP..BenefitDimension
SELECT * FROM Barbershop_OLAP..PackageDimension
SELECT * FROM Barbershop_OLAP..SupplierDimension
SELECT * FROM Barbershop_OLAP..TimeDimensionSales
SELECT * FROM Barbershop_OLAP..FilterTimeStampSales
SELECT * FROM Barbershop_OLAP..TimeDimensionPurchase
SELECT * FROM Barbershop_OLAP..FilterTimeStampPurchase
SELECT * FROM Barbershop_OLAP..TimeDimensionSubscription
SELECT * FROM Barbershop_OLAP..FilterTimeStampSubscription
SELECT * FROM Barbershop_OLAP..TimeDimensionHaircut
SELECT * FROM Barbershop_OLAP..FilterTimeStampHaircut

--ETL FOR TimeDimension (Sale)
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSales WHERE TableName = 'TimeDimensionSales')
BEGIN
SELECT
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] = SaleDate 
FROM OLTP_BarbershopTO..TrSaleHeader ) as AllDate
WHERE AllDate.[Date] > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSales WHERE TableName = 'TimeDimensionSales')
END
ELSE
BEGIN
SELECT 
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] = SaleDate 
FROM OLTP_BarbershopTO..TrSaleHeader ) as AllDate
END

-- ETL FilterTimeStamp Sales
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSales WHERE TableName = 'TimeDimensionSales')
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampSales
SET LastETL = GETDATE()
WHERE TableName = 'TimeDimensionSales'
END
ELSE
BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampSales VALUES 
 ('TimeDimensionSales', GETDATE())
END

----ETL FOR TimeDimension (Purchase)
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampPurchase WHERE TableName = 'TimeDimensionPurchase')
BEGIN
SELECT
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] = PurchaseDate
FROM OLTP_BarbershopTO..TrPurchaseHeader ) as AllDate
WHERE AllDate.[Date] > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampPurchase WHERE TableName = 'TimeDimensionPurchase')
END
ELSE
BEGIN
SELECT 
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] = PurchaseDate
FROM OLTP_BarbershopTO..TrPurchaseHeader ) as AllDate
END

-- ETL FilterTimeStamp Purchase
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampPurchase WHERE TableName = 'TimeDimensionPurchase')
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampPurchase
SET LastETL = GETDATE()
WHERE TableName = 'TimeDimensionPurchase'
END
ELSE
BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampPurchase VALUES 
 ('TimeDimensionPurchase', GETDATE())
END

-- ETL TimeDimension (Subscription)
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSubscription WHERE TableName = 'TimeDimensionSubscription')
BEGIN
SELECT
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] =  SubscriptionEndDate
FROM OLTP_BarbershopTO..TrSubscriptionHeader ) as AllDate
WHERE AllDate.[Date] > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSubscription WHERE TableName = 'TimeDimensionSubscription')
END
ELSE
BEGIN
SELECT 
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] = SubscriptionEndDate 
FROM OLTP_BarbershopTO..TrSubscriptionHeader ) as AllDate
END

-- ETL FilterTimeStamp Subscription
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSubscription WHERE TableName = 'TimeDimensionSubscription')
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampSubscription
SET LastETL = GETDATE()
WHERE TableName = 'TimeDimensionSubscription'
END
ELSE
BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampSubscription VALUES 
 ('TimeDimensionSubscription', GETDATE())
END

--ETL TimeDimension (Haircut)
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampHaircut WHERE TableName = 'TimeDimensionHaircut')
BEGIN
SELECT
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] =  HaircutDate
FROM OLTP_BarbershopTO..TrHaircutHeader ) as AllDate
WHERE AllDate.[Date] > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampHaircut WHERE TableName = 'TimeDimensionHaircut')
END
ELSE
BEGIN
SELECT 
AllDate.[Date],
[Day] = DAY(AllDate.[Date]),
[Month] = MONTH(AllDate.[Date]),
[Quarter] = DATEPART(QUARTER,AllDate.[Date]),
[Year] = YEAR(AllDate.[Date])
FROM (
SELECT [Date] = HaircutDate
FROM OLTP_BarbershopTO..TrHaircutHeader ) as AllDate
END

--ETL FilterTimeStamp Haircut
IF EXISTS (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampHaircut WHERE TableName = 'TimeDimensionHaircut')
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampHaircut
SET LastETL = GETDATE()
WHERE TableName = 'TimeDimensionHaircut'
END
ELSE
BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampHaircut VALUES 
 ('TimeDimensionHaircut', GETDATE())
END

