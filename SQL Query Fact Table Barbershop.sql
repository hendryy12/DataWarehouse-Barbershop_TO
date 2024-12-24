--Fact Table
--SalesTransaction
SELECT 
ProductCode,
StaffCode,
CustomerCode,
TimeCode,
[TotalSalesEarnings] = SUM(pd.ProductSellingPrice * tsd.Quantity),
[TotalHaircareProductsSold] = SUM(tsd.Quantity)
FROM OLTP_BarbershopTO..TrSaleHeader tsh
JOIN OLTP_BarbershopTO..TrSaleDetail tsd ON tsd.SaleID = tsh.SaleID
JOIN Barbershop_OLAP..ProductDimension pd ON pd.ProductID = tsd.ProductID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tsh.StaffID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = tsh.CustomerID
JOIN Barbershop_OLAP..TimeDimensionSales tds ON tds.Date = tsh.SaleDate
GROUP BY ProductCode, StaffCode, CustomerCode, TimeCode

--ETL Sales Transaction
IF EXISTS (
	SELECT * FROM Barbershop_OLAP..FilterTimeStampSales WHERE TableName = 'SalesTransaction'
)
BEGIN
SELECT 
ProductCode,
StaffCode,
CustomerCode,
TimeCode,
[TotalSalesEarnings] = SUM(pd.ProductSellingPrice * tsd.Quantity),
[TotalHaircareProductsSold] = SUM(tsd.Quantity)
FROM OLTP_BarbershopTO..TrSaleHeader tsh
JOIN OLTP_BarbershopTO..TrSaleDetail tsd ON tsd.SaleID = tsh.SaleID
JOIN Barbershop_OLAP..ProductDimension pd ON pd.ProductID = tsd.ProductID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tsh.StaffID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = tsh.CustomerID
JOIN Barbershop_OLAP..TimeDimensionSales tds ON tds.Date = tsh.SaleDate
WHERE tsh.SaleDate > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSales WHERE TableName = 'SalesTransaction')
GROUP BY ProductCode, StaffCode, CustomerCode, TimeCode
END

ELSE

BEGIN
SELECT 
ProductCode,
StaffCode,
CustomerCode,
TimeCode,
[TotalSalesEarnings] = SUM(pd.ProductSellingPrice * tsd.Quantity),
[TotalHaircareProductsSold] = SUM(tsd.Quantity)
FROM OLTP_BarbershopTO..TrSaleHeader tsh
JOIN OLTP_BarbershopTO..TrSaleDetail tsd ON tsd.SaleID = tsh.SaleID
JOIN Barbershop_OLAP..ProductDimension pd ON pd.ProductID = tsd.ProductID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tsh.StaffID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = tsh.CustomerID
JOIN Barbershop_OLAP..TimeDimensionSales tds ON tds.Date = tsh.SaleDate
GROUP BY ProductCode, StaffCode, CustomerCode, TimeCode
END

--EXECUTE SQL TASK SalesTransaction
IF EXISTS (
SELECT * FROM Barbershop_OLAP..FilterTimeStampSales WHERE TableName = 'SalesTransaction'
)
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampSales
SET LastETL = GETDATE()
WHERE TableName = 'SalesTransaction'
END

ELSE

BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampSales VALUES 
 ('SalesTransaction', GETDATE())
END

-- DISPLAY Sales Transaction
SELECT * FROM Barbershop_OLAP..SalesTransaction
SELECT * FROM Barbershop_OLAP..FilterTimeStampSales

--PurchaseTransaction
SELECT 
ProductCode,
StaffCode,
SupplierCode,
TimeCode,
[TotalPurchaseExpense] = SUM(pd.ProductBuyingPrice * tpd.Quantity),
[TotalMaterialPurchased] = SUM(tpd.Quantity)
FROM OLTP_BarbershopTO..TrPurchaseHeader tph
JOIN OLTP_BarbershopTO..TrPurchaseDetail tpd ON tph.PurchaseID = tpd.PurchaseID
JOIN Barbershop_OLAP..ProductDimension pd ON pd.ProductID = tpd.ProductID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tph.StaffID
JOIN Barbershop_OLAP..SupplierDimension sud ON sud.SupplierID = tph.SupplierID
JOIN Barbershop_OLAP..TimeDimensionPurchase tdp ON tdp.Date = tph.PurchaseDate
GROUP BY ProductCode, StaffCode, SupplierCode, TimeCode

--ETL Purchase Transaction
IF EXISTS (
	SELECT * FROM Barbershop_OLAP..FilterTimeStampPurchase WHERE TableName = 'PurchaseTransaction'
)
BEGIN
SELECT 
ProductCode,
StaffCode,
SupplierCode,
TimeCode,
[TotalPurchaseExpense] = SUM(pd.ProductBuyingPrice * tpd.Quantity),
[TotalMaterialPurchased] = SUM(tpd.Quantity)
FROM OLTP_BarbershopTO..TrPurchaseHeader tph
JOIN OLTP_BarbershopTO..TrPurchaseDetail tpd ON tph.PurchaseID = tpd.PurchaseID
JOIN Barbershop_OLAP..ProductDimension pd ON pd.ProductID = tpd.ProductID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tph.StaffID
JOIN Barbershop_OLAP..SupplierDimension sud ON sud.SupplierID = tph.SupplierID
JOIN Barbershop_OLAP..TimeDimensionPurchase tdp ON tdp.Date = tph.PurchaseDate
WHERE tph.PurchaseDate > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampPurchase WHERE TableName = 'PurchaseTransaction')
GROUP BY ProductCode, StaffCode, SupplierCode, TimeCode
END

ELSE

BEGIN
SELECT 
ProductCode,
StaffCode,
SupplierCode,
TimeCode,
[TotalPurchaseExpense] = SUM(pd.ProductBuyingPrice * tpd.Quantity),
[TotalMaterialPurchased] = SUM(tpd.Quantity)
FROM OLTP_BarbershopTO..TrPurchaseHeader tph
JOIN OLTP_BarbershopTO..TrPurchaseDetail tpd ON tph.PurchaseID = tpd.PurchaseID
JOIN Barbershop_OLAP..ProductDimension pd ON pd.ProductID = tpd.ProductID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tph.StaffID
JOIN Barbershop_OLAP..SupplierDimension sud ON sud.SupplierID = tph.SupplierID
JOIN Barbershop_OLAP..TimeDimensionPurchase tdp ON tdp.Date = tph.PurchaseDate
GROUP BY ProductCode, StaffCode, SupplierCode, TimeCode
END

--EXECUTE SQL TASK PurchaseTransaction
IF EXISTS (
SELECT * FROM Barbershop_OLAP..FilterTimeStampPurchase WHERE TableName = 'PurchaseTransaction'
)
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampPurchase
SET LastETL = GETDATE()
WHERE TableName = 'PurchaseTransaction'
END

ELSE

BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampPurchase VALUES 
 ('PurchaseTransaction', GETDATE())
END

--DISPLAY Purchase Transaction
SELECT * FROM Barbershop_OLAP..PurchaseTransaction
SELECT * FROM Barbershop_OLAP..FilterTimeStampPurchase

--Subscription Transaction
SELECT 
CustomerCode,
StaffCode,
BenefitCode,
TimeCode,
[TotalSubscriptionEarning] = SUM(bd.BenefitPrice),
[TotalSubscriber] = COUNT(tsh.SubscriptionID)
FROM OLTP_BarbershopTO..TrSubscriptionHeader tsh 
JOIN OLTP_BarbershopTO..TrSubscriptionDetail tsd ON tsh.SubscriptionID = tsd.SubscriptionID
JOIN Barbershop_OLAP..BenefitDimension bd ON bd.BenefitID = tsd.BenefitID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = tsh.CustomerID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tsh.StaffID
JOIN Barbershop_OLAP..TimeDimensionSubscription tds ON tds.Date = tsh.SubscriptionEndDate
GROUP BY CustomerCode, StaffCode, BenefitCode, TimeCode

--ETL Subscription Transaction
IF EXISTS (
	SELECT * FROM Barbershop_OLAP..FilterTimeStampSubscription WHERE TableName = 'SubscriptionTransaction'
)
BEGIN
SELECT 
CustomerCode,
StaffCode,
BenefitCode,
TimeCode,
[TotalSubscriptionEarning] = SUM(bd.BenefitPrice),
[TotalSubscriber] = COUNT(tsh.SubscriptionID)
FROM OLTP_BarbershopTO..TrSubscriptionHeader tsh 
JOIN OLTP_BarbershopTO..TrSubscriptionDetail tsd ON tsh.SubscriptionID = tsd.SubscriptionID
JOIN Barbershop_OLAP..BenefitDimension bd ON bd.BenefitID = tsd.BenefitID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = tsh.CustomerID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tsh.StaffID
JOIN Barbershop_OLAP..TimeDimensionSubscription tds ON tds.Date = tsh.SubscriptionEndDate
WHERE tsh.SubscriptionEndDate > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampSubscription WHERE TableName = 'SubscriptionTransaction')
GROUP BY CustomerCode, StaffCode, BenefitCode, TimeCode
END

ELSE

BEGIN
SELECT 
CustomerCode,
StaffCode,
BenefitCode,
TimeCode,
[TotalSubscriptionEarning] = SUM(bd.BenefitPrice),
[TotalSubscriber] = COUNT(tsh.SubscriptionID)
FROM OLTP_BarbershopTO..TrSubscriptionHeader tsh 
JOIN OLTP_BarbershopTO..TrSubscriptionDetail tsd ON tsh.SubscriptionID = tsd.SubscriptionID
JOIN Barbershop_OLAP..BenefitDimension bd ON bd.BenefitID = tsd.BenefitID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = tsh.CustomerID
JOIN Barbershop_OLAP..StaffDimension sd ON sd.StaffID = tsh.StaffID
JOIN Barbershop_OLAP..TimeDimensionSubscription tds ON tds.Date = tsh.SubscriptionEndDate
GROUP BY CustomerCode, StaffCode, BenefitCode, TimeCode
END

--EXEQUTE SQL TASK SubscriptionTransaction
IF EXISTS (
SELECT * FROM Barbershop_OLAP..FilterTimeStampSubscription WHERE TableName = 'SubscriptionTransaction'
)
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampSubscription
SET LastETL = GETDATE()
WHERE TableName = 'SubscriptionTransaction'
END

ELSE

BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampSubscription VALUES 
 ('SubscriptionTransaction', GETDATE())
END

--DISPLAY Subscription Transaction
SELECT * FROM Barbershop_OLAP..SubscriptionTransaction
SELECT * FROM Barbershop_OLAP..FilterTimeStampSubscription

--Haircut Transaction
SELECT 
CustomerCode,
PackageCode,
BarberCode,
TimeCode,
[TotalHaircutEarning] = SUM(pd.PackagePrice * thd.Quantity),
[TotalBarber] = COUNT(thh.BarberID)
FROM OLTP_BarbershopTO..TrHaircutHeader thh
JOIN OLTP_BarbershopTO..TrHaircutDetail thd ON thh.HaircutID = thd.HaircutID
JOIN Barbershop_OLAP..PackageDimension pd ON pd.PackageID = thd.PackageID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = thh.CustomerID
JOIN Barbershop_OLAP..BarberDimension bd ON bd.BarberID = thh.BarberID
JOIN Barbershop_OLAP..TimeDimensionHaircut tdh ON tdh.Date = thh.HaircutDate
GROUP BY CustomerCode, PackageCode, BarberCode, TimeCode

--ETL Haircut Transaction
IF EXISTS (
	SELECT * FROM Barbershop_OLAP..FilterTimeStampHaircut WHERE TableName = 'HaircutTransaction'
)
BEGIN
SELECT 
CustomerCode,
PackageCode,
BarberCode,
TimeCode,
[TotalHaircutEarning] = SUM(pd.PackagePrice * thd.Quantity),
[TotalBarber] = COUNT(thh.BarberID)
FROM OLTP_BarbershopTO..TrHaircutHeader thh
JOIN OLTP_BarbershopTO..TrHaircutDetail thd ON thh.HaircutID = thd.HaircutID
JOIN Barbershop_OLAP..PackageDimension pd ON pd.PackageID = thd.PackageID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = thh.CustomerID
JOIN Barbershop_OLAP..BarberDimension bd ON bd.BarberID = thh.BarberID
JOIN Barbershop_OLAP..TimeDimensionHaircut tdh ON tdh.Date = thh.HaircutDate
WHERE thh.HaircutDate > (SELECT LastETL FROM Barbershop_OLAP..FilterTimeStampHaircut WHERE TableName = 'HaircutTransaction')
GROUP BY CustomerCode, PackageCode, BarberCode, TimeCode
END

ELSE

BEGIN
SELECT 
CustomerCode,
PackageCode,
BarberCode,
TimeCode,
[TotalHaircutEarning] = SUM(pd.PackagePrice * thd.Quantity),
[TotalBarber] = COUNT(thh.BarberID)
FROM OLTP_BarbershopTO..TrHaircutHeader thh
JOIN OLTP_BarbershopTO..TrHaircutDetail thd ON thh.HaircutID = thd.HaircutID
JOIN Barbershop_OLAP..PackageDimension pd ON pd.PackageID = thd.PackageID
JOIN Barbershop_OLAP..CustomerDimension cd ON cd.CustomerID = thh.CustomerID
JOIN Barbershop_OLAP..BarberDimension bd ON bd.BarberID = thh.BarberID
JOIN Barbershop_OLAP..TimeDimensionHaircut tdh ON tdh.Date = thh.HaircutDate
GROUP BY CustomerCode, PackageCode, BarberCode, TimeCode
END

--EXEQUTE SQL TASK Haircut Transaction
IF EXISTS (
SELECT * FROM Barbershop_OLAP..FilterTimeStampHaircut WHERE TableName = 'HaircutTransaction'
)
BEGIN
UPDATE Barbershop_OLAP..FilterTimeStampHaircut
SET LastETL = GETDATE()
WHERE TableName = 'HaircutTransaction'
END

ELSE

BEGIN
 INSERT INTO Barbershop_OLAP..FilterTimeStampHaircut VALUES 
 ('HaircutTransaction', GETDATE())
END

--DISPLAY Haircut Transaction
SELECT * FROM Barbershop_OLAP..HaircutTransaction
SELECT * FROM Barbershop_OLAP..FilterTimeStampHaircut