CREATE VIEW spec1 AS
WITH
dates(date) AS (SELECT DISTINCT date FROM options ORDER BY date DESC LIMIT 10),
LH(date,month,fut,strike,dte,price) AS (SELECT o.date, o.month, f.C, CAST(o.strike AS INT), o.dte, o.price FROM options o LEFT JOIN futures f ON o.date=f.date AND o.code=f.code AND o.undly=f.month WHERE o.code='LH' AND o.type='C' AND o.date IN (SELECT date FROM dates) AND o.price>0),
LC(date,month,fut,strike,dte,price) AS (SELECT o.date, o.month, f.C, CAST(o.strike AS INT), o.dte, o.price FROM options o LEFT JOIN futures f ON o.date=f.date AND o.code=f.code AND o.undly=f.month WHERE o.code='LC' AND o.type='C' AND o.date IN (SELECT date FROM dates)),
diff(date,month,strike,lhFut,lhDTE,lhPrice,lcFut,lcDTE,lcPrice,diff) AS (SELECT LH.date, LH.month, LH.strike, LH.fut, LH.dte, LH.price, LC.fut, LC.dte, LC.price, ROUND(LH.price-LC.price,3) diff FROM LH JOIN LC USING(date,month,strike) WHERE LH.price>=LC.price),
maxD(date, month, maxStrikes) AS (SELECT date, month, GROUP_CONCAT(strike) FROM diff WHERE (date,month,diff) IN (SELECT date, month, MAX(diff) FROM diff GROUP BY date, month) GROUP BY date, month),
info(date,month,lhFut,lcFut,minStrike,lhPrice,lcPrice,maxDiff) AS (SELECT date, month, lhFut, lcFut, MIN(strike), MAX(lhPrice), MAX(lcPrice), MAX(diff) FROM diff GROUP BY date, month, lhFut, lcFut)
SELECT date, month, lhFut, lcFut, minStrike, lhPrice, lcPrice, maxDiff, maxStrikes FROM info NATURAL JOIN maxD
/* spec1(date,month,lhFut,lcFut,minStrike,lhPrice,lcPrice,maxDiff,maxStrikes) */;
