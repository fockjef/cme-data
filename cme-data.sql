CREATE TABLE products( ID TEXT PRIMARY KEY, code TEXT, name TEXT, strikeMult REAL, priceMult REAL, dollarMult REAL, minTickF REAL, minTickO REAL, isDecimal INT);
INSERT OR REPLACE INTO products VALUES ('C','C','Corn',100.0,100.0,50,0.25,0.125,0);
INSERT OR REPLACE INTO products VALUES ('W','W','Wheat',100.0,100.0,50,0.25,0.125,0);
INSERT OR REPLACE INTO products VALUES ('KW','KW','KC Wheat',100.0,100.0,50,0.25,0.125,0);
INSERT OR REPLACE INTO products VALUES ('S','S','Soybeans',100.0,100.0,50,0.25,0.125,0);
INSERT OR REPLACE INTO products VALUES ('06','SM','Soybean Meal',1.0,1.0,100.0,0.1,0.05,1);
INSERT OR REPLACE INTO products VALUES ('07','BO','Soybean Oil',1.0,1.0,600,0.01,0.005,1);
INSERT OR REPLACE INTO products VALUES ('48','LC','Live Cattle',1.0,1.0,400,0.025,0.025,1);
INSERT OR REPLACE INTO products VALUES ('62','FC','Feeder Cattle',1.0,1.0,500,0.025,0.0125,1);
INSERT OR REPLACE INTO products VALUES ('LN','LH','Lean Hogs',1.0,1.0,400,0.025,0.0125,1);

CREATE TABLE intrate (date DATE PRIMARY KEY, r REAL);
INSERT OR REPLACE INTO intrate VALUES ('2017-01-01',0.01);

CREATE TABLE futures (date DATE, code TEXT, month TEXT, H REAL, L REAL, C REAL, change REAL, volume INT, oi INT, PRIMARY KEY(date,code,month));
CREATE TABLE options (date DATE, code TEXT, month TEXT, undly TEXT, dte INT, strike REAL, type TEXT, price REAL, change REAL, volume INT, oi INT, vol REAL, skewA REAL, skewB REAL, skewC REAL, PRIMARY KEY(date,code,month,strike,type));
CREATE TABLE optvols (date DATE, code TEXT, month TEXT, volATM REAL, PRIMARY KEY(date,code,month));

CREATE VIEW  settles AS SELECT o.date date, o.code code, o.month month, f.H H, f.L L, f.C C, f.change change, f.volume volume, f.oi oi, o.volATM volATM FROM optvols o NATURAL LEFT JOIN futures f;

-- cubic spline interpolation table
CREATE TABLE __knots (optID INT PRIMARY key, code TEXT, month TEXT, t REAL, y REAL, h REAL, b REAL, u REAL, v REAL, z REAL, skewA REAL, skewB REAL, skewC REAL);
CREATE TRIGGER trigUpsertKnots BEFORE INSERT ON __knots WHEN (SELECT 1 FROM __knots WHERE rowid=NEW.rowid)
BEGIN
	UPDATE __knots SET h=NEW.h, b=NEW.b WHERE NEW.h IS NOT NULL AND rowid=NEW.rowid;
	UPDATE __knots SET u=NEW.u, v=NEW.v WHERE NEW.u IS NOT NULL AND rowid=NEW.rowid;
	UPDATE __knots SET z=NEW.z          WHERE NEW.z IS NOT NULL AND rowid=NEW.rowid;
	UPDATE __knots SET skewA=NEW.skewA, skewB=NEW.skewB, skewC=NEW.skewC WHERE NEW.skewB IS NOT NULL AND rowid=NEW.rowid;
END;

-- import CSV view + triggers
CREATE VIEW importCSV AS SELECT '' BizDt, '' Sym, '' ID, '' StrkPx, '' SecTyp, '' MMY, '' MatDt, '' PutCall, '' Exch, '' Desc, '' LastTrdDt, '' BidPrice, '' OpeningPrice, '' SettlePrice, '' SettleDelta, '' HighLimit, '' LowLimit, '' DHighPrice, '' DLowPrice, '' HighBid, '' LowBid, '' PrevDayVol, '' PrevDayOI, '' FixingPrice, '' UndlyExch, '' UndlyID, '' UndlySecTyp, '' UndlyMMY, '' BankBusDay;
CREATE TRIGGER trigImportCSV_Fut INSTEAD OF INSERT ON importCSV WHEN NEW.SecTyp='FUT' AND NEW.ID IN (SELECT ID FROM products)
BEGIN
	-- insert new futures data
	INSERT OR REPLACE INTO futures(date,code,month,H,L,C,volume,oi)
	SELECT
		NEW.BizDt,
		p.code,
		SUBSTR(NEW.MMY,3,2)||SUBSTR('FGHJKMNQUVXZ',SUBSTR(NEW.MMY,-2)+0,1),
		COALESCE(NULLIF(NEW.DHighPrice,''),NULLIF(NEW.HighBid,''))*p.priceMult,
		COALESCE(NULLIF(NEW.DLowPrice ,''),NULLIF(NEW.LowBid ,''))*p.priceMult,
		NEW.SettlePrice*p.priceMult,
		NEW.PrevDayVol,
		NEW.PrevDayOI
	FROM
		products p
	WHERE
		p.ID=NEW.ID;
END;
CREATE TRIGGER trigImportCSV_Opt INSTEAD OF INSERT ON importCSV WHEN NEW.SecTyp='OOF' AND NEW.ID IN (SELECT ID FROM products)
BEGIN
	-- insert new options data
	INSERT OR REPLACE INTO options(date,code,month,undly,dte,strike,type,price,volume,oi)
	SELECT
		NEW.BizDt,
		p.code,
		SUBSTR(NEW.MMY,3,2)||SUBSTR('FGHJKMNQUVXZ',SUBSTR(NEW.MMY,-2)+0,1),
		SUBSTR(NEW.UndlyMMY,3,2)||SUBSTR('FGHJKMNQUVXZ',SUBSTR(NEW.UndlyMMY,-2)+0,1),
		CAST(julianday(NEW.MatDt)-julianday(NEW.BizDt) AS INT),
		NEW.StrkPx*p.strikeMult,
		SUBSTR('PC',NEW.PutCall+1,1),
		COALESCE(NULLIF(NEW.SettlePrice+0,9999999),0)*p.priceMult,
		NEW.PrevDayVol,
		NEW.PrevDayOI
	FROM
		products p
	WHERE
		p.ID=NEW.ID;
END;

-- process "date" view + triggers
CREATE VIEW processDate AS SELECT '' date;
CREATE TRIGGER trigProcessDate INSTEAD OF INSERT ON processDate
BEGIN
	-- update volume/open interest for previous day
	UPDATE futures SET
		volume=COALESCE((SELECT f.volume FROM futures f WHERE f.date=NEW.date AND f.code=futures.code AND f.month=futures.month),volume),
		oi    =COALESCE((SELECT f.oi     FROM futures f WHERE f.date=NEW.date AND f.code=futures.code AND f.month=futures.month),oi)
	WHERE date=(SELECT MAX(date) FROM futures WHERE date<NEW.date);
	UPDATE options SET
		volume=COALESCE((SELECT o.volume FROM options o WHERE o.date=NEW.date AND o.code=options.code AND o.month=options.month AND o.strike=options.strike AND o.type=options.type),volume),
		oi    =COALESCE((SELECT o.oi     FROM options o WHERE o.date=NEW.date AND o.code=options.code AND o.month=options.month AND o.strike=options.strike AND o.type=options.type),oi)
	WHERE date=(SELECT MAX(date) FROM futures WHERE date<NEW.date);

	-- reset volume/open interest and set change for current day
	UPDATE futures SET
		volume=NULL,
		oi    =NULL,
		change=ROUND(C-(SELECT f.C FROM futures f WHERE f.date=(SELECT MAX(date) FROM futures WHERE date<NEW.date) AND f.code=futures.code AND f.month=futures.month),6)
	WHERE date=NEW.date;
	UPDATE options SET
		volume=NULL,
		oi    =NULL,
		change=ROUND(price-(SELECT o.price FROM options o WHERE o.date=(SELECT MAX(date) FROM futures WHERE date<NEW.date) AND o.code=options.code AND o.month=options.month AND o.strike=options.strike AND o.type=options.type),6)
	WHERE date=NEW.date;

	-- generate cubic spline
	DELETE FROM __knots;
	INSERT INTO __knots(optID,code,month,t,y) WITH
		currate(date,r) AS (SELECT MAX(date), r FROM intrate WHERE date<=NEW.date)
		SELECT o.optId, o.code, o.month, o.strike, whaley_IV(f.C,o.strike,CASE o.type WHEN 'P' THEN 0 ELSE 1 END,o.price,o.dte/365.0,(SELECT r FROM currate)) FROM (SELECT rowid optId, date, code, month, undly, dte, strike, type, MIN(price) price FROM options WHERE date=NEW.date AND dte GROUP BY date, code, month, strike) o NATURAL JOIN products p JOIN futures f ON o.date=f.date AND o.code=f.code AND o.undly=f.month WHERE o.price>p.minTickO ORDER BY o.code, o.month, o.strike;
	-- h + b
	INSERT OR IGNORE INTO __knots(rowid,h,b) SELECT a.rowid, b.t-a.t, 6*(b.y-a.y)/(b.t-a.t) FROM __knots a LEFT JOIN __knots b ON b.rowid=a.rowid+1 AND a.code=b.code AND a.month=b.month;
	-- u + v
	INSERT OR IGNORE INTO __knots(rowid,code,month,u,v) WITH RECURSIVE
	T(i,code,month,h,b,u,v) AS (
		SELECT MIN(rowid), code, month, h, b, NULL, NULL FROM __knots GROUP BY code, month
		UNION ALL
		SELECT a.rowid, a.code, a.month, a.h, a.b, 2*(a.h+b.h)-COALESCE(b.h*b.h/b.u,0), a.b-b.b-COALESCE(b.h*b.v/b.u,0) FROM __knots a JOIN T b ON a.rowid=b.i+1 AND a.code=b.code AND a.month=b.month
	)SELECT i, code, month, u, v FROM T;
	-- z
	INSERT OR IGNORE INTO __knots(rowid,code,month,z) WITH RECURSIVE
	T(i,code,month,z) AS (
		SELECT MAX(rowid), code, month, 0 FROM __knots GROUP BY code, month
		UNION ALL
		SELECT a.rowid, a.code, a.month, COALESCE((a.v-a.h*b.z)/a.u,0) FROM __knots a JOIN T b ON a.rowid=b.i-1 AND a.code=b.code AND a.month=b.month
	)SELECT i, code, month, z FROM T;
	-- A + B + C
	INSERT OR IGNORE INTO __knots(rowid,skewA,skewB,skewC) SELECT a.rowid, (b.z-a.z)/(6.0*a.h), a.z/2.0, -a.h*b.z/6.0-a.h*a.z/3.0+(b.y-a.y)/a.h FROM __knots a LEFT JOIN __knots b ON b.rowid=a.rowid+1 AND a.code=b.code AND a.month=b.month;  
	-- update skew coefficients in options
	UPDATE options SET vol=(SELECT y FROM __knots WHERE optID=options.rowid), skewA=(SELECT skewA FROM __knots WHERE optID=options.rowid), skewB=(SELECT skewB FROM __knots WHERE optID=options.rowid), skewC=(SELECT skewC FROM __knots WHERE optID=options.rowid) WHERE date=NEW.date;
	-- calculate ATM volatility
	INSERT OR REPLACE INTO optvols(date,code,month,volATM) SELECT NEW.date, code, month, volATM FROM (SELECT o.code code, o.month month, MAX(o.strike), o.vol+(f.C-o.strike)*(o.skewC+(f.C-o.strike)*(skewB+(f.C-o.strike)*skewA)) volATM FROM options o JOIN futures f ON o.date=f.date AND o.code=f.code AND o.undly=f.month WHERE o.date=NEW.date AND o.strike<=f.C AND o.skewA IS NOT NULL GROUP BY o.date, o.code, o.month);
	DELETE FROM __knots;
END;
