CREATE TABLE products(ID TEXT PRIMARY KEY, code TEXT, name TEXT, strikeMult REAL, priceMult REAL, dollarMult REAL, minTickF REAL, minTickO REAL, precision INT, isDecimal INT);
CREATE TABLE intrate (date DATE PRIMARY KEY, r REAL);
CREATE TABLE futures (date DATE, code TEXT, month TEXT, H REAL, L REAL, C REAL, change REAL, volume INT, oi INT, PRIMARY KEY(date,code,month));
CREATE TABLE options (date DATE, code TEXT, month TEXT, undly TEXT, dte INT, strike REAL, type TEXT, price REAL, change REAL, volume INT, oi INT, vol REAL, skewA REAL, skewB REAL, skewC REAL, PRIMARY KEY(date,code,month,strike,type));
CREATE TABLE optvols (date DATE, code TEXT, month TEXT, volATM REAL, PRIMARY KEY(date,code,month));
CREATE TABLE __knots (optID INT PRIMARY key, code TEXT, month TEXT, t REAL, y REAL, h REAL, b REAL, u REAL, v REAL, z REAL, skewA REAL, skewB REAL, skewC REAL);
CREATE VIEW importCSV AS SELECT '' BizDt, '' Sym, '' ID, '' StrkPx, '' SecTyp, '' MMY, '' MatDt, '' PutCall, '' Exch, '' Desc, '' LastTrdDt, '' BidPrice, '' OpeningPrice, '' SettlePrice, '' SettleDelta, '' HighLimit, '' LowLimit, '' DHighPrice, '' DLowPrice, '' HighBid, '' LowBid, '' PrevDayVol, '' PrevDayOI, '' FixingPrice, '' UndlyExch, '' UndlyID, '' UndlySecTyp, '' UndlyMMY, '' BankBusDay
/* importCSV(BizDt,Sym,ID,StrkPx,SecTyp,MMY,MatDt,PutCall,Exch,"Desc",LastTrdDt,BidPrice,OpeningPrice,SettlePrice,SettleDelta,HighLimit,LowLimit,DHighPrice,DLowPrice,HighBid,LowBid,PrevDayVol,PrevDayOI,FixingPrice,UndlyExch,UndlyID,UndlySecTyp,UndlyMMY,BankBusDay) */;
CREATE VIEW settles AS SELECT o.date date, o.code code, o.month month, f.H H, f.L L, f.C C, f.change change, f.volume volume, f.oi oi, o.volATM volATM FROM optvols o NATURAL LEFT JOIN futures f
/* settles(date,code,month,H,L,C,change,volume,oi,volATM) */;
CREATE VIEW process AS SELECT '' function, '' date
/* process(function,date) */;
CREATE TRIGGER trigImportCSV_Fut INSTEAD OF INSERT ON importCSV WHEN NEW.SecTyp='FUT' AND NEW.ID IN (SELECT ID FROM products)
BEGIN
    -- insert new futures data
    INSERT OR REPLACE INTO futures(date,code,month,H,L,C,volume,oi)
    SELECT
        NEW.BizDt,
        p.code,
        SUBSTR(NEW.MMY,3,2)||SUBSTR('FGHJKMNQUVXZ',SUBSTR(NEW.MMY,-2)+0,1),
        IFNULL(NULLIF(NEW.DHighPrice,''),NULLIF(NEW.HighBid,''))*p.priceMult,
        IFNULL(NULLIF(NEW.DLowPrice ,''),NULLIF(NEW.LowBid ,''))*p.priceMult,
        NEW.SettlePrice*p.priceMult,
        IFNULL(NEW.PrevDayVol,0),
        IFNULL(NEW.PrevDayOI,0)
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
        CAST(JULIANDAY(NEW.MatDt)-JULIANDAY(NEW.BizDt)-2.0*CAST((JULIANDAY(NEW.MatDt)-JULIANDAY(NEW.BizDt)+5-STRFTIME('%w',NEW.MatDt))/7.0 AS INTEGER) AS INTEGER),
        NEW.StrkPx*p.strikeMult,
        SUBSTR('PC',NEW.PutCall+1,1),
        IFNULL(NULLIF(NEW.SettlePrice+0,9999999),0)*p.priceMult,
        IFNULL(NEW.PrevDayVol,0),
        IFNULL(NEW.PrevDayOI,0)
    FROM
        products p
    WHERE
        p.ID=NEW.ID;
END;
CREATE TRIGGER trigUpsertKnots BEFORE INSERT ON __knots WHEN EXISTS (SELECT 1 FROM __knots WHERE rowid=NEW.rowid)
BEGIN
    UPDATE __knots SET h=NEW.h, b=NEW.b WHERE NEW.h IS NOT NULL AND rowid=NEW.rowid;
    UPDATE __knots SET u=NEW.u, v=NEW.v WHERE NEW.u IS NOT NULL AND rowid=NEW.rowid;
    UPDATE __knots SET z=NEW.z          WHERE NEW.z IS NOT NULL AND rowid=NEW.rowid;
    UPDATE __knots SET skewA=NEW.skewA, skewB=NEW.skewB, skewC=NEW.skewC WHERE NEW.skewB IS NOT NULL AND rowid=NEW.rowid;
END;
CREATE TRIGGER trigProcessProcessDate INSTEAD OF INSERT ON process WHEN NEW.function='processDate'
BEGIN
    INSERT INTO process(function,date) VALUES ('setVolume', NEW.date);
    INSERT INTO process(function,date) VALUES ('setOI'    , NEW.date);
    INSERT INTO process(function,date) VALUES ('setChange', NEW.date);
    INSERT INTO process(function,date) VALUES ('calcVols' , NEW.date);
END;
CREATE TRIGGER trigProcessSetVolume INSTEAD OF INSERT ON process WHEN NEW.function='setVolume' AND EXISTS (SELECT date FROM futures WHERE date=NEW.date GROUP BY date HAVING MAX(volume)>0)
BEGIN
    -- update volume for previous day
    UPDATE futures SET
    volume = (SELECT f.volume FROM futures f WHERE f.date=NEW.date AND f.code=futures.code AND f.month=futures.month)
    WHERE date=(SELECT MAX(date) FROM futures WHERE date<NEW.date);
    UPDATE options SET
    volume = (SELECT o.volume FROM options o WHERE o.date=NEW.date AND o.code=options.code AND o.month=options.month AND o.strike=options.strike AND o.type=options.type)
    WHERE date=(SELECT MAX(date) FROM futures WHERE date<NEW.date);
    UPDATE futures SET volume = NULL WHERE date=NEW.date;
    UPDATE options SET volume = NULL WHERE date=NEW.date;
END;
CREATE TRIGGER trigProcessSetOI INSTEAD OF INSERT ON process WHEN NEW.function='setOI'
BEGIN
    -- update open interest for previous day
    UPDATE futures SET
    oi = (SELECT f.oi FROM futures f WHERE f.date=NEW.date AND f.code=futures.code AND f.month=futures.month)
    WHERE date=(SELECT MAX(date) FROM futures WHERE date<NEW.date);
    UPDATE options SET
    oi = (SELECT o.oi FROM options o WHERE o.date=NEW.date AND o.code=options.code AND o.month=options.month AND o.strike=options.strike AND o.type=options.type)
    WHERE date=(SELECT MAX(date) FROM futures WHERE date<NEW.date);
    UPDATE futures SET oi = NULL WHERE date=NEW.date;
    UPDATE options SET oi = NULL WHERE date=NEW.date;
END;
CREATE TRIGGER trigProcessSetChange INSTEAD OF INSERT ON process WHEN NEW.function='setChange'
BEGIN
    -- set change for current day
    UPDATE futures SET
    change=ROUND(C-(SELECT f.C FROM futures f WHERE f.date=(SELECT MAX(date) FROM futures WHERE date<NEW.date) AND f.code=futures.code AND f.month=futures.month),6)
    WHERE date=NEW.date;
    UPDATE options SET
    change=ROUND(price-(SELECT o.price FROM options o WHERE o.date=(SELECT MAX(date) FROM futures WHERE date<NEW.date) AND o.code=options.code AND o.month=options.month AND o.strike=options.strike AND o.type=options.type),6)
    WHERE date=NEW.date;
    -- delete holidays
    DELETE FROM futures WHERE futures.date=NEW.date AND EXISTS (SELECT f.date FROM futures f WHERE f.date=NEW.date GROUP BY f.date HAVING MAX(ABS(f.change))=0);
    DELETE FROM options WHERE options.date=NEW.date AND EXISTS (SELECT o.date FROM options o WHERE o.date=NEW.date GROUP BY o.date HAVING MAX(ABS(o.change))=0);
END;
CREATE TRIGGER trigProcessCalcVols INSTEAD OF INSERT ON process WHEN NEW.function='calcVols'
BEGIN
    -- generate cubic spline
    DELETE FROM __knots;
    INSERT INTO __knots(optID,code,month,t,y) WITH
        currate(date,r) AS (SELECT date, r FROM intrate WHERE date<=NEW.date ORDER BY date DESC LIMIT 1)
        SELECT o.optId, o.code, o.month, o.strike, whaley_IV(f.C,o.strike,CASE o.type WHEN 'P' THEN 0 ELSE 1 END,o.price,o.dte/261.0,MAX(IFNULL((SELECT r FROM currate),0),0.0001)) FROM (SELECT rowid optId, date, code, month, undly, dte, strike, type, MIN(price) price FROM options WHERE date=NEW.date AND dte GROUP BY date, code, month, strike) o NATURAL JOIN products p JOIN futures f ON o.date=f.date AND o.code=f.code AND o.undly=f.month WHERE o.price>p.minTickO ORDER BY o.code, o.month, o.strike;
    -- h + b
    INSERT OR IGNORE INTO __knots(rowid,h,b) SELECT a.rowid, b.t-a.t, 6*(b.y-a.y)/(b.t-a.t) FROM __knots a LEFT JOIN __knots b ON b.rowid=a.rowid+1 AND a.code=b.code AND a.month=b.month;
    -- u + v
    INSERT OR IGNORE INTO __knots(rowid,code,month,u,v) WITH RECURSIVE
    T(i,code,month,h,b,u,v) AS (
        SELECT MIN(rowid), code, month, h, b, NULL, NULL FROM __knots GROUP BY code, month
        UNION ALL
        SELECT a.rowid, a.code, a.month, a.h, a.b, 2*(a.h+b.h)-IFNULL(b.h*b.h/b.u,0), a.b-b.b-IFNULL(b.h*b.v/b.u,0) FROM __knots a JOIN T b ON a.rowid=b.i+1 AND a.code=b.code AND a.month=b.month
    )SELECT i, code, month, u, v FROM T;
    -- z
    INSERT OR IGNORE INTO __knots(rowid,code,month,z) WITH RECURSIVE
    T(i,code,month,z) AS (
        SELECT MAX(rowid), code, month, 0 FROM __knots GROUP BY code, month
        UNION ALL
        SELECT a.rowid, a.code, a.month, IFNULL((a.v-a.h*b.z)/a.u,0) FROM __knots a JOIN T b ON a.rowid=b.i-1 AND a.code=b.code AND a.month=b.month
    )SELECT i, code, month, z FROM T;
    -- A + B + C
    INSERT OR IGNORE INTO __knots(rowid,skewA,skewB,skewC) SELECT a.rowid, (b.z-a.z)/(6.0*a.h), a.z/2.0, -a.h*b.z/6.0-a.h*a.z/3.0+(b.y-a.y)/a.h FROM __knots a LEFT JOIN __knots b ON b.rowid=a.rowid+1 AND a.code=b.code AND a.month=b.month;
    -- update skew coefficients in options
    UPDATE options SET vol=(SELECT y FROM __knots WHERE optID=options.rowid), skewA=(SELECT skewA FROM __knots WHERE optID=options.rowid), skewB=(SELECT skewB FROM __knots WHERE optID=options.rowid), skewC=(SELECT skewC FROM __knots WHERE optID=options.rowid) WHERE date=NEW.date;
    -- calculate ATM volatility
    INSERT OR REPLACE INTO optvols(date,code,month,volATM) SELECT NEW.date, code, month, volATM FROM (SELECT o.code code, o.month month, MAX(o.strike), o.vol+(f.C-o.strike)*(o.skewC+(f.C-o.strike)*(skewB+(f.C-o.strike)*skewA)) volATM FROM options o JOIN futures f ON o.date=f.date AND o.code=f.code AND o.undly=f.month WHERE o.date=NEW.date AND o.strike<=f.C AND o.skewA IS NOT NULL GROUP BY o.date, o.code, o.month);
    DELETE FROM __knots;
END;
CREATE INDEX products_idx_code ON products(code);
CREATE VIEW cmedata AS
WITH
dateOpt(date) AS (SELECT MIN(date) FROM (SELECT DISTINCT date FROM futures ORDER BY date DESC LIMIT 260)),
dateFut(date) AS (SELECT MIN(date) FROM (SELECT DISTINCT date FROM futures ORDER BY date DESC LIMIT 520)),
allCM(code,month) AS (SELECT DISTINCT code, month FROM optvols WHERE date>=(SELECT date FROM dateOpt)),
opt(date,code,month,vol) AS (SELECT date, code, month, ROUND(volATM,5) FROM optvols WHERE date>=(SELECT date FROM dateOpt) AND (code,month) IN (SELECT code, month FROM allCM) ORDER BY date DESC),
fut(date,code,month,C) AS (SELECT date, code, month, C FROM futures WHERE date>=(SELECT date FROM dateFut) AND (code,month) IN (SELECT code, month FROM allCM) ORDER BY date DESC),
dates(date) AS (SELECT DISTINCT date FROM opt ORDER BY date DESC),
expiry(code,month,date) AS (SELECT code, month, MAX(date) FROM opt GROUP BY code, month),
offset(code,month,offset) AS (SELECT e.code, e.month, COUNT(d.date) FROM expiry e LEFT JOIN dates d ON e.date<d.date GROUP BY e.code, e.month),
serial(code,month,isSerial) AS (SELECT DISTINCT code, month, 1 FROM optvols EXCEPT SELECT DISTINCT code, month, 1 FROM futures),
optMonth(code,month,vols) AS (SELECT code, month, json_group_array(vol) FROM opt GROUP BY code, month ORDER BY code, month),
futMonth(code,month,settles) AS (SELECT code, month, json_group_array(C) FROM fut GROUP BY code, month ORDER BY code, month),
data(products) AS (SELECT json_object('code',opt.code,'name',p.name,'precision',p.precision,'months',json_group_array(json_object('month',opt.month,'offset',o.offset,'serial',IFNULL(s.isSerial,0),'vols',json_extract(opt.vols,'$'),'settles',IFNULL(json_extract(fut.settles,'$'),json_array())))) FROM optMonth opt NATURAL LEFT JOIN futMonth fut NATURAL LEFT JOIN serial s NATURAL JOIN offset o NATURAL JOIN products p GROUP BY opt.code ORDER BY opt.code)
SELECT json_object('data',(SELECT json_group_array(json_extract(products,'$')) FROM data),'dates',(SELECT json_group_array(date) FROM dates)) json
/* cmedata(json) */;

INSERT INTO products VALUES('C','C','Corn',100.0,100.0,50.0,0.25,0.125,2,0);
INSERT INTO products VALUES('W','W','Wheat',100.0,100.0,50.0,0.25,0.125,2,0);
INSERT INTO products VALUES('KW','KW','KC Wheat',100.0,100.0,50.0,0.25,0.125,2,0);
INSERT INTO products VALUES('S','S','Soybeans',100.0,100.0,50.0,0.25,0.125,2,0);
INSERT INTO products VALUES('06','SM','Soybean Meal',1.0,1.0,100.0,0.1,0.05,1,1);
INSERT INTO products VALUES('07','BO','Soybean Oil',1.0,1.0,600.0,0.01,0.005,2,1);
INSERT INTO products VALUES('48','LC','Live Cattle',1.0,1.0,400.0,0.025,0.025,3,1);
INSERT INTO products VALUES('62','FC','Feeder Cattle',1.0,1.0,500.0,0.025,0.0125,3,1);
INSERT INTO products VALUES('LN','LH','Lean Hogs',1.0,1.0,400.0,0.025,0.0125,3,1);

