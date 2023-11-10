drop table if exists CARES2SQL.CARES2SQL.JUSTIN_NETWORKLOOP_BASERESULTS_NEW
select a.instrument, a.state,b.ObjectID as FacilityID,e.Miles as RiverMiles, isnull(d.Miles,0) as RailMiles,RROWNER1, d.STUSPS as RailState, isnull(f.Miles,0) as RoadMiles, c.ObjectID as RouteID, c.Total_Length as TotalMiles
, (Case when a.instrument LIKE '%CORN%' then e.Miles * 0.000025 when a.instrument LIKE '%SOY%' then e.Miles * 0.000027 end) + 
(case when a.instrument like '%CORN%' then isnull(d.Miles,0) * 0.001240334 when a.instrument LIKE '%Soy%' then isnull(d.Miles,0) * 0.001144203 end) +
(case when a.instrument like '%CORN%' then isnull(f.miles,0) * 0.004320086 when a.instrument LIKE '%Soy%' then isnull(f.miles,0) * 0.004628634 end) as costperbushel
,[53], [BELLEVILLE LOCKS & DAM], [CANNELTON LOCK & DAM], [CAPT ANT MELDAHL LOCK & DAM]
,[CHAINS OF ROCKS L/D 27],[DRESDEN ISLAND LOCK & DAM],[GREENUP LOCKS & DAM], [HANNIBAL LOCKS & DAM],
[JOHN T. MYERSLOCK & DAM], [LAGRANGE LOCK & DAM], [LOCK & DAM 10], [LOCK & DAM 11],
[LOCK & DAM 12], [LOCK & DAM 13],[LOCK & DAM 14], [LOCK & DAM 15], [LOCK & DAM 16],
[LOCK & DAM 17],[LOCK & DAM 18], [LOCK & DAM 19], [LOCK & DAM 2], [LOCK & DAM 20], [LOCK & DAM 21], [LOCK & DAM 22], [lOCK & DAM 24],[LOCK & DAM 25],
[LOCK & DAM 3], [LOCK & DAM 4], [LOCK & DAM 5], [LOCK & DAM 52], [LOCK & DAM 5A], [LOCK & DAM 6], [LOCK & DAM 7], [LOCK & DAM 8], [LOCK & DAM 9],
[MARKLAND LOCKS & DAM], [MARSEILLES LOCK & DAM], [MCALPINE LOCKS & DAM], [MEL PRICE LOCK & DAM], [NEW CUMBERLAND LOCK & DAM],
[NEWBURGH LOCK & DAM], [OLMSTED LOCKS AND DAM], [PEORIA LOCK & DAM], [PIKE ISLAND LOCK & DAM], [RACINE LOCKS & DAM], [ROBERT C. BYRD],
[SMITHLAND LOCK & DAM],[STARVED ROCK LOCK & DAM],[WILLOW ISLAND LOCKS & DAM]
into CARES2SQL.CARES2SQL.JUSTIN_NETWORKLOOP_BASERESULTS_NEW
from CARES2SQL.CARES2SQL.JUSTIN_NETWORKSILOS a
FULL JOIN CARES2SQL.CARES2SQL.Justin_BaseFacilities b on a.ObjectID = b.ObjectID
full join CARES2SQL.CARES2SQL.Justin_BaseRoutes c on b.ObjectID = c.FacilityID
full join (select RouteID, RROWNER1, coalesce(STUSPS, STATEAB) as STUSPS,sum(cast(Attr_Length as float)) as Miles from CARES2SQL.CARES2SQL.Justin_BaseEdges a left join 
CARES2SQL.CARES2SQL.JUSTIN_NETWORKRAILS_LOOP b on a.SOURCEOID = b.ObjectID_1 where SourceName LIKE '%Rail%' Group by RouteID, RROWNER1, coalesce(STUSPS, STATEAB)) d on c.ObjectID = d.RouteID
full join (select RouteID, sum(cast(Attr_Length as float)) as Miles from CARES2SQL.CARES2SQL.Justin_BaseEdges where SourceName LIKE '%River%' Group by RouteID) e on c.ObjectID = e.RouteID
full join (select RouteID, sum(cast(Attr_Length as float)) as Miles from CARES2SQL.CARES2SQL.Justin_BaseEdges where SourceName LIKE '%Road%' Group by RouteID) f on c.ObjectID = f.RouteID
left join (select FacilityID, [53], [BELLEVILLE LOCKS & DAM], [CANNELTON LOCK & DAM], [CAPT ANT MELDAHL LOCK & DAM]
,[CHAINS OF ROCKS L/D 27],[DRESDEN ISLAND LOCK & DAM],[GREENUP LOCKS & DAM], [HANNIBAL LOCKS & DAM],
[JOHN T. MYERSLOCK & DAM], [LAGRANGE LOCK & DAM], [LOCK & DAM 10], [LOCK & DAM 11],
[LOCK & DAM 12], [LOCK & DAM 13],[LOCK & DAM 14], [LOCK & DAM 15], [LOCK & DAM 16],
[LOCK & DAM 17],[LOCK & DAM 18], [LOCK & DAM 19], [LOCK & DAM 2], [LOCK & DAM 20], [LOCK & DAM 21], [LOCK & DAM 22], [lOCK & DAM 24],[LOCK & DAM 25],
[LOCK & DAM 3], [LOCK & DAM 4], [LOCK & DAM 5], [LOCK & DAM 52], [LOCK & DAM 5A], [LOCK & DAM 6], [LOCK & DAM 7], [LOCK & DAM 8], [LOCK & DAM 9],
[MARKLAND LOCKS & DAM], [MARSEILLES LOCK & DAM], [MCALPINE LOCKS & DAM], [MEL PRICE LOCK & DAM], [NEW CUMBERLAND LOCK & DAM],
[NEWBURGH LOCK & DAM], [OLMSTED LOCKS AND DAM], [PEORIA LOCK & DAM], [PIKE ISLAND LOCK & DAM], [RACINE LOCKS & DAM], [ROBERT C. BYRD],
[SMITHLAND LOCK & DAM],[STARVED ROCK LOCK & DAM],[WILLOW ISLAND LOCKS & DAM]
from (select FacilityID, PMSNAME from CARES2SQL.CARES2SQL.Justin_baseLocks) as datasource 
PIVOT(count(PMSNAME) for PMSNAME IN([53], [BELLEVILLE LOCKS & DAM], [CANNELTON LOCK & DAM], [CAPT ANT MELDAHL LOCK & DAM]
,[CHAINS OF ROCKS L/D 27],[DRESDEN ISLAND LOCK & DAM],[GREENUP LOCKS & DAM], [HANNIBAL LOCKS & DAM],
[JOHN T. MYERSLOCK & DAM], [LAGRANGE LOCK & DAM], [LOCK & DAM 10], [LOCK & DAM 11],
[LOCK & DAM 12], [LOCK & DAM 13],[LOCK & DAM 14], [LOCK & DAM 15], [LOCK & DAM 16],
[LOCK & DAM 17],[LOCK & DAM 18], [LOCK & DAM 19], [LOCK & DAM 2], [LOCK & DAM 20], [LOCK & DAM 21], [LOCK & DAM 22], [lOCK & DAM 24],[LOCK & DAM 25],
[LOCK & DAM 3], [LOCK & DAM 4], [LOCK & DAM 5], [LOCK & DAM 52], [LOCK & DAM 5A], [LOCK & DAM 6], [LOCK & DAM 7], [LOCK & DAM 8], [LOCK & DAM 9],
[MARKLAND LOCKS & DAM], [MARSEILLES LOCK & DAM], [MCALPINE LOCKS & DAM], [MEL PRICE LOCK & DAM], [NEW CUMBERLAND LOCK & DAM],
[NEWBURGH LOCK & DAM], [OLMSTED LOCKS AND DAM], [PEORIA LOCK & DAM], [PIKE ISLAND LOCK & DAM], [RACINE LOCKS & DAM], [ROBERT C. BYRD],
[SMITHLAND LOCK & DAM],[STARVED ROCK LOCK & DAM],[WILLOW ISLAND LOCKS & DAM])) as pivottable) g on c.FacilityID = g.FacilityID
--where instrument = 'CORNFECHWD-C1'
order by state, instrument