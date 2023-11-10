Drop table if exists CARES2SQL.CARES2SQL.Justin_NetworkLoop_RESULTS10_27_23
select a.*, 
case when a.instrument like '%SOY%' then (case when c.Barge IS NULL then NULL else (isnull(a.RiverMiles,0) * isnull(c.Barge,0)) + Case when isnull(a.RailMiles,0) > 0 then isnull(c.Rail,0) else 0 end + (isnull(a.RoadMiles,0) * isnull(c.Road,0)) end) 
when a.instrument like '%CORN%' then (case when z.Barge IS NULL then NULL else (isnull(a.RiverMiles,0) * isnull(z.Barge,0)) + Case when isnull(a.RailMiles,0) > 0 then isnull(z.Rail,0) else 0 end + (isnull(a.RoadMiles,0) * isnull(z.Road,0)) end)
 end as CostPerBushel, case WHEN a.TotalMiles IS NULL then NULL when round(a.TotalMiles,0) <> round(b.TotalMiles,0) then 1 else 0 end as RouteChange 
, MAX(case when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) AND (d.FLOOD_Flash_flooding = 1 OR d.Hurricanes_Typhoons_Tropical_Storms = 1 OR d.Tornadoes = 1 OR d.Mudslides_Debris_Flows_Landslides = 1 OR d.Winter_Storms_Ice_Storms_Snow_Blizzard = 1) then 1 else 0 end) as disasterdummy
, MAX(case when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) AND (d.FLOOD_Flash_flooding = 1) then 1 else 0 end) as flashflood
, MAX(case when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) AND (d.Hurricanes_Typhoons_Tropical_Storms = 1) then 1 else 0 end) as hurricane
, MAX(case when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) AND (d.Tornadoes = 1) then 1 else 0 end) as tornado
, MAX(case when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) AND (d.Mudslides_Debris_Flows_Landslides = 1) then 1 else 0 end) as mudslide
, MAX(case when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) AND (d.Winter_Storms_Ice_Storms_Snow_Blizzard = 1) then 1 else 0 end) as winterstorm
, MAX(case when e.FIPS IS NULL then NULL when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) and a.FIPS in (e.fips) and (d.FLOOD_Flash_flooding = 1) then 1 else 0 end) as riveradjct_flashflood
, MAX(case when e.FIPS IS NULL then NULL when ((cast(a.Analysis_Date as datetime) >= cast(d.Begin_Date as datetime)) and (cast(a.Analysis_Date as datetime) <= cast(d.End_Date as datetime))) and a.FIPS in (e.fips) and (d.Hurricanes_Typhoons_Tropical_Storms = 1) then 1 else 0 end) as riveradjct_hurricane
into CARES2SQL.CARES2SQL.Justin_NetworkLoop_RESULTS10_27_23
from (
select a.instrument, a.FIPS, a.state,cast(left(b.Analysis_Date,10) as nvarchar(50)) as Analysis_Date,b.ObjectID as FacilityID,e.Miles as RiverMiles, isnull(d.Miles,0) as RailMiles,RROWNER1, d.STUSPS as RailState, isnull(f.Miles,0) as RoadMiles, c.ObjectID as RouteID, c.Total_Length as TotalMiles
,[53], [BELLEVILLE LOCKS & DAM], [CANNELTON LOCK & DAM], [CAPT ANT MELDAHL LOCK & DAM]
,[CHAINS OF ROCKS L/D 27],[DRESDEN ISLAND LOCK & DAM],[GREENUP LOCKS & DAM], [HANNIBAL LOCKS & DAM],
[JOHN T. MYERSLOCK & DAM], [LAGRANGE LOCK & DAM], [LOCK & DAM 10], [LOCK & DAM 11],
[LOCK & DAM 12], [LOCK & DAM 13],[LOCK & DAM 14], [LOCK & DAM 15], [LOCK & DAM 16],
[LOCK & DAM 17],[LOCK & DAM 18], [LOCK & DAM 19], [LOCK & DAM 2], [LOCK & DAM 20], [LOCK & DAM 21], [LOCK & DAM 22], [lOCK & DAM 24],[LOCK & DAM 25],
[LOCK & DAM 3], [LOCK & DAM 4], [LOCK & DAM 5], [LOCK & DAM 52], [LOCK & DAM 5A], [LOCK & DAM 6], [LOCK & DAM 7], [LOCK & DAM 8], [LOCK & DAM 9],
[MARKLAND LOCKS & DAM], [MARSEILLES LOCK & DAM], [MCALPINE LOCKS & DAM], [MEL PRICE LOCK & DAM], [NEW CUMBERLAND LOCK & DAM],
[NEWBURGH LOCK & DAM], [OLMSTED LOCKS AND DAM], [PEORIA LOCK & DAM], [PIKE ISLAND LOCK & DAM], [RACINE LOCKS & DAM], [ROBERT C. BYRD],
[SMITHLAND LOCK & DAM],[STARVED ROCK LOCK & DAM],[WILLOW ISLAND LOCKS & DAM]
from CARES2SQL.CARES2SQL.JUSTIN_NETWORKSILOS_LOOP a
left JOIN CARES2SQL.CARES2SQL.Justin_LoopFacilities b on a.ObjectID = b.ObjectID and left(a.type,4) = left(b.type,4) 
full join CARES2SQL.CARES2SQL.Justin_LoopRoutes c on b.ObjectID = c.FacilityID and cast(left(b.Analysis_Date,10) as date) = cast(left(c.Analysis_Date,10) as date) and left(b.type,4) = left(c.type,4) 
full join CARES2SQL.CARES2SQL.Justin_Edge_Rail_Loop d on c.ObjectID = d.RouteID and cast(left(c.Analysis_Date,10) as date) = cast(d.Analysis_Date as date) and left(c.type,4) = left(d.type,4) 
full join CARES2SQL.CARES2SQL.Justin_LoopEdges_River_Loop e on c.ObjectID = e.RouteID and cast(left(c.Analysis_Date,10) as date) = cast(e.Analysis_Date as date) and left(c.type,4) = left(e.type,4)
full join CARES2SQL.CARES2SQL.Justin_LoopEdges_Road f on c.ObjectID = f.RouteID  and cast(left(c.Analysis_Date,10) as date) = cast(f.Analysis_Date as date) and left(c.type,4) = left(f.type,4)
left join CARES2SQL.CARES2SQL.Justin_Locks_Piovt_Loop  g on cast(c.FacilityID as int)= g.FacilityID and cast(left(c.Analysis_Date,10) as date) = cast(g.Analysis as date) and left(c.type,4) = left(g.type,4)
) a left join
(select instrument, MAX(totalMiles) as TotalMiles from CARES2SQL.CARES2SQL.JUSTIN_NETWORKLOOP_BASERESULTS_NEW group by instrument) b on a.instrument = b.instrument
left join
CARES2SQL.CARES2SQL.Justin_LoopCosts_Soy c on CAST(left(a.Analysis_Date,10) as date) = CAST(c.Week_of as date)
left join
CARES2SQL.CARES2SQL.Justin_LoopCosts_Corn z on CAST(left(a.Analysis_Date,10) as date) = cast(z.Week_of as date)
left join
(select * from CARES2SQL.CARES2SQL.Justin_DisasterData where isdate(End_Date) = 1) d on a.FIPS = d.FIPS and cast(left(a.Analysis_Date,10) as date) BETWEEN CAST(d.Begin_Date as datetime) and cast(d.End_Date as datetime)
left join CARES2SQL.CARES2SQL.Justin_RiverCounties e on a.FIPS = e.FIPS
group by a.instrument, a.FIPS, a.state, Analysis_Date, FacilityID, RiverMiles, RailMiles,RROWNER1, RailState, RoadMiles, RouteID, a.TotalMiles
,[53], [BELLEVILLE LOCKS & DAM], [CANNELTON LOCK & DAM], [CAPT ANT MELDAHL LOCK & DAM]
,[CHAINS OF ROCKS L/D 27],[DRESDEN ISLAND LOCK & DAM],[GREENUP LOCKS & DAM], [HANNIBAL LOCKS & DAM],
[JOHN T. MYERSLOCK & DAM], [LAGRANGE LOCK & DAM], [LOCK & DAM 10], [LOCK & DAM 11],
[LOCK & DAM 12], [LOCK & DAM 13],[LOCK & DAM 14], [LOCK & DAM 15], [LOCK & DAM 16],
[LOCK & DAM 17],[LOCK & DAM 18], [LOCK & DAM 19], [LOCK & DAM 2], [LOCK & DAM 20], [LOCK & DAM 21], [LOCK & DAM 22], [lOCK & DAM 24],[LOCK & DAM 25],
[LOCK & DAM 3], [LOCK & DAM 4], [LOCK & DAM 5], [LOCK & DAM 52], [LOCK & DAM 5A], [LOCK & DAM 6], [LOCK & DAM 7], [LOCK & DAM 8], [LOCK & DAM 9],
[MARKLAND LOCKS & DAM], [MARSEILLES LOCK & DAM], [MCALPINE LOCKS & DAM], [MEL PRICE LOCK & DAM], [NEW CUMBERLAND LOCK & DAM],
[NEWBURGH LOCK & DAM], [OLMSTED LOCKS AND DAM], [PEORIA LOCK & DAM], [PIKE ISLAND LOCK & DAM], [RACINE LOCKS & DAM], [ROBERT C. BYRD],
[SMITHLAND LOCK & DAM],[STARVED ROCK LOCK & DAM],[WILLOW ISLAND LOCKS & DAM], case when a.instrument like '%SOY%' then (case when c.Barge IS NULL then NULL else (isnull(a.RiverMiles,0) * isnull(c.Barge,0)) + Case when isnull(a.RailMiles,0) > 0 then isnull(c.Rail,0) else 0 end + (isnull(a.RoadMiles,0) * isnull(c.Road,0)) end) 
when a.instrument like '%CORN%' then (case when z.Barge IS NULL then NULL else (isnull(a.RiverMiles,0) * isnull(z.Barge,0)) + Case when isnull(a.RailMiles,0) > 0 then isnull(z.Rail,0) else 0 end + (isnull(a.RoadMiles,0) * isnull(z.Road,0)) end)
 end, case when a.TotalMiles IS NULL then NULL when round(a.TotalMiles,0) <> round(b.TotalMiles,0) then 1 else 0 end
order by state, instrument, analysis_date