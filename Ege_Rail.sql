drop table if exists CARES2SQL.CARES2SQL.Justin_Edge_Rail_Update
select CAST(RouteID as bigint) as RouteID, RROWNER1, coalesce(STUSPS, STATEAB) as STUSPS,sum(cast(Attr_Length as float)) as Miles,left(Analysis_Date,10) as Analysis_Date, Type
into CARES2SQL.CARES2SQL.Justin_Edge_Rail_Loop
from CARES2SQL.CARES2SQL.Justin_LoopEdges a left join 
CARES2SQL.CARES2SQL.JUSTIN_NETWORKRAILS_LOOP b on a.SourceOID = b.ObjectID_1 where SourceName LIKE '%Rail%' Group by CAST(RouteID as bigint), RROWNER1, coalesce(STUSPS, STATEAB), left(Analysis_Date,10), Type