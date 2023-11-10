Drop table if exists CARES2SQL.CARES2SQL.Justin_LoopEdges_River_Loop

select CAST(RouteID as bigint) as RouteID, sum(cast(Attr_Length as float)) as Miles , left(replace(Analysis_Date,'"',''),10)  as Analysis_Date, Type
into CARES2SQL.CARES2SQL.Justin_LoopEdges_River_Loop
from CARES2SQL.CARES2SQL.Justin_LoopEdges where SourceName LIKE '%River%' Group by CAST(RouteID as bigint), left(replace(Analysis_Date,'"',''),10), Type