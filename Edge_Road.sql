drop table if exists [DB].[Schema].Justin_LoopEdges_Road
select CAST(RouteID as bigint) as RouteID, sum(cast(Attr_Length as float)) as Miles, left(Analysis_Date,10) as Analysis_Date, Type
into [DB].[Schema].Justin_LoopEdges_Road
from [DB].[Schema].Justin_LoopEdges where SourceName LIKE '%Road%' Group by CAST(RouteID as bigint), left(Analysis_Date,10), Type
