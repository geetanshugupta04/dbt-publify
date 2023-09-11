select 

_airbyte_data:_id,
_airbyte_data:createdOn,
_airbyte_data:imp[0],
_airbyte_data:imp[0]._id,
_airbyte_data:imp[0].banner,
_airbyte_data:imp[0].banner.h,
_airbyte_data:imp[0].banner.pos,
_airbyte_data:imp[0].banner.w,
_airbyte_data:imp[0].bidfloor,
_airbyte_data:imp[0].bidfloorcur,
_airbyte_data:imp[0].instl,
_airbyte_data:imp[0].secure,
_airbyte_data:imp[0].tagid,
_airbyte_data:user.buyeruid

from bidrequesttele.bid_request_schema._airbyte_raw_bidrequestad 
limit 100;