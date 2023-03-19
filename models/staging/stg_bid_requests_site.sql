select 

_airbyte_data:_id,
_airbyte_data:createdOn,
_airbyte_data:site,
_airbyte_data:site._id,
_airbyte_data:site.page,
_airbyte_data:site.domain,
_airbyte_data:site.publisher._id

_airbyte_data:user.buyeruid

from bidrequesttele.bid_request_schema._airbyte_raw_bidrequestad 
limit 100;