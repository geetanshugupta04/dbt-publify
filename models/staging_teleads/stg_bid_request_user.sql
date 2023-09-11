select 

_airbyte_data:_id,
_airbyte_data:createdOn,

_airbyte_data:user.buyeruid

from bidrequesttele.bid_request_schema._airbyte_raw_bidrequestad 
limit 100;