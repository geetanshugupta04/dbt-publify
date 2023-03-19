select 

_airbyte_data:_id,
_airbyte_data:createdOn,
_airbyte_data:device,
_airbyte_data:device.geo.country,
_airbyte_data:device.geo.lat,
_airbyte_data:device.geo.lon,
_airbyte_data:device.ifa,
_airbyte_data:device.ip,
_airbyte_data:device.ipv6,
_airbyte_data:device.os,
_airbyte_data:device.osv,
_airbyte_data:buyeruid

from bidrequesttele.bid_request_schema._airbyte_raw_bidrequestad 
limit 100;