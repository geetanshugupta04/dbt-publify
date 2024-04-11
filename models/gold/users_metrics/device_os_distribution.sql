{% macro get_payment_methods() %}
{% set payment_methods_query %}
    select 
    distinct ad_type 
    from dbt_paytunes_gold.gaming_users
    order by 1
{% endset %}
{% endmacro %}


/*


ssp app name in 


yupptv
q play+
samsung tv


YuppTV	null
YuppTV - Hisense	yupptv.hisense
Samsung TV	YuppTV
YuppTV	YuppTV
Opera	com.yupptv.operatv
COUCHPLAY_CTV	Sudoku 1
YuppTV - LG	YuppTV
wedotv	ed
Fetch TV	JioTV
3Cat	null
Sunrise	null

YuppTV	null
Samsung TV	YuppTV
Viki: TV Dramas & Movies	Viki
BrowseHere	TCL CHANNEL
TCL Channel	TCL CHANNEL

YuppTV for AndroidTV	YuppTV
YuppTV - LiveTV Movies Shows	Rooter


company_make, master_model 
Jio	Fiber Set Top Box
AMAZON	Fire TV Stick
Sony	32W830K
Xiaomi	Mi TV New 4K QLED
Sony	Bravia*

LG webOS
Samsung Tizen
Hisense Vidaa 
Vizio smartcast








*/