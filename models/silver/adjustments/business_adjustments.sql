with 
track_camp_lineitems_after_pincode_adjustments as (

    select * from 
)
merged_with_adjustments as (
  select
    campaign_name,
    campaign_start_date,
    campaign_end_date,
    c_line_item_name,
    c_line_item_type,
    line_item_name,
    line_item_type,
    publisher_group,
    dealcode,
    ssp,
    bundle,
    date,
    impression,
    -- Complete
    (
      case
        -- Offline ssp adjustment
        when ssp = 'offline' 
		-- Change data for line_item_type = display and c_line_item_type not display
        or (
          (lower(line_item_type)) = 'display'
          and (lower(c_line_item_type)) not in ('display', 'retargeted_banner')
        ) then impression
        else complete
      end
    ) as complete,
  
    -- Creative view
    (
      case
        -- Offline ssp adjustment
        when ssp = 'offline' 
		-- Change creative view data for spotify, saavn  publisher
        or lower(publisher_group) in ('spotify', 'saavan') 
		-- Change data for Triton Audio using global parameter for 90%
        or lower(c_line_item_type) in ('audio', 'audio_with_companion', 'video') 
		-- Change data for line_item_type = display and c_line_item_type not display
        or (
          (lower(line_item_type)) = 'display'
          and (lower(c_line_item_type)) not in ('display', 'retargeted_banner')
        ) then impression
        else creative_view
      end
    ) as creative_view,
    click
  from
    merged
),
