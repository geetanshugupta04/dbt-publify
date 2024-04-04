{% macro calculate_weighted_variance(partition_by, weighted_mean, fp, bids) %}
    sum({{ bids }} * power({{ fp }} - {{ weighted_mean }}, 2)) over (
        partition by {{ partition_by }}
        range between unbounded preceding and unbounded following
    ) / sum({{ bids }}) over (
        partition by {{ partition_by }}
        range between unbounded preceding and unbounded following
    )
{% endmacro %}
