{% macro calculate_weighted_mean(partition_by, fp, bids) %}
            sum(fp * bids) over (
                partition by {{ partition_by}}
                range between unbounded preceding and unbounded following
            ) / sum(bids) over (
                partition by {{ partition_by}}
                range between unbounded preceding and unbounded following
            )
{% endmacro %}
