{% test hub_check(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} = 'Dubnaa'

{% endtest %}