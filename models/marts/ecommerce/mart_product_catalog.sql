with products as (

    select
        *
    from {{ ref('stg_fakestore_products') }}

),

category_summary as (

    select
        product_category,

        count(*) as product_count,

        min(product_price) as min_product_price,
        max(product_price) as max_product_price,
        avg(product_price) as average_product_price,
        sum(product_price) as total_catalog_value,

        avg(rating_rate) as average_rating,
        sum(rating_count) as total_rating_count,

        countif(rating_rate >= 4) as highly_rated_product_count,
        countif(product_price >= 100) as premium_product_count,

        current_timestamp() as mart_updated_at

    from products

    group by
        product_category

)

select
    *
from category_summary