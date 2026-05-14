with source as (

    select
        *
    from {{ source('fakestore_raw', 'products') }}

),

renamed as (

    select
        safe_cast(id as int64) as product_id,
        cast(title as string) as product_title,
        safe_cast(price as numeric) as product_price,
        cast(description as string) as product_description,
        lower(trim(cast(category as string))) as product_category,
        cast(image as string) as product_image_url,

        safe_cast(rating_rate as numeric) as rating_rate,
        safe_cast(rating_count as int64) as rating_count,

        safe_cast(_ingested_at as timestamp) as ingested_at,
        cast(_source as string) as source_name

    from source

),

deduplicated as (

    select
        *
    from renamed
    qualify row_number() over (
        partition by product_id
        order by ingested_at desc
    ) = 1

)

select
    *
from deduplicated