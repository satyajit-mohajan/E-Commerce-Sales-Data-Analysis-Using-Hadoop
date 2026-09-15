sales = LOAD '/project/Amazon Sale Report.csv'
USING PigStorage(',')
AS (
    index:int,
    order_id:chararray,
    date:chararray,
    status:chararray,
    fulfilment:chararray,
    sales_channel:chararray,
    ship_service_level:chararray,
    style:chararray,
    sku:chararray,
    category:chararray,
    size:chararray,
    asin:chararray,
    courier_status:chararray,
    qty:int,
    currency:chararray,
    amount:double,
    ship_city:chararray,
    ship_state:chararray,
    ship_postal_code:chararray,
    ship_country:chararray,
    promotion_ids:chararray,
    b2b:chararray,
    fulfilled_by:chararray,
    unnamed_22:chararray
);

sales = FILTER sales BY category != 'Category';

grouped_data = GROUP sales BY category;

category_quantity = FOREACH grouped_data GENERATE
    group AS category,
    SUM(sales.qty) AS total_quantity;

ordered_data = ORDER category_quantity BY total_quantity DESC;

top10_categories = LIMIT ordered_data 10;

STORE top10_categories
INTO '/project/Top10Categories.txt'
USING PigStorage(',');