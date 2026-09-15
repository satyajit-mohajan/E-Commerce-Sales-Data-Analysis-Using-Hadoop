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

category_maximum_amount = FOREACH grouped_data GENERATE
    group AS category,
    MAX(sales.amount) AS max_amount;

STORE category_maximum_amount
INTO '/project/CategoryWiseMaximumAmount.txt'
USING PigStorage(',');