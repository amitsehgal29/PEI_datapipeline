-- SQL to calculate Profit by Year

SELECT
    YEAR(ORDER_DATE) AS YEAR,
    SUM(PROFIT) AS TOTAL_PROFIT
FROM
    curated.order_extended
GROUP BY
    YEAR(ORDER_DATE)
ORDER BY
    YEAR(ORDER_DATE);

-- SQL to calculate Profit by Year + Product Category

SELECT
    YEAR(ORDER_DATE) AS YEAR,
    CATEGORY,
    SUM(PROFIT) AS TOTAL_PROFIT
FROM
    curated.order_extended
GROUP BY
    YEAR(ORDER_DATE), CATEGORY
ORDER BY
    YEAR(ORDER_DATE), CATEGORY;

-- SQL to calculate Profit by Customer

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    SUM(PROFIT) AS TOTAL_PROFIT
FROM
    curated.order_extended
GROUP BY
    CUSTOMER_ID, CUSTOMER_NAME
ORDER BY
    TOTAL_PROFIT DESC;

-- SQL to calculate Profit by Customer + Year

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    YEAR(ORDER_DATE) AS YEAR,
    SUM(PROFIT) AS TOTAL_PROFIT
FROM
    curated.order_extended
GROUP BY
    CUSTOMER_ID, CUSTOMER_NAME, YEAR(ORDER_DATE)
ORDER BY
    CUSTOMER_ID, YEAR(ORDER_DATE);