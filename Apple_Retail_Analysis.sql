create database project3;

use project3;

# creating tables
create table stores(
	store_id varchar(10) primary key,
    store_name varchar(30),
    city varchar(25),
    country varchar(25)
);

create table category(
	category_id varchar(10) primary key,
    category_name varchar(20)
);

create table products(
	product_id varchar(10) primary key,
    product_name varchar(35),
    category_id varchar(10),
    launch_date date,
    price double precision,
    foreign key (category_id) references category(category_id)
);

create table sales(
	sale_id varchar(15) primary key,
    sale_date varchar(10),
    store_id varchar(10),
    product_id varchar(10),
    quantity int,
    foreign key (store_id) references stores(store_id),
    foreign key (product_id) references products(product_id)
);
UPDATE sales
SET sale_date = STR_TO_DATE(sale_date, '%d-%m-%Y');
ALTER TABLE sales MODIFY sale_date DATE;

create table warranty(
	claim_id varchar(10) primary key,
    claim_date date,
    sale_id varchar(15),
    repair_status varchar(15),
    foreign key (sale_id) references sales(sale_id)
);

select * from products;
select * from category;
select * from stores;
select * from sales;
select * from warranty;

#Loading large data into sales table
SHOW VARIABLES LIKE 'local_infile'; # should be on to import local data

LOAD DATA LOCAL INFILE 'D:/SQL_Projects/Apple_Retail_Sales/sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

#Loading large data into warranty table
LOAD DATA LOCAL INFILE 'D:/SQL_Projects/Apple_Retail_Sales/warranty.csv'
INTO TABLE warranty
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# Improving query performance- Index created on columns in sales table for faster read of data

# see all existing index in the table- index for primary/foreign keys columns are created automatically by mySQL
SHOW INDEX FROM sales;

# without index table_scan execution time= 306ms
# after creating index for the date column, execution time= 5ms
EXPLAIN ANALYZE
SELECT * FROM sales
WHERE sale_date='2021-02-24';

CREATE INDEX sales_sale_date ON sales(sale_date);

  
# Business Problems
-- 1. Find the number of stores in each country.
SELECT country, COUNT(store_id) AS store_count
FROM stores
GROUP BY country
ORDER BY store_count DESC;


-- 2. Calculate the total number of units sold by each store.
SELECT st.store_id, st.store_name, SUM(s.quantity) AS total_units_sold
FROM sales AS s
JOIN stores AS st 
ON s.store_id = st.store_id
GROUP BY 1;


-- 3. Identify how many sales occurred in December 2023.
SELECT COUNT(*) AS total_sales_Dec2023
FROM sales
WHERE sale_date BETWEEN '2023-12-1' AND '2023-12-31';

# slow as doesn't makes use of index
SELECT COUNT(*) AS total_sales
FROM sales
WHERE YEAR(sale_date) = 2023 AND MONTH(sale_date) = 12;


-- 4. Determine how many stores have never had a warranty claim filed.
SELECT COUNT(*)
FROM stores
WHERE store_id NOT IN (
	SELECT DISTINCT s.store_id
	FROM sales AS s
	RIGHT JOIN warranty AS w 
    ON s.sale_id = w.sale_id);


-- 5. Calculate the percentage of warranty claims marked as "Rejected".
SET SQL_SAFE_UPDATES = 1;

-- trimming extra characters \r 
UPDATE warranty
SET repair_status = REPLACE(repair_status, CHAR(13), '');

SELECT 
	ROUND(COUNT(*) / (SELECT COUNT(*) FROM warranty) * 100, 2) AS reject_percent
FROM warranty
WHERE repair_status = 'Rejected';


-- 6. Identify which store had the highest total units sold in the last 2 years.
SELECT s.store_id, st.store_name, SUM(s.quantity) AS total_units
FROM sales as s
JOIN stores as st
ON s.store_id= st.store_id
WHERE s.sale_date >= (CURRENT_DATE - INTERVAL 2 YEAR)
GROUP BY s.store_id
ORDER BY total_units DESC
LIMIT 1;


-- 7. Count the number of unique products sold in the last 2 years.
SELECT COUNT(DISTINCT product_id)
FROM sales
WHERE sale_date >= (CURRENT_DATE - INTERVAL 2 YEAR);


-- 8. Find the average price of products in each category.
SELECT p.category_id, c.category_name, ROUND(AVG(p.price), 2) AS avg_category_price
FROM products AS p
JOIN category AS c
ON p.category_id= c.category_id
GROUP BY p.category_id
ORDER BY avg_category_price DESC;


-- 9. How many warranty claims were filed in first half of 2024?
SELECT COUNT(*) as first_half_claims
FROM warranty
WHERE claim_date BETWEEN '2024-1-1' AND '2024-6-1';


-- 10. For each store, identify the best-selling day based on highest quantity sold.
-- Using Sub-query
select *
from (select 
		store_id, DATE_FORMAT(sale_date, '%W') AS day, 
		sum(quantity) as total_quantity, 
		rank() over(partition by store_id order by sum(quantity) desc) as high_rank
	from sales
	group by store_id, day) as t1
where high_rank =1;


-- 11. Identify the least selling product in each country for each year based on total units sold.
-- Using CTE
with product_rank as(
	select st.country, p.product_name, sum(s.quantity),
	rank() over(partition by st.country order by sum(quantity)) as low_rank
	from sales as s
	join stores as st
	on s.store_id = st.store_id
	join products as p
	on s.product_id = p.product_id
	group by 1,2
)
select * from product_rank
where low_rank=1;


-- 12. Calculate how many warranty claims were filed within 180 days of a product sale.
SELECT COUNT(s.sale_id) AS EarlyClaim_Count
FROM warranty AS w
LEFT JOIN sales AS s 
ON s.sale_id = w.sale_id
WHERE DATEDIFF(w.claim_date, s.sale_date) <= 180;


-- 13. Determine how many warranty claims were filed for products launched in the last two years.
SELECT p.product_name, p.launch_date,
    COUNT(w.claim_id) AS total_claims
FROM sales AS s
JOIN products AS p 
ON s.product_id = p.product_id
JOIN warranty AS w 
ON s.sale_id = w.sale_id
WHERE p.launch_date >= CURRENT_DATE() - INTERVAL 2 YEAR
GROUP BY 1 , 2;
-- where p.launch_date between '2023-11-18' and '2025-11-18';


-- 14. List the months in the last three years where sales exceeded 10,000 units in the USA.
SELECT 
    YEAR(sale_date) AS sale_year,
    MONTH(sale_date) AS sale_month,
    SUM(s.quantity) AS total_quantity
FROM sales AS s
JOIN stores AS st 
ON s.store_id = st.store_id
WHERE st.country = 'United States'
        AND s.sale_date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
GROUP BY 1 , 2
HAVING SUM(s.quantity) > 10000
ORDER BY 1 , 2;


-- 15. Identify the product category with the most warranty claims filed in the last two years.
SELECT c.category_name, COUNT(w.claim_id) AS total_claim
FROM warranty AS w
LEFT JOIN sales AS s 
ON w.sale_id = s.sale_id
JOIN products AS p 
ON s.product_id = p.product_id
JOIN category AS c 
ON p.category_id = c.category_id
WHERE w.claim_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
GROUP BY 1
ORDER BY 2 DESC;


-- 16. Determine the percentage chance of receiving warranty claims after each purchase for each country.
SELECT country, ROUND( total_claims/total_sales * 100, 2) AS claim_percentage
FROM (SELECT st.country,
            SUM(s.quantity) AS total_sales,
            COUNT(w.claim_id) AS total_claims
    FROM sales AS s
    JOIN stores AS st ON s.store_id = st.store_id
    LEFT JOIN warranty AS w ON s.sale_id = w.sale_id
    GROUP BY 1) AS t1
ORDER BY claim_percentage DESC;


-- 17. Analyze the year-by-year growth ratio for each store.
with 
yearly_sales AS(
	select st.store_name, year(s.sale_date) as sale_year, sum(p.price) as total_sale
	from sales as s
	join stores as st
	on s.store_id = st.store_id
	join products as p
	on s.product_id = p.product_id
	group by 1,2
	order by 1,2
),
growth_ratio AS(
	select store_name, sale_year, total_sale as current_year_sale, 
		lag(total_sale,1) over(partition by store_name order by sale_year) as last_year_sale
	from yearly_sales
)
select store_name, sale_year, current_year_sale, last_year_sale, 
	round((current_year_sale-last_year_sale)/current_year_sale * 100,3) as growth_ratio
from growth_ratio
where last_year_sale is not null;


-- 18. Calculate the correlation between product price and warranty claims for products sold in the last five years, segmented by price range.
SELECT 
    CASE
        WHEN p.price < 500 THEN 'Low range product'
        WHEN p.price BETWEEN 500 AND 1000 THEN 'Mid range product'
        ELSE 'High range product'
    END AS price_segment,
    COUNT(w.claim_id) AS total_claims
FROM warranty AS w
LEFT JOIN sales AS s 
ON w.sale_id = s.sale_id
JOIN products AS p 
ON s.product_id = p.product_id
GROUP BY 1;


-- 19. Identify the store with the highest percentage of "Completed" claims relative to total claims filed.
with t1 as(
	select s.store_id, count(claim_id) as total_claims
	from warranty as w
	left join sales as s
	on w.sale_id=s.sale_id
    group by s.store_id
),
t2 as(
	select s.store_id, count(claim_id) as completed_claims
	from warranty as w
	left join sales as s
	on w.sale_id = s.sale_id
	where w.repair_status="Completed"
    group by s.store_id
)
select t1.store_id, st.store_name, t1.total_claims, t2.completed_claims,
	round(t2.completed_claims/t1.total_claims*100,2) as completed_percentage
from t1 as t1
join t2 as t2
on t1.store_id = t2.store_id
join stores as st
on t1.store_id = st.store_id;


-- 20. Write a query to calculate the monthly running total of sales for each store over the past four years and compare trends during this period.
with monthly_sales 
as(
	select
		s.store_id,
		extract(year from sale_date) as sale_year,
		extract(month from sale_date) as sale_month,
		sum(s.quantity*p.price) as total_sales
	from sales as s
	join products as p
	on s.product_id=p.product_id
	group by 1,2,3
	order by 1,2,3
)
select store_id, sale_year, sale_month, total_sales, 
	sum(total_sales) over(partition by store_id order by sale_year,sale_month) as running_total
from monthly_sales;


-- 21. Analyze product sales trends over time, segmented into key periods: from launch to 6 months, 6-12 months, 12-18 months, and beyond 18 months.
SELECT 
    p.product_name,
    CASE
        WHEN s.sale_date BETWEEN p.launch_date AND p.launch_date + INTERVAL 6 MONTH THEN '6 Months'
        WHEN s.sale_date BETWEEN p.launch_date + INTERVAL 6 MONTH AND p.launch_date + INTERVAL 12 MONTH THEN '6-12 Months'
        WHEN s.sale_date BETWEEN p.launch_date + INTERVAL 12 MONTH AND p.launch_date + INTERVAL 18 MONTH THEN '12-18 Months'
        ELSE '18+ months'
    END AS month_segment,
    SUM(s.quantity) AS total_units
FROM sales AS s
JOIN products AS p 
ON s.product_id = p.product_id
GROUP BY 1 , 2
ORDER BY 1 , 2;




