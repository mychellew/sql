/* ASSIGNMENT 2 */
/* SECTION 2 */

-- COALESCE
/* 1. Our favourite manager wants a detailed long list of products, but is afraid of tables! 
We tell them, no problem! We can produce a list with all of the appropriate details. 

Using the following syntax you create our super cool and not at all needy manager a list:

SELECT 
product_name || ', ' || product_size|| ' (' || product_qty_type || ')'
FROM product

But wait! The product table has some bad data (a few NULL values). 
Find the NULLs and then using COALESCE, replace the NULL with a 
blank for the first problem, and 'unit' for the second problem. 

HINT: keep the syntax the same, but edited the correct components with the string. 
The `||` values concatenate the columns into strings. 
Edit the appropriate columns -- you're making two edits -- and the NULL rows will be fixed. 
All the other rows will remain the same.) */
/* Select for where there are NULL values in the data */

SELECT
product_id,
product_name || ', ' || 
COALESCE(IFNULL(product_size, ''),'')|| 
' (' || IFNULL(product_qty_type, 'unit') || ')' 
AS product_summary,
product_category_id
FROM product;

--Windowed Functions
/* 1. Write a query that selects from the customer_purchases table and numbers each customer’s  
visits to the farmer’s market (labeling each market date with a different number). 
Each customer’s first visit is labeled 1, second visit is labeled 2, etc. 

You can either display all rows in the customer_purchases table, with the counter changing on
each new market date for each customer, or select only the unique market dates per customer 
(without purchase details) and number those visits. 
HINT: One of these approaches uses ROW_NUMBER() and one uses DENSE_RANK(). */
/* Approach using ROW_NUMBER()*/

SELECT
customer_id,
market_date
,ROW_NUMBER()
OVER(PARTITION BY customer_id
ORDER BY market_date) AS num_customer_visits
FROM customer_purchases
GROUP BY customer_id, market_date
ORDER BY customer_id;

/* 2. Reverse the numbering of the query from a part so each customer’s most recent visit is labeled 1, 
then write another query that uses this one as a subquery (or temp table) and filters the results to 
only the customer’s most recent visit. */

SELECT *
FROM(SELECT
		customer_id,
		market_date
		,ROW_NUMBER()
		OVER(PARTITION BY customer_id
		ORDER BY market_date DESC) AS num_customer_visits
	FROM customer_purchases
	GROUP BY customer_id, market_date
	ORDER BY market_date DESC)
WHERE num_customer_visits = 1
ORDER BY market_date;

/* 3. Using a COUNT() window function, include a value along with each row of the 
customer_purchases table that indicates how many different times that customer has purchased that product_id. */

SELECT 
    customer_id,
    product_id,
    purchase_frequency
FROM (
    SELECT 
        customer_id,
        product_id,
        COUNT(product_id) OVER (
            PARTITION BY customer_id, product_id
        ) AS purchase_frequency,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, product_id
            ORDER BY customer_id
        ) AS times_purchased
    FROM customer_purchases
) AS frequency
WHERE times_purchased = 1
ORDER BY customer_id;


-- String manipulations
/* 1. Some product names in the product table have descriptions like "Jar" or "Organic". 
These are separated from the product name with a hyphen. 
Create a column using SUBSTR (and a couple of other commands) that captures these, but is otherwise NULL. 
Remove any trailing or leading whitespaces. Don't just use a case statement for each product! 

| product_name               | description |
|----------------------------|-------------|
| Habanero Peppers - Organic | Organic     |

Hint: you might need to use INSTR(product_name,'-') to find the hyphens. INSTR will help split the column. */

SELECT 
product_id,
product_name, 
product_category_id, 
product_size,
RTRIM(LTRIM(SUBSTR(
	CASE WHEN product_name LIKE '%-%'
		THEN product_name
		ELSE NULL
		END
	,INSTR(product_name, '-')+1))) AS description
FROM product;


/* 2. Filter the query to show any product_size value that contain a number with REGEXP. */

SELECT 
product_id,
product_name, 
product_size,
RTRIM(LTRIM(SUBSTR(
	CASE WHEN product_name LIKE '%-%'
		THEN product_name
		ELSE NULL
		END
	,INSTR(product_name, '-')+1))) AS description
FROM product
WHERE product_size REGEXP '^[0-9]';


-- UNION
/* 1. Using a UNION, write a query that displays the market dates with the highest and lowest total sales.

HINT: There are a possibly a few ways to do this query, but if you're struggling, try the following: 
1) Create a CTE/Temp Table to find sales values grouped dates; 
2) Create another CTE/Temp table with a rank windowed function on the previous query to create 
"best day" and "worst day"; 
3) Query the second temp table twice, once for the best day, once for the worst day, 
with a UNION binding them. */

-- Determine the Sales for the Day
DROP TABLE IF EXISTS sales_grouped_dates;

CREATE TEMP TABLE sales_grouped_dates AS

SELECT
market_date,
sales
FROM(SELECT *
	,SUM(cost_to_customer_per_qty * quantity) OVER(
		PARTITION BY market_date
		ORDER BY market_date)
		AS sales
	,ROW_NUMBER() OVER(
		PARTITION BY market_date
		ORDER BY market_date)
		AS ranked_row
	FROM customer_purchases)
WHERE ranked_row = 1
ORDER BY market_date;

-- Table for Best and Worst Day
SELECT
market_date,
sales
FROM(SELECT *
, RANK() OVER(
	ORDER BY market_date ASC) AS rank_day
FROM sales_grouped_dates)
WHERE rank_day =1

UNION

SELECT
market_date,
sales
FROM(SELECT *
, RANK() OVER(
	ORDER BY market_date DESC) AS rank_day
FROM sales_grouped_dates)
WHERE rank_day = 1;

-- Alternative of finding Best/Worst days without UNION
-- still requires sales_grouped_dates 
/* 
SELECT
market_date,
sales
FROM(SELECT *
, RANK() OVER(
	ORDER BY market_date ASC) AS rank_day
FROM sales_grouped_dates)
WHERE sales IN ((SELECT MAX(sales) FROM sales_grouped_dates),
	(SELECT MIN(sales) FROM sales_grouped_dates))
*/

/* SECTION 3 */

-- Cross Join
/*1. Suppose every vendor in the `vendor_inventory` table had 5 of each of their products to sell to **every** 
customer on record. How much money would each vendor make per product? 
Show this by vendor_name and product name, rather than using the IDs.

HINT: Be sure you select only relevant columns and rows. 
Remember, CROSS JOIN will explode your table rows, so CROSS JOIN should likely be a subquery. 
Think a bit about the row counts: how many distinct vendors, product names are there (x)?
How many customers are there (y). 
Before your final group by you should have the product of those two queries (x*y).  */

SELECT
    product_id,
    product_name,
    vendor_name,
    original_price,
	product_per_customer,
	original_price * product_per_customer * 5 AS sales_5_per_customer
FROM (
    SELECT
        p.product_id,
        p.product_name,
        v.vendor_name,
        vi.original_price,
		-- COUNT the number of instances that a product shows up, this will represent how many customers need a product
        COUNT(p.product_name) OVER (PARTITION BY p.product_name) AS product_per_customer,
        ROW_NUMBER() OVER (PARTITION BY p.product_name ORDER BY p.product_id) AS row_num
    FROM product AS p
    INNER JOIN (
        SELECT
            vendor_id,
            product_id,
            original_price
        FROM (
            SELECT 
                vendor_id,
                product_id,
                original_price,
                ROW_NUMBER() OVER (
                    PARTITION BY product_id, vendor_id
                    ORDER BY product_id
                ) AS row_count
            FROM vendor_inventory
        ) AS subquery
        WHERE row_count = 1
    ) AS vi
    ON p.product_id = vi.product_id
    INNER JOIN vendor AS v
    ON vi.vendor_id = v.vendor_id
    CROSS JOIN customer
) AS deduplicate
WHERE row_num = 1;

-- INSERT
/*1.  Create a new table "product_units". 
This table will contain only products where the `product_qty_type = 'unit'`. 
It should use all of the columns from the product table, as well as a new column for the `CURRENT_TIMESTAMP`.  
Name the timestamp column `snapshot_timestamp`. */

DROP TABLE IF EXISTS product_units;

CREATE TEMP TABLE product_units AS

SELECT *
FROM product
WHERE product_qty_type = 'unit';

/*2. Using `INSERT`, add a new row to the product_units table (with an updated timestamp). 
This can be any product you desire (e.g. add another record for Apple Pie). */

ALTER TABLE product_units
ADD snapshot_timestamp date;

INSERT INTO product_units
VALUES(24, 'Apple Pie', '10"', 3, 'unit',CURRENT_TIMESTAMP);

-- DELETE
/* 1. Delete the older record for the whatever product you added. 

HINT: If you don't specify a WHERE clause, you are going to have a bad time.*/
DELETE FROM product_units
WHERE product_id = 24 AND product_name = 'Apple Pie';


-- UPDATE
/* 1.We want to add the current_quantity to the product_units table. 
First, add a new column, current_quantity to the table using the following syntax.

ALTER TABLE product_units
ADD current_quantity INT;

Then, using UPDATE, change the current_quantity equal to the last quantity value from the vendor_inventory details.

HINT: This one is pretty hard. 
First, determine how to get the "last" quantity per product. 
Second, coalesce null values to 0 (if you don't have null values, figure out how to rearrange your query so you do.) 
Third, SET current_quantity = (...your select statement...), remembering that WHERE can only accommodate one column. 
Finally, make sure you have a WHERE statement to update the right row, 
	you'll need to use product_units.product_id to refer to the correct row within the product_units table. 
When you have all of these components, you can run the update statement. */

ALTER TABLE product_units
ADD current_quantity INT;

UPDATE product_units AS pu
SET current_quantity = COALESCE(
    (SELECT vi.quantity
		FROM (
        -- Subquery to assign row numbers and identify the latest inventory
           SELECT 
               product_id,
               quantity,
               market_date,
               ROW_NUMBER() OVER (
                  PARTITION BY product_id
                  ORDER BY market_date DESC) AS date_rank
            FROM vendor_inventory) AS vi
        WHERE vi.product_id = pu.product_id
          AND vi.date_rank = 1), 0);