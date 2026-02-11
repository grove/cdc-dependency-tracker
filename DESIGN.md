# Dependency tracker tool for CDC streams

This is a command line tool that takes the following input:

- a base table and a set of join criterias between it and dependent tables.
- can listen to a set of CDC streams, e.g. PostgreSQL logical replication.
- given the CDC events received it should look at the join criterias and figure out what dependent data in the other tables require reprocessing.

# Design discussion

We only care about customers that need reprocessing, and they need reprocessing if any dependent order line or product is inserted, updated or deleted. Join keys can be mutable so that must be taken into account (example: order lines can change their product ids).


# Practical example

## Example table definitions

-- Create customers table
CREATE TABLE customers (
    _id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    credit_limit INTEGER NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE
);

-- Create orders table
CREATE TABLE orders (
    _id VARCHAR PRIMARY KEY,
    cust_id VARCHAR NOT NULL,
    order_date DATE NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (cust_id) REFERENCES customers(_id)
);

-- Create order_lines table
CREATE TABLE order_lines (
    _id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (order_id) REFERENCES orders(_id),
    FOREIGN KEY (product_id) REFERENCES products(_id)
);

-- Create products table
CREATE TABLE products (
    _id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    price DECIMAL(10,2) NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE
);

## Example SQL query 

SELECT 
    c._id as customer_id,
    c.name as customer_name,
    c.credit_limit,
    o._id as order_id,
    o.order_date,
    ol._id as order_line_id,
    p._id as product_id,
    p.name as product_name,
    p.description as product_description,
    ol.quantity,
    ol.unit_price,
    (ol.quantity * ol.unit_price) as line_total
FROM customers c
JOIN orders o ON c._id = o.cust_id
JOIN order_lines ol ON o._id = ol.order_id
JOIN products p ON ol.product_id = p._id
WHERE c._deleted = FALSE
  AND o._deleted = FALSE
  AND ol._deleted = FALSE
  AND p._deleted = FALSE
ORDER BY c._id, o._id, ol._id;

For this query the "customers" table is the base table and the one we want to figure out what rows in this table that need to be reprocessed if data in any of the other dependent tables change.

Task: extract the join criterias from this query.


## Control questions

- If a product name changes how do we find out which customers need to be reprocessed?
- If an order line's product id changes how do we find out which customers need to be reprocessed?
