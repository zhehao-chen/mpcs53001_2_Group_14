# mpcs53001_2_Group_14
Final Project 
1. mysql_db.sql:
   * purpose: used to create MySQL tables based on ERD / Data design
2. mysql_core.py:
   * purpose: use faker to simulate the data based on the time series order
   * function: create 1000 users, 5000 products and 100,000 orders in MySQL tables
3. mongodb_product_details.py
   * purpose: use faker to simulate the product details data based on category
   * function: create 5000 product details based on the product_id and its coresponding category
4. mongodb_user_behavior.py
   * purpose: use faker to simulate the various of user behaviors
   * function: create 500,000 user events/behaviors, each user generated 500 user behaviors
5. redis_user.py
   * purpose: use faker to simulate user behavior and its coresponding data change or update
   * function: create redis information based on user behavior and update relevant tables in Mysql,i.e., order, shopping cart, payment,etc
