-- 1. Create MySQL database

CREATE SCHEMA IF NOT EXISTS eCommerce_DB DEFAULT CHARACTER SET utf8mb4;
USE eCommerce_DB;

-- 2. Create tables without any dependence first
-- Country Table
CREATE TABLE Country (
    Country_id INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL
);

-- Category Table
CREATE TABLE Category (
    Category_id INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL
);

-- Payment Table
CREATE TABLE Payment (
    Payment_id INT PRIMARY KEY AUTO_INCREMENT,
    Payment_method ENUM('Credit_card', 'Debt_card', 'PayPal') NOT NULL,
    Payment_status ENUM('Paid', 'Unpaid', 'Refunded') NOT NULL
);

-- User Table
CREATE TABLE User (
    User_id INT PRIMARY KEY AUTO_INCREMENT,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    Phone BIGINT,
    Email VARCHAR(100) UNIQUE NOT NULL
);

-- 3. Create dependent tables
-- Returns Table
CREATE TABLE Returns ( 
    Return_id INT PRIMARY KEY AUTO_INCREMENT,
    Return_status ENUM('returned', 'in_process') NOT NULL,
    Refund_amount DECIMAL(12,2) NOT NULL,
    Reason TEXT,
    Expected_refund_date DATE,
    Refund_date DATE NULL,
    Payment_id INT NOT NULL,
    FOREIGN KEY (Payment_id) REFERENCES Payment(Payment_id)
);

-- City Table
CREATE TABLE City (
    City_id INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL,
    Country_id INT NOT NULL,
    FOREIGN KEY (Country_id) REFERENCES Country(Country_id)
);

-- Address Table
CREATE TABLE Address (
    Address_id INT PRIMARY KEY AUTO_INCREMENT,
    Address VARCHAR(250) NOT NULL,
    Address2 VARCHAR(250),
    District VARCHAR(100),
    Postal_code INT NOT NULL,
    City_id INT NOT NULL,
    FOREIGN KEY (City_id) REFERENCES City(City_id)
);

-- Product Table
CREATE TABLE Product (
    Product_id INT PRIMARY KEY AUTO_INCREMENT,
    Product_name VARCHAR(100) NOT NULL,
    Unit_price DECIMAL(12,2) NOT NULL,
    Category_id INT NOT NULL,
    FOREIGN KEY (Category_id) REFERENCES Category(Category_id)
);

-- Inventory Table
CREATE TABLE Inventory (
    Inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    Product_id INT NOT NULL UNIQUE, 
    Quantity INT NOT NULL,
    FOREIGN KEY (Product_id) REFERENCES Product(Product_id)
);

-- Shipping Table
CREATE TABLE Shipping (
    Shipping_id INT PRIMARY KEY AUTO_INCREMENT UNIQUE,
    Address_id INT NOT NULL,
    Shipping_method ENUM('Express', 'Ground') NOT NULL,
    Shipping_cost DECIMAL(12,2) NOT NULL,
    Shipping_date DATE NULL,
    Estimated_day_of_arrival DATE,
    Day_of_arrival DATE NULL,
    FOREIGN KEY (Address_id) REFERENCES Address(Address_id)
);

-- Orders Table
CREATE TABLE Orders (
    Order_id INT PRIMARY KEY AUTO_INCREMENT,
    Order_status ENUM('Placed', 'Shipped', 'Deliverd', 'Refunded') NOT NULL,
    Order_price DECIMAL(12,2) NOT NULL,
    Order_date DATE NOT NULL,
    User_id INT NOT NULL,
    Shipping_id INT NULL,
    Payment_id INT NOT NULL,
    Return_id INT NULL, 
    FOREIGN KEY (User_id) REFERENCES User(User_id),
    FOREIGN KEY (Shipping_id) REFERENCES Shipping(Shipping_id),
    FOREIGN KEY (Payment_id) REFERENCES Payment(Payment_id),
    FOREIGN KEY (Return_id) REFERENCES Returns(Return_id)
);

CREATE TABLE Shopping_cart (
    Cart_id INT PRIMARY KEY AUTO_INCREMENT UNIQUE,
    User_id INT NOT NULL,
    Order_id INT NULL,
    Order_placed BOOLEAN NOT NULL DEFAULT FALSE,
    Created_by DATE NOT NULL,
    FOREIGN KEY (User_id) REFERENCES User(User_id),
    FOREIGN KEY (Order_id) REFERENCES Orders(Order_id)
);


-- 4. Create Join Tables (Many-to-Many Relationships)

-- User_address Join Table
CREATE TABLE User_address (
    User_id INT NOT NULL,
    Address_id INT NOT NULL,
    PRIMARY KEY (User_id, Address_id), 
    FOREIGN KEY (User_id) REFERENCES User(User_id),
    FOREIGN KEY (Address_id) REFERENCES Address(Address_id)
);

-- Order_Product Join Table
CREATE TABLE Order_Product (
    Order_product_id INT PRIMARY KEY AUTO_INCREMENT UNIQUE,
    Order_id INT NOT NULL,
    Product_id INT NOT NULL,
    Quantity INT NOT NULL,
    Unit_price DECIMAL(12,2) NOT NULL,
    Product_name VARCHAR(259) NOT NULL,
    UNIQUE KEY (Order_id, Product_id),    
    FOREIGN KEY (Order_id) REFERENCES Orders(Order_id),
    FOREIGN KEY (Product_id) REFERENCES Product(Product_id)
);