CREATE TABLE products(
   id SERIAL PRIMARY KEY,
   name VARCHAR(50),
   description varchar(150),
   price VARCHAR(50),
   creation_date VARCHAR(50)
);

INSERT INTO products (name, description, price, creation_date)
VALUES ('Samsung Galaxy M52', '6.7 inches, Qualcomm SM7325 Snapdragon 778G 5G', '13499', '2022-02-24 04:00:00'),
       ('Xiaomi Redmi Note 9 Pro', '6.67 inches, Qualcomm SM7125 Snapdragon 720G Octa-core', '11699', '2022-02-24 04:00:00'),
       ('Apple iPhone 14', '6.1 inches, Apple A15 Bionic', '41499', '2022-02-24 04:00:00');