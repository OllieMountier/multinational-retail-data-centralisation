# multinational-retail-data-centralisation

This project is my implementation of many data science and analysis techniques such as data cleaning and also my first use of postgreSQL.
I started off by creating 3 new files with different objectives that would later import into the middle file to be used in the if __name__==__main__ condition. The 'User' database was then accessed by creating a YAML file of the AWS credentials and using them to creat an SQLalchemy databse engine. The tables would then would then be accesssible and from there I would create a Pandas dataframe out of each one. For each table, I would thoroughly clean them by removing the NULL values, editing the dates and incorrectly typed values as well editing rows with wrong information. The user and card details would then be accessed through an AWS S3 bucket, where the data would be transformed from a PDF file to a dataframe and then cleaned. The store details would be cleaned by the use of an API. We were given header details and 2 endpoints-one would lead to a specified store and one would would lead to the number of stores. The stores were extracted and cleaned. This process was followed for the product details extracted from a CSV file in an S3 bucket, the orders table extracted from an AWS RDS and the events data from a JSON file.

After cleaning all the data, I would cast each created table to an SQL table and each column would be set to the right data type. This was all in preparation of the star based schema. The last task for this milestone was creating primary keys in each table that matched the keys in the "orders_table', and then the foreign keys in the 'orders_table', finalising the star-based database schema.

To test the database, I was tasked to retreive metrics from the data in order to demonstrate how a business would make more data-driven decisions and get a better sales understanding. These queries would include calculating what store type is selling the most and what locations have the highest number of stores.
