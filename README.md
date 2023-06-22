# multinational-retail-data-centralisation

This project modelled a real life data analysis situation based around cleaning and and querying multiple databases to analyse the performance metrics for your theoretical employer. 

The first database to access was the user data in an AWS RDS database. After being given the database credentials, I created a YAML file containing these credentials which would later be used to extract the data. I would then create a function to read the YAML file and return a dictionary of the credentials, another function called init_db_engine to create an sqlalchemy database engine and finally a list_db_tables function in order to pinpoint the tables I needed.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/c0be81e4-3f85-4d1d-9c05-8e70451cef41)
I would then extract the database table containing the user data and return a pandas dataframe ready to be cleaned.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/e7697a44-6ac8-44f2-86ec-b925b0c60688)
Now the table is in the correct format, it is ready to be cleaned. This included cleaning various types of data and ensuring no false information was given. Examples of this would include checking names were alphabetical only, email addresses contained necessary characters such as "@", countries/country codes matched, UUID's were correct format, and dates were genuine and in the correct format. Once cleaned, the dataframe is returned and uploaded to my sales_data database using the following function.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/db1752a4-1f80-4097-8310-e7a58c3554d1)

The next data to extract was stored in a PDF document inside an AWS S3 bucket. The following function extracts the pages of the PDF and returns a Pandas dataframe.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/e02ec2a8-ca76-47c0-b91d-148f072515cb)
The data is then cleaned with examples such as ensuring expiry dates and payment dates are correct, card numbers are numerical only and card providers aren't invalid inputs. This is then uploaded to my sales_data database using the upload to db function.

The third dataset to extract was found using an API. The API used has two GET methods, one to return the number of stores the business has and the other to return a given store number. The list_number_of_stores function below extracts how many stores need to be extracted by the API by taking in an endpoint and header dictionary.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/5f0db4dd-7c9d-490e-81ee-57f1ffec584e)
I would then use another function, shown below, to extract all the stores and returns them combined into a pandas Dataframe.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/b0f5ef42-070b-4c53-907e-e56402712342)
The dataframe would then be cleaned, examples including mapping categorical data- continents and country codes, formatting the opening dates and ensuring the longitude\latitude were correct for each store.
It would then be uploaded using the upload_to_db method.
The fourth database is the product details found in CSV fomrmat in an S3 bucket on AWS. The following function uses boto3 to download and extract the information returning a pandas dataframe.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/e2ac082c-ca25-45c3-8fec-f7cdcdd82349)
This would then be cleaned, such as converting the product weights to the same format and then uploaded to the sales_data database.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/25af6fc4-3722-4ac4-a1ad-460f76f22054)
The penultimate database is the orders table also stored in an AWS RDS database. As per the users table, the orders table is found using the list_db+tables function. The same process is followed as the users table ready to be cleaned. Cleaning consisted of removing columns such as first name, last name and 1, leaving columns that match the other tables, ready to be the center of the star based schema I will eventually create.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/5ff367af-98a1-47a0-882e-d534e37c4592)
The final table was was the date events found in a JSON file stored on S3. 
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/e20bb0ae-dcc3-434e-907f-646a5bece612)
This was then cleaned, being dates, all the columns were ensured to be correct, such as months all being valid, days being under 31, times and date UUID's being the correct format.
After this it was then uploaded to the sales_data, giving me 7 tables extracted, cleaned and uploaded to postgresql ready for querying.

The second milestone of this project was to develop a star-based schema of my database. First, I had to cast the columns to the corrrect datatype, below is an example of the task.
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/bdba8b01-5d21-49ba-a5fb-f1175a759dba)
For some of the columns, further changes were required, such as merging the "lat" and "latitude" columns for the store details table and creating weight categories in the product's table for the delivery team.  
The first step in actually develoing the star based schema was assigning primary to the tables except for the orders_table. One column from each of the 6 tables one in the orders table, which was assigned the primary key.
Finally, foreign keys were assigned to the orders table to reference to the primary keys in the others, completing the star based schema. If all is correct, the keys should match perfectly and assign, if there are extra values in the orders table or vice versa, further cleaning or re-evaluating of the clean would need to be done.

The final milestone was querying the schema to gather metrics allowing the company to make more data-driven decisions and understand its sales. Examples of these queries and metrics include the speed of which the company is making sales and which month produced the most sales. Screenshots of these queries are shown below. 
![image](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/a38f449a-03ae-42b4-a1bd-e443c1c36e81)
![2023-06-22 (20)](https://github.com/OllieMountier/multinational-retail-data-centralisation/assets/116648304/daa012ec-6c2e-4c99-912c-83c460551f31)



