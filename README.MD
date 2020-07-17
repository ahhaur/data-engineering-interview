# Instructions
Candidates shall use python and T-sql to implement the task.

**Fork a copy first into your github!**. Implement the task and document/ comment the codes in your own github repository to achive the expected results below.

Deposit your CV in the same personal github repository when you are completed with the Task.

# Task
Implement a structured **data flow orchestration** to process the raw data to generate a set of pipe-delimited load files and stored in the path (/dataextract/yy/MM/dd/factstore).

Then, load the processed data model into a database (name it FactStore) for querying the results to achieve the expected results (see sample DDL-DML.sql to create corresponding data structure in FactStore database).

### Pipe-delimited Data Schema
DateID: date (yyyy-MM-dd)
StoreID: Int
ProductID: Int
OnHandQty: Int
OnOrderQty: Int
DaysInStock: Int
MinDayInStock: Int
MaxDayInStock: Int

### Expected Result #1: Query average number of stocks on hand by category.
Audio: 21
Cameras and camcords: 20
Cell phones: 62
Computers: 21
Games and Toys: 75
Home Appliances: 20
Music, Movies and Audio Books:
TV and Video: 20

### Expected Result #2: Query average number of stocks on hand by store.
Catalog: 59
Online: 47
Reseller: 37
Store: 24
