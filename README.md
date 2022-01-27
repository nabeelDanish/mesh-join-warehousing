# Mesh Join Algorithm and Data Warehouse
A complete Mesh-Join Algorithm Implementation as provided in the paper [R-MESHJOIN](https://dl.acm.org/doi/10.1145/1871940.1871952) . This is demonstrated by the use of a single Data warehouse that is a simplified version of a WallMart-style Shopping and Retail Business. The Repository provides code for both the Warehouse and the MESHJOIN algorithm Code.
## Introduction
### Problem Statement
This project aims to create a centralized Data Warehouse for the analysis and querying of various business aspects of the store. The warehouse needs to operate in real-time, meaning that any changes in the ODS stores should almost immediately be reflected in the warehouse for querying. This is done by creating an ETL Service that can run on a server and can monitor the ODS to check for updates and insertions. The service then fetches and prepares data to be aggregated and then loaded into the warehouse. We implement such a service using the Java Programming Language in this project.
### Scope
The scope of the project is limited to only the transactional data of the ODS. The transactional data is provided in a small file that amounts to 10,000 transactions. The project also provides us with a “MasterData” Table, which contains information on the products catalogues, and their prices. The ETL Service, therefore, needs to perform a real-time join operation to calculate the total sale of each transaction, and store them in the warehouse. This is the primary goal of the project.
## Data Warehouse
### Schema
![image](https://drive.google.com/uc?export=view&id=1eAssb52VXnUypysieMUxdADMDm30SdI8)

The schema includes a single Fact table called TRANSCATIONS. The Schema design follows the Star Schema template, and is made on the basis of the ODS Schema provided below:

![image](https://drive.google.com/uc?export=view&id=1fVm_2kg1AW8Bl0cI-wNUvjAtrlXfEbEk)
## Usage
### File Structure
- Java Project
	- Contains the complete source code of the Eclipse and IntelliJ Java Project
	- The code is found in the `src/com/dwh` folder
	- The main function is found in the `meshJoin.java` file

- MySQL Project
	- Contains files for the MySQL Workbench project
	- This project file creates a model that is used throughout the project

- SQL Files
	- Contains all the files for SQL Functions
	- `createDW.sql`
		- Run this SQL File to implement the Data Warehouse
		- This file is executed and tested on MySQL Workbench

	- `queriesDW.sql`
		- Contains all the OLAP queries for the project

	- `mainSQL.sql`
		- Contains all the OLAP queries for the project
		- Also contains addtional queries used for testing and debugging
	- `Transaction_and_MasterData_Generator.sql`
		- Generates Data for the MESHJOIN algorithm to work
		- This is the ODS schema that is used to load the warehouse into MySQL Projects

### Code Compilation
1. Using the MySQL Workbench File provided, Open the Project
2. You can either use Forward Enigneering feature, or execute DWH Script on the connected database
3. Execute the `TransactionGenerator.sql` File provided
4. Execute the `createDW.sql` File provided to create the warehouse
5. Load the Eclipse/IDEA Project into the IDE
6. Compile and Build the program
7. If a JDBC error occurs, then add the provided `jdbc-connection.jar` file into the project build settings
8. When the program starts it's execution, it asks for use input on the number of transactions already loaded into the warehouse
	- Enter 0 if it is the first time running the script, and we need to load fresh data
	- Enter the number of transactions already loaded, if new data has been added into the tables
9. The Menu asks to perform ETL or quit
	- Press 1 for ETL, and the script executes showing the progress
	- Press 0 for Exiting
