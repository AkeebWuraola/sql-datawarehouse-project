# sql-datawarehouse
<p>Build a Robust DataWarehouse</p>
<p>What is a data warehouse?</p>
A data warehouse is a subject-oriented(focused on a business area e.g sales, revenue),integrated(data ingestion from multiple sources),time_variant(data lifecycle) and non volatile collection of data in support of management's decision-making process.

## what is ETL?
ETL is the core element of the data warehouse. Extract-Transform-Load

Extract is identifying the data that is needed from the source system and not changing anything.
The extracted data is pulled and transformed via data manipulations and cleaning. The data is reformated and reshaped into the way we want it.
Loading is taking this transformed data and moving it into the target system.

## Data Architectiure
<li>Layer 1 - from source, loading the data as is into layer 1 without making changes</li>
<li>Layer 2 - perform transformation on data on layer 1 and move to Layer 2 completing the ETL Process </li>

## Extraction 
  ## Extraction Methods 
  <li>pull extraction is if the data ware house is pulling the data from the source</li>
  <li>push extraction is if the source is pushing the data into the data warehouse</li>
  
  ## Extracion Types
  <li>Full Extraction - this is a drop/truncate and load. Everything is dropped and loaded freshly</li>
  <li>Incremental Extraction - this is a insert/update. Here only the new data is loaded</li>
  
  ## Extraction Techniques
  <li>Manual Data Extraction</li>
  <li>Database Querying</li>
  <li>File Parsing</li>
  <li>API Calls</li>
  <li>Event Based Streaming</li>
  <li>CDC - change data capture</li>
  <li>Web Scraping</li>

## Transformation Techniques
  <li>Data Enrichment</li>
  <li>Data Integration</li>
  <li>Derived Columns</li>
  <li>Data Normalization and Standardization</li>
  <li>Business Rules & Logic</li>
  <li>Data Aggregations</li>
  <li>Data Cleansing: Remove Duplicates,Handling Missing Data, Handling Invalid Values,Data Filtering,Outlier Detection,Handling Unwanted Spaces, Data Type Casting</li>

## Load
 ## Processing Types
 <li>Batch Processing: Loading the data in one batch and job runs only once to refresh the content of the data warehouse</li>
 <li>Streaming Processing: changes are processes as soon as possible, almost real time </li>

 ## Load Methods
 <h5>This is similar to extraction methods</h5>
 <li>Full Load: Truncate & Insert; Upsert(Update & Insert); Drop,create & insert</li>
 <li>Incremental Load: Upsert; append(insert); merge(insert or delete)</li>

 ## Slowly Changing Dimensions
  <li>SCD 0: No historization meaning no history of the data is kept</li>
  <li>SCD 1: Overwrite, changes are to be overwritten, e.g if a customers status changes from dormant to active, status column should be overwritten</li>
  <li>SCD 2: Historization; if there is any changes, you want to keep the history of the change</li>
  <li>SCD..</li>

  <h1> Data Architecture</h1>
  <p>There are 4 major types</p>
  <p>Data Warehouse: Suitable if you have only structured data and business wants to build solid foundation for analysis and reporting</p>
  <p>Data Lake: This is more flexible, you can store both structured and semi structured data. If you have mixed types of data, like logs, db tables,images,video. It is not as orhanized like a data warehouse</p>
  <p>Data Lakehouse: Flexibility of having different types of data like data lake but can still structure your data like a data warehouse</p>
  <p>Data Mesh: In this case you have decentralised data management of system sort of like having multiple departments and multiple domains</p>

  ## How to build data warehouse
  <p>There are different approaches of building a data warehouse</p>
  <li><b>Immon:</b> There are 3 layers: Data is ingested into the stage; then into the EDW where transformation is carried out; then datamarts are created that connects to the BI Reports</li>
  <li><b>Kimball:</b> The EDW layers is removed and data is moved from the staging area to the creation of datamarts. This is a very fast approach however, there is risk of creating isolated transformation logic that should have been reusable</li>
  <li><b>Data Vault:</b> The EDW Layer is much more encompassing with the transformation being split into raw vault and business vault. Here the business logic and rules is taken into the account during the transformation stage before data marts are created.</li>
  <li><b>Medallion Architecture:</b> There are 3 layers; bronze, silver and gold. The bronze layer is the stage area where you have the data as is from the source system. The silver layer is where you do transformation and data cleansing but no business rules are applied here. The gold layer is similar to the data mart where you can build different types of data mart that caters for different purposes such as reporting, machine learning, AI</li>
  <img width="790" height="553" alt="image" src="https://github.com/user-attachments/assets/d20b48db-d946-469b-9ede-b1194d32d054" />


<p>Separation of Concerns involve ensuring each layer are divided into distinct parts each responsible for well defined responsibilities. For example, ingestion happens in the bronze layer, transformation and cleaning happens in the silver layer while business logic is applied in the gold layer. Ingestion should not be happening in the silver layer and vice versa</p>
