# sql-datawarehouse
Build a Robust DataWarehouse
What is a data warehouse?
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

 ## Load Methods:
 <h5>This is similar to extraction methods</h5>
 <li>Full Load: Truncate & Insert; Upsert(Update & Insert); Drop,create & insert</li>
 <li>Incremental Load: Upsert; append(insert); merge(insert or delete)</li>

 ## Slowly Changing Dimensions
  <li>SCD 0: No historization meaning no history of the data is kept</li>
  <li>SCD 1: Overwrite, changes are to be overwritten, e.g if a customers status changes from dormant to active, status column should be overwritten</li>
  <li>SCD 2: Historization; if there is any changes, you want to keep the history of the change</li>
  <li>SCD..</li>
  
