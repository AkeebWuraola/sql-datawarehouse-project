# SQL Data Warehouse & Analytics Project
<p>What is a data warehouse?</p>
<p>A data warehouse is a subject-oriented(focused on a business area e.g sales, revenue),integrated(data ingestion from multiple sources),time_variant(data lifecycle) and non volatile collection of data in support of management's decision-making process.</p>

<h1>Project Requirements</h1>

**Building a Robust Data Warehouse (Data Engineering)**

**Objective**
<p>Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.</p>

<b>Specifications</b>
<li><b>Data Sources:</b> Import data from two source systems (ERP and CRM) provided as CSV files.</li>
<li><b>Data Quality:</b> Cleanse and resolve data quality issues prior to analysis.</li>
<li><b>Integration:</b> Combine both sources into a single, user-friendly data model designed for analytical queries.</li>
<li><b>Scope:</b> Focus on the latest dataset only; historization of data is not required.</li>
<li><b>Documentation:</b> Provide clear documentation of the data model to support both business stakeholders and analytics teams.</li>

# Project Plan
A detailed Project Implementation Plan has been structured and developed in Notion.  
This is accessible via [Data Warehouse Project](https://www.notion.so/Data-Warehouse-Project-2714cb2905d580249a23c0fe5ae25374?source=copy_link).



# Data Architecture
<p>The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:</p>
<img width="892" height="652" alt="image" src="https://github.com/user-attachments/assets/63b4b84d-a02c-4b31-b0b9-64aab4d287f3" />

# Naming Convention & General Principles
It is a set of rules or guidelines for naming anything in the project. whether you are naming, database,schema,tables or stored procedure. some of the rules involve using only lowercase, using Propercase without underscore . This helps to ensure consistency.  

snake case with all lowercase and underscores to separate words will be used for this project  
English Language will be used  
SQL Reserved words will not be used as object names

**Bronze & Silver Rules**
- All names must start with source system name and table names must match their original names from the source i.e <sourcesystem>_(tablename) e.g crm_customersinfo
  
**Gold Rules**
- All names must be meaningful, business aligned names for tables starting with category prefix i.e <category>_<tablename> e.g dim_customers,fact_sales
- category describes the role of the table; if its a fact (fact) or dimension(dim) table

**Column Naming Convention**

**Surrogate Keys**
- Surrogate keys must use suffix _key: surrogate keys are system generated unique identifiers assigned to each record in a table
- All primary keys must use suffix _id.
  
**Technical Columns**
- Technical columns should start with dw_<column_name> e.g dw_load_date

**Stored Procedures**
- All stored procedures must start with stp_load_<layer>



# What does Success Look Like?

The data warehouse is successfully delivering a single, reliable source of truth that enables timely, accurate, and actionable insights for decision-making across the organization.


