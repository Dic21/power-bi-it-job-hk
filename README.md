# Power BI Dashboard - IT Job Market Statistics in Hong Kong 
This Power BI project involves collecting job advertisements from two major job-seeking websites in Hong Kong to build a comprehensive dashboard for trend and job category analysis.

## Screenshot
![image](https://github.com/Dic21/power-bi-it-job-hk/assets/108064133/f2688b17-cd98-4ba7-851e-501b458129c8)
![image](https://github.com/Dic21/power-bi-it-job-hk/assets/108064133/05d9e38c-bcbf-4e08-a815-63b75946570b)


## Project Overview
The main objectives of this project are:
1. Collect and aggregate data on IT job listings in Hong Kong from various online sources.
2. Perform data cleaning, transformation, and enrichment to prepare the data for analysis.
3. Develop an interactive Power BI dashboard to explore trends and patterns in the IT job market during the period.


## Data Sources
The data for this project was collected using a Python web scraping script. In this project, web scraping and parsing using BeautifulSoup was demonstrated instead of calling API.
The following data sources were used:
1. ctgoodjobs
2. jobsdb

The data is stored in CSV files, meanwhile, is backed up to a MongoDB database using insertToMongo function.

## Data Processing and Visualization
The data processing workflow includes the following steps:<br><br>
Data Extraction: The data from the CSV files was loaded into Power BI. <br>
Data Transformation: The extracted data was cleaned, transformed, and enriched using Power Query in Power BI.<br>
Data Modeling: The transformed data was loaded into a Power BI data model, and further processing mapping for job categories.<br>
Dashboard: Using DAX to generate new measures for data visualization. Job Trend Analysis and Job Category breakdown was demonstrated. 

