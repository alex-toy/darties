# Darties - Data Engineering project 6 -  Data Warehouse with AWS Redshift

By Alessio Rea

==============================

You need to have Python 3.8.5 installed for this project

# General explanation 

## 1. Purpose of the project

The purpose of the project is to build an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms data into a set of dimensional tables for analysis and insights in what songs users are listening to. Data is modelled according to a star schema with fact and dimension tables for fast and easy analysis. Redshift gives the opportunity to execute SQL statements that create the analytics tables from these staging tables.



## 2. Database schema design and ETL pipeline

In this project, initial dataset comes from two json files :

-  Song Dataset
    
    Here is a filepaths to one file that could be found in such a dataset :

    ```
    sales_data/2020/sales.json
    ```

    Here is an example of what a single sale object may looks like :

    ```
    {"City":"Alencon","Brand":"Darty","Ads":116.2,"Region":"Nord_Ouest","Location":"Centre_Ville","Nb_cash_register":15,"Population":1394451,"Blue_collar_rate":14.7,"White_collar_rate":2.7,"Jobless_rate":39.4,"Lt_25_yo":35.5,"25_35_yo":14.6,"gt_35_yo":49.9}
    ```

    Those files contain the following features : 'City', 'Brand', 'Ads', 'Region', 'Location', 'Nb_cash_register', 'Population', 'Blue_collar_rate', 'White_collar_rate', 'Jobless_rate', 'Lt_25_yo', '25_35_yo', 'gt_35_yo'



Here is how the data is modelled according to a star schema :

- Fact table : to be done

- Dimension tables : 

    - dim1 - description to be done. Features : to be done
    - dim2 - description to be done. Features : to be done
    - dim3 - description to be done. Features : to be done
    - dim4 - description to be done. Features : to be done
    - dim5 - description to be done. Features : to be done



## 3. Example queries and results for song play analysis

Once the data has been ETLed, you are free to take full benefit from the power of star modelling and make business driven queries like :

    - How many sales have been perfomed in region 1 by salesman x?
    - How many sales have been performed between 2020 and 2021 in region 2 ?



# Project Organization 
----------------------

    ├── README.md
    ├── activate.sh
    ├── airflow
    │   ├── dags
    │   │   ├── __pycache__
    │   │   │   └── udac_example_dag.cpython-36.pyc
    │   │   ├── create_tables.sql
    │   │   └── global_dag.py
    │   └── plugins
    │       ├── __init__.py
    │       ├── __pycache__
    │       │   └── __init__.cpython-36.pyc
    │       ├── helpers
    │       │   ├── __init__.py
    │       │   ├── __pycache__
    │       │   │   ├── __init__.cpython-36.pyc
    │       │   │   └── sql_queries.cpython-36.pyc
    │       │   └── sql_queries.py
    │       └── operators
    │           ├── __init__.py
    │           ├── __pycache__
    │           │   ├── __init__.cpython-36.pyc
    │           │   ├── data_quality.cpython-36.pyc
    │           │   ├── load_dimension.cpython-36.pyc
    │           │   ├── load_fact.cpython-36.pyc
    │           │   └── stage_redshift.cpython-36.pyc
    │           ├── data_quality.py
    │           ├── load_dimension.py
    │           ├── load_fact.py
    │           └── stage_redshift.py
    ├── app
    │   ├── __init__.py
    │   ├── __pycache__
    │   │   ├── __init__.cpython-37.pyc
    │   │   └── __init__.cpython-38.pyc
    │   ├── application
    │   │   ├── create_output_file.py
    │   │   └── upload_file.py
    │   ├── config
    │   │   ├── __init__.py
    │   │   ├── __pycache__
    │   │   │   ├── __init__.cpython-37.pyc
    │   │   │   ├── __init__.cpython-38.pyc
    │   │   │   ├── config.cpython-37.pyc
    │   │   │   └── config.cpython-38.pyc
    │   │   └── config.py
    │   ├── domain
    │   │   ├── BusinessData.py
    │   │   └── __pycache__
    │   │       ├── BusinessData.cpython-37.pyc
    │   │       └── BusinessData.cpython-38.pyc
    │   └── infrastructure
    │       ├── SalesData.py
    │       ├── __init__.py
    │       └── __pycache__
    │           ├── SalesData.cpython-37.pyc
    │           ├── SalesData.cpython-38.pyc
    │           ├── __init__.cpython-37.pyc
    │           └── __init__.cpython-38.pyc
    ├── data
    │   ├── 2020_HISTO.xlsx
    │   ├── 2021_BUDGET.xlsx
    │   ├── Janvier_2021.xlsx
    │   └── README.md
    ├── generals
    │   └── Pre?\201sentation\ Projet\ Darties.pdf
    ├── init
    ├── init.sh
    ├── output
    │   ├── 2020
    │   │   └── sales.json
    │   └── README.md
    ├── poetry.lock
    ├── pyproject.toml
    ├── utils
    │   ├── IaC_1.py
    │   ├── IaC_2.py
    │   ├── create_bucket.py
    │   ├── release_resources.py
    │   ├── settings.py
    │   └── upload_file.py
    └── utils.txt


# Getting started

## 1. Clone this repository

```
$ git clone <this_project>
$ cd <this_project>
```

## 2. Install requirements

I suggest you create a python virtual environment for this project : <https://docs.python.org/3/tutorial/venv.html>

I had a problem installing psycopg2. The following lines did the trick though :

```
- export LDFLAGS="-L/usr/local/opt/openssl/lib"
- export CPPFLAGS="-I/usr/local/opt/openssl/include"
```

```
$ pip install -r requirements.txt
```

--------


## 2. Configuration of project

You need to have an AWS account to run the complete analysis. You also need to create a user that has AmazonRedshiftFullAccess as well as AmazonS3ReadOnlyAccess policies. Make sure to keep its KEY and SECRET credentials in a safe place.

1. Copy the *dwh.cfg* into a safe place.
2. Fill in all fields except *LOG_DATA*, *LOG_JSONPATH*, *SONG_DATA* which are already filled and *DWH_ENDPOINT*, *DWH_ROLE_ARN* which will be automatically filled for you. 
3. In file *settings.py*, give the path to *dwh.cfg* to variable *config_file*.
4. Run *IaC_1.py* and wait untill you see the cluster available in your console.
4. Run *IaC_2.py*.
5. Run *create_tables.py* and check that all tables are created in the redshift query editor.
6. Run *etl_staging.py*, then *etl_tables.py*. In the query editor, run queries to ensure that tables *staging_events* and *staging_songs* and other fact and dimension tables are properly populated.
7. Fill free to write queries in *test.py* to analyse the data.
8. Once done, don't forget to *release_resources.py* !!!!


--------



