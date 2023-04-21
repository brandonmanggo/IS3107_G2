# IS3107_G2

<div id="top"></div>

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<!-- PROJECT LOGO -->
<br />

<h3 align="center">Novus Hotel</h3>

  <p align="center">
    Lisbon, Portugal
    <br />
  </p>
</div>

<!-- ABOUT THE PROJECT -->

## About The Project

<p align="right">(<a href="#top">back to top</a>)</p>
<p> Leveraging on the benefits of Kappa Architecture, Kafka is used to stream real-time data into Google BigQuery in addition to the past batch data. Using the batch and streaming data, machine learning models are trained and will be used to predict both cancellation and price of future bookings streaming in from real time and retraining the model every quarter of the year to keep it up to date respectively. The results from modelling and EDA performed will be built into a dashboard to allow Novus Hotel to gain a more comprehensive understanding of customer demographic and behaviour within the region.
</p>
<br />

### Built With

- [Google BigQuery]
- [Apache Airflow Version 2.5.1]
- [Apache Kafka Version 3.4.0]
- [Tableau]

<p align="right">(<a href="#top">back to top</a>)</p>

<br />

## Instructions to Run

### Google BigQuery

#### Set the OS Environment and Credentials.

<br />

### Apache Airflow

Put the DAGS into Airflow DAGS Folder
- `batch_etl_dags.py`
- `streaming_etl_dag.py`
- `quarterly_dag.py`

Run this command in the virtual environment 

#### Start the airflow webserver.
`airflow standalone`

<br />

### Apache Kafka

Run the following in `kafka_2.13-3.4.0` directory.

#### Start the Zookeeper Service.
`bin/zookeeper-server-start.sh config/zookeeper.properties`.

#### Open another terminal and start the Broker Service.
`bin/kafka-server-start.sh config/server.properties`

#### Create a topic to store events.
`bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`

<br />

### Python Scripts

Run the following commands in the "main" directory 

#### Open a terminal and run Consumer.
`python3 consumer.py`.

##### Open another terminal and run main.
`python main.py`.
