# Project 1: Data Modeling with PostgreSQL

At Sparkify, we want to understand our customers in order to give them the best experience. This implies that we want to know about each customer's preferences -- when, where and how they listen to which music. 

### What to Run and How to Run it

- Terminal: `python create_tables.py` -- creates tables (and overwrites existing ones)
- Terminal: `python etl.py` -- parses and processes files in the `data` directory and inserts the data into the tables
- Jupyter: `test.ipynb` -- executes `create_tables.py` and `etl.py` before showing some of the data in each table


### Purpose of the Sparkify Database

In this database, we combine two sources of data: our music library as well as the user log data from our customers. 

With the user log data alone, we can get insights on 
- the preferred times and days of the week at which a user likes to turn on Sparkify, and for how long  
- the usage behavior of premium users vs. free users (quantitatively and qualitatively, and we can improve our offers)
- device-specific errors or anomalies of the Sparkify app, which helps with troubleshooting and debugging 
- whether Sparkify is equally popular among male and female users

In combination with our music library, we can find out
- whether users like local music (i.e., they often listen to nearby artists)
- which artists might be interesting for two users with similar tastes in music
- how broad users' tastes in music are

### The Design of the Database and the ETL Pipeline

The database is designed in a STAR schema. The central table, `songplays`, provides foreign keys to the tables `songs`, `artists`, `users`, and `time`. 
In its standard form, the database is in 2NF, which means that while some queries will need to employ JOINs, these can be achieved in a simple way without composite keys. At the same time, each entity (users, songs, artists, songplays, timestamps) are identifiable with a unique value. The table `time` holds values extracted from the `timestamp` attribute in the user log data. Storing these information in 'extracted' format will reduce the number of computations whenever data is selected by a date or a time.

The ETL pipeline processes all available song data first (tables `songs` and `artists`). Afterwards, the user log data is processed (tables `users`, `time` and `songplays`). This order is important, because `songplays` contains the foreign keys `song_id` and `artist_id` which refer to `songs` and `artists`, respectively. Therefore, these two attributes need to be available at the time that data is inserted into `songplays`. 

The `timestamp` data is logged as an integer of milliseconds, which means that it needs to be divided by 1000 in order to properly convert to datetime format for further internal processing. Nonetheless, the timestamp itself is stored as integer as to avoid confusion.


