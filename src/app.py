import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import psycopg2 as ps

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = ps.connect(connection_string)
cur = engine.cursor()

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

with open("src/sql/create.sql", "r") as file:
   created = file.read()
cur.execute(created)

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

with open("src/sql/insert.sql", "r") as file:
    inserted = file.read()
cur.execute(inserted)

engine.commit()

# 4) Use pandas to print one of the tables as dataframes using read_sql function
publisher_df = pd.read_sql("SELECT * FROM authors", engine)
print(publisher_df)

engine.close()