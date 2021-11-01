import asyncio
import pandas as pd
import databases
import sqlalchemy
from sqlalchemy.sql.schema import ForeignKey


DATABASE_URL = "sqlite:///./gases.db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

countries_table = sqlalchemy.Table(
    "countries",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, nullable=False, autoincrement=True),
    sqlalchemy.Column("country", sqlalchemy.String),
)

all_data_table = sqlalchemy.Table(
    "all_data",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, nullable=False, autoincrement=True),
    sqlalchemy.Column("year", sqlalchemy.Integer),
    sqlalchemy.Column("value", sqlalchemy.Integer),
    sqlalchemy.Column("gas_symbol", sqlalchemy.String),
    sqlalchemy.Column("country_id", ForeignKey("countries.id")),
)

engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

async def setup_database():
    metadata.drop_all(engine)
    metadata.create_all(engine)
    await database.connect()


gas_symbols = set()
known_gas_symbols = ['co2', 'ghgs', 'hfcs', 'ch4', 'nf3', 'n2o', 'pfcs', 'sf6']

gas_symbols.update(known_gas_symbols)

def extract_gas_symbol(text):
    """
    Extracts gas symbol by comparing tokens in emissions with a set of know gas symbols
    Logic can be updated to use the last token before _emissions as gas_symbol if known list is not available
    """
    gas = text.split("_emissions")[0]
    gas = gas.split("_")
    gas = [item for item in gas if item in gas_symbols]
    # Due to limitation of sqlite, setting the field as a string type
    # postgresql has array(primitive_type) which could have been used
    return ",".join(gas)

async def insert_data(table, data):
    query = table.insert().values(data.to_dict(orient='records'))
    await database.execute(query)

if __name__ == "__main__":
    asyncio.run(setup_database())
    df = pd.read_csv('archive.zip')
    df['gas_symbol'] = df['category'].apply(extract_gas_symbol)
    countries = df['country_or_area'].unique()
    countries = pd.DataFrame(countries, columns=['country'])
    df.reset_index(drop=True)
    asyncio.run(insert_data(countries_table, countries))

    countries = engine.execute("SELECT * FROM countries").fetchall()

    country_ids = dict()
    for item in countries:
        country_ids[item[1]] = item[0]
    df['country_id'] = df['country_or_area'].apply(lambda x: country_ids[x])
    df.drop(['country_or_area', 'category'], axis=1, inplace=True)
    df.reset_index(drop=True)
    asyncio.run(insert_data(all_data_table, df))

    # all_data = engine.execute("SELECT * FROM all_data WHERE country_id = 1").fetchall()
    # print(all_data)
