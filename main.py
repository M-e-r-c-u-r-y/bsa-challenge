from typing import List, Optional


import databases

import sqlalchemy
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.expression import func
from sqlalchemy import select
from fastapi import FastAPI
from fastapi_camelcase import CamelModel
from pydantic import BaseModel, Field

# SQLAlchemy specific code, as with any other app

DATABASE_URL = "sqlite:///./gases.db"

# DATABASE_URL = "postgresql://user:password@postgresserver/db"


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
metadata.create_all(engine)


class CountryData(CamelModel):
    id: int
    country: str
    year: int
    value: int
    gas_symbol: str

class Country(CamelModel):
    id: int
    country: str
    start_year: int
    end_year: int

app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/countries/", response_model=List[Country])
async def read_countries():
    """
    Get all countries info
    """

    j = countries_table.join(all_data_table, countries_table.c.id == all_data_table.c.country_id)

    query = select(countries_table.c.id, countries_table.c.country, \
            func.min(all_data_table.c.year).label('startYear'), func.max(all_data_table.c.year).label('endYear')) \
            .group_by(countries_table.c.id) \
            .select_from(j)
    # print(query)
    return await database.fetch_all(query)

@app.get("/country/{id}", response_model=List[CountryData])
async def read_countryData(id: int, startYear: int = 0, endYear: int = 3000, gas: str = ""):
    """
    Get specific country's info
    """
    multi = False
    case = None
    gases = gas.lower().split(" ")
    if len(gases) > 3:
        gas = "%"
    if len(gases) == 3:
        if gases[1] in ["and", "or"] and gases[0].isalnum() is True and gases[2].isalnum() is True:
            multi = True

    if multi is True:
        case = gases[1]
    else:
        if gas.isalnum() is False:
            gas = "%"

    j = all_data_table.join(countries_table, countries_table.c.id == all_data_table.c.country_id)

    if multi is False:
        query = select(countries_table.c.id, countries_table.c.country, all_data_table.c.year, all_data_table.c.value, all_data_table.c.gas_symbol) \
                .select_from(j).where(
                    (all_data_table.c.country_id == id) &
                    (all_data_table.c.year >= startYear) &
                    (all_data_table.c.year <= endYear) &
                    (all_data_table.c.gas_symbol.contains(gas))
                ).order_by(all_data_table.c.year)
    else:
        if case == 'and':
            query = select(countries_table.c.id, countries_table.c.country, all_data_table.c.year, all_data_table.c.value, all_data_table.c.gas_symbol) \
                    .select_from(j).where(
                        (all_data_table.c.country_id == id) &
                        (all_data_table.c.year >= startYear) &
                        (all_data_table.c.year <= endYear) &
                        (all_data_table.c.gas_symbol.contains(gases[0]) & all_data_table.c.gas_symbol.contains(gases[2]))
                    ).order_by(all_data_table.c.year)
        else:
            query = select(countries_table.c.id, countries_table.c.country, all_data_table.c.year, all_data_table.c.value, all_data_table.c.gas_symbol) \
                    .select_from(j).where(
                        (all_data_table.c.country_id == id) &
                        (all_data_table.c.year >= startYear) &
                        (all_data_table.c.year <= endYear) &
                        (all_data_table.c.gas_symbol.contains(gases[0]) | all_data_table.c.gas_symbol.contains(gases[2]))
                    ).order_by(all_data_table.c.year)
    print(query)
    return await database.fetch_all(query)
