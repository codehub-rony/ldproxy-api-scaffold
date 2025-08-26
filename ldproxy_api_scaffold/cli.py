from InquirerPy import inquirer
from sqlalchemy import create_engine, inspect

def get_db_connection_input(message):
    print("\n---------------------------------------------")
    print(message)
    print("---------------------------------------------\n")
    db = {}
    db["DB_HOST"] = inquirer.text(message="database host:", default="localhost").execute()
    db["DB_PORT"] = inquirer.text(message="Database port:", default="5432").execute()
    db["DATABASE"] = inquirer.text(message="Database name:").execute()
    db["DB_USER"] = inquirer.text(message="Database user:", default="postgres").execute()
    db["DB_PASSWORD"] = inquirer.secret(message="Enter your password:").execute()
    db["connection_as_str"] = f"postgresql://{db['DB_USER']}:{db['DB_PASSWORD']}@{db['DB_HOST']}:{db['DB_PORT']}/{db['DATABASE']}"

    return db

def connect_to_db(db_config):
    print(f"\nConnecting to postgres database {db_config['DATABASE']}")

    try:
        engine = create_engine(db_config["connection_as_str"])
        insp = inspect(engine)
        print("\nconnection succesfull\n")
        print("------------------------------------------")
        print("--------- Configure api services ---------")
        print("------------------------------------------")
        print('\n')

        return engine, insp

    except Exception as e:
        print("Error connecting to PostgreSQL:", str(e))
        db_config = get_db_connection_input("Connection to database failed, please review connection details.")


def create_table_config(tablenames, db_schema, insp):
    table_config = {"db_schema": db_schema, "tables": []}

    for tablename in tablenames:

        columns = insp.get_columns(tablename, schema=db_schema)
        table_config["tables"].append({"tablename": tablename, "columns":columns })

    return table_config

def main():

    db_connection_sucessful = False

    db_config = get_db_connection_input("Provide connection details for source database")

    print(f"\nConnecting to postgres database {db_config['DATABASE']}")

    while not db_connection_sucessful:

        try:
            engine = create_engine(db_config["connection_as_str"])
            insp = inspect(engine)
            print("\nconnection succesfull\n")
            print("------------------------------------------")
            print("--------- Configure api services ---------")
            print("------------------------------------------")
            print('\n')
            db_connection_sucessful = True

        except Exception as e:
            print("Error connecting to PostgreSQL:", str(e))
            db_config = get_db_connection_input("Connection to database failed, please review connection details.")


    service_id = inquirer.text(message="What is the service_id?").execute()
    schemas = insp.get_schema_names()
    db_config['DB_SCHEMA'] = inquirer.select(
        message= "For which schema do you wish to create API service",
        choices=schemas
    ).execute()

    schema_table_choices = insp.get_table_names(schema=db_config['DB_SCHEMA'])

    target_tables = inquirer.checkbox(
        message="For which tables to you want to create API's? (use arrows and space to select multiple)",
        choices=['all'] + schema_table_choices,
        validate=lambda result: len(result) >= 1,
        invalid_message="Select at least one option using spacebar",

    ).execute()

    if 'all' in target_tables:
        target_tables = schema_table_choices

    building_block_choices = ["QUERYABLES","CRS", "FILTER","TILES", "STYLES", "PROJECTIONS"]

    api_buildingblocks = inquirer.checkbox(
        message="Select the API blocks (use arrows and space to select multiple)",
        choices=['all'] + building_block_choices,
        validate=lambda result: len(result) >= 1,
        invalid_message="Select at least option using spacebar",
    ).execute()

    if 'all' in api_buildingblocks:
        api_buildingblocks = building_block_choices


    in_docker = inquirer.confirm(
        message="Are you running ldproxy in docker and connecting to a local database?",
        default=True
    ).execute()

    table_config = create_table_config(target_tables, db_schema=db_config['DB_SCHEMA'], insp=insp)

    service_yaml = Service(service_id, table_config, api_buildingblocks, engine)
    SQLProvider_yaml = SQLProvider(service_id, force_axis_order="LON_LAT", table_config=table_config, engine=engine, db_config=db_config, docker=in_docker)
    TileProvider_yaml = TileProvider(service_id, table_config)

    service_yaml.create_yaml()
    SQLProvider_yaml.create_yaml()
    TileProvider_yaml.create_yaml()


    # Display the collected information
    print(f"\nCreating files with the following input:")
    print(f"service_id: {service_id}")
    print(f"tables: {', '.join(target_tables)}")
    print(f"api_blocks: {', '.join(api_buildingblocks)}")
    engine.dispose()

if __name__ == "__main__":
    main()