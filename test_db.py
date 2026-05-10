from simulator.db import make_engine, init_schema

engine = make_engine()
if engine is None:
    print("ERROR: DB_URL not found in .env")
else:
    init_schema(engine)
    print("Connected — tables created.")