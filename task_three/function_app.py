import azure.functions as func
import logging
import random
import json
import time
from ..task_one_two.function_app import Sensor


app = func.FunctionApp()

@app.schedule(
    schedule="*/10 * * * * *",  # every 10 seconds
    arg_name="timer",
    run_on_startup=True,
    use_monitor=True
)

@app.sql_output(
    arg_name="sensorData",
    command_text="[dbo].[SensorData]",
    connection_string_setting="ConnectionString"
)
def add_sensors_timer(timer: func.TimerRequest, sensorData: func.Out[func.SqlRowList]) -> None:
    start = time.time()
    if timer.past_due:
        logging.warning("Timer is past due!")

    # Generate 20 sensors
    sensors = [Sensor(i) for i in range(1, 21)]
    rows = [s.get_data() for s in sensors]

    # Insert rows into SQL
    sensorData.set(func.SqlRowList(rows))
    logging.info("Inserted %d sensor records in %.2f seconds", len(rows), time.time() - start)
   




@app.function_name(name="SensorSqlTrigger")
@app.sql_trigger(arg_name="sensorChanges",
                 table_name="SensorData",
                 connection_string_setting="ConnectionString")
@app.sql_input(
    arg_name="sensorData",
    command_text="SELECT * FROM [dbo].[SensorData]",
    connection_string_setting="ConnectionString"
)
def sensor_sql_trigger(sensorChanges: str, sensorData: func.SqlRowList) -> None:
    rows = [dict(row) for row in sensorData]

    datas = ['temperature', 'windSpeed', 'humidity', 'carbonLevel']
    all_data = {sensor: {d: [] for d in datas} for sensor in range(1, 21)}

    for row in rows:
        sensor_id = row['sensorId']
        for d in datas:
            all_data[sensor_id][d].append(row[d])

    response = ""
    for sensor in all_data:
        response += f"Sensor {sensor}:\n"
        for d in datas:
            vals = all_data[sensor][d]
            if vals:
                avg = sum(vals) / len(vals)
                minimum = min(vals)
                maximum = max(vals)
                response += f"  {d} - Avg: {avg:.2f}, Min: {minimum}, Max: {maximum}\n"
