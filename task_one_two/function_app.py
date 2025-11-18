import azure.functions as func
import logging
import random
import json
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# -----------------------------
# ADD SENSORS
# -----------------------------
@app.route(route="add_sensors")
@app.sql_output(
    arg_name="sensorData",
    command_text="[dbo].[SensorData]",
    connection_string_setting="ConnectionString"
)
def add_sensors(req: func.HttpRequest, sensorData: func.Out[func.SqlRowList]) -> func.HttpResponse:
    start = time.time()
    logging.info("Python HTTP trigger 'add_sensors' processed a request.")

    # Get ?count= from query
    count_param = req.params.get("count")
    sensor_count = int(count_param) if count_param else 20

    # Generate sensor objects
    sensors = [Sensor(i) for i in range(1, sensor_count + 1)]
    rows = [s.get_data() for s in sensors]

    # Insert rows to SQL
    sensorData.set(func.SqlRowList(rows))

    return func.HttpResponse(
        body=json.dumps({
            "results": rows,
            "processing_time": time.time() - start
        }),
        status_code=200,
        mimetype="application/json"
    )


# -----------------------------
# GET SENSORS
# -----------------------------
@app.route(route="get_sensors")
@app.sql_input(
    arg_name="sensorData",
    command_text="SELECT * FROM [dbo].[SensorData]",
    connection_string_setting="ConnectionString"
)
def get_sensors(req: func.HttpRequest, sensorData: func.SqlRowList) -> func.HttpResponse:
    start = time.time()
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

    return func.HttpResponse(
        body=json.dumps({
            "response": response,
            "processing_time": time.time() - start
        }),
        status_code=200,
        mimetype="application/json"
    )


class Sensor:
    def __init__(self, sensor_id: int):
        self.sensor_id = sensor_id

    def get_data(self):
        return {
            "sensorId": self.sensor_id,
            "temperature": self.read_temperature(),
            "windSpeed": self.read_wind_speed(),
            "humidity": self.read_humidity(),
            "carbonLevel": self.read_co2_level()
        }

    def round_normal(self, low, high):
        while True:
            x = random.normalvariate((high + low) / 2, (high + low) / 6)
            if low <= x <= high:
                return round(x, 2)
            return low if x < low else high

    def read_temperature(self):
        return self.round_normal(5, 18)

    def read_wind_speed(self):
        return self.round_normal(12, 24)

    def read_humidity(self):
        return self.round_normal(30, 60)

    def read_co2_level(self):
        return self.round_normal(400, 1000)