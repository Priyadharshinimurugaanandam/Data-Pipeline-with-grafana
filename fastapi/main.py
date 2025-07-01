from fastapi import FastAPI
from pymongo import MongoClient
from fastapi.responses import JSONResponse

app = FastAPI()

# Connect to MongoDB (running in Docker)
client = MongoClient("mongodb://host.docker.internal:27017")
db = client["airflow_db"]
collection = db["joints_collection"]

@app.get("/jointdata")
def get_joint_data():
    data = []
    for doc in collection.find().limit(500):
        try:
            # Convert time in seconds to milliseconds
            timestamp_ms = int(float(doc.get("time", 0)) * 1000)
        except:
            timestamp_ms = 0

        data.append({
            "time": timestamp_ms,
            "joint1": doc.get(" root/AxesControl/axesPositionsActual[0]", 0),
            "joint2": doc.get(" root/AxesControl/axesPositionsActual[1]", 0),
            "joint3": doc.get(" root/AxesControl/axesPositionsActual[2]", 0),
            "joint4": doc.get(" root/AxesControl/axesPositionsActual[3]", 0),
            "joint5": doc.get(" root/AxesControl/axesPositionsActual[4]", 0),
            "joint6": doc.get(" root/AxesControl/axesPositionsActual[5]", 0),
        })

    return JSONResponse(content=data)
