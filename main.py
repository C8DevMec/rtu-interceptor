from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta
from string import Template

import uvicorn
import asyncio
import os
import requests
import tomli
import json
import subprocess
import multiprocessing


# TODO: Add logging
# TODO: Remove get_json function

load_dotenv()

if os.path.exists("config.toml"):
    with open("config.toml", mode="rb") as f:
        CONFIG = tomli.load(f)
else:
    raise FileNotFoundError("config.toml not found")


class Date(BaseModel):
    Value: str
    Status: int


class Point(BaseModel):
    Name: str
    Date: Date
    Value: float
    ValueRaw: int
    Status: int


class DataPoint(BaseModel):
    Points: List[Point]
    ChangeID: Optional[int] = None


class SuccessResponse(BaseModel):
    message: str
    status: int


app = FastAPI()


@app.get("/api/v1/interceptor")
async def interceptor() -> SuccessResponse:
    asyncio.create_task(start_process())
    return SuccessResponse(
        message="Data was received and will be processed.", status=200
    )


async def start_process():
    rtu_data = await pull_data()
    with open("response.json", "w") as f:
        json.dump(rtu_data, f, indent=4)
    filtered_data = await collect_forced_data(rtu_data)
    for obj in filtered_data:
        timestamp = convert_date(obj["Date"]["Value"])
        created, config_path = await create_config(obj["Name"], timestamp, obj["Value"])
        if not created:
            raise Exception("Failed to create config file.")

        if CONFIG["piconfig"]["push"]:
            print("Pushing data to PI...")
            await run_piconfig(config_path)


async def pull_data():
    response = requests.post(
        CONFIG["rtu"]["url"],
        auth=(CONFIG["rtu"]["username"], CONFIG["rtu"]["password"]),
        verify=False,
    )
    return response.json()


async def collect_forced_data(data):
    result = [
        obj
        for obj in data["Points"]
        if obj.get("Status") == CONFIG["rtu"]["status"]["forced"]
    ]
    return result


#! TODO: REMOVE THIS FUNCTION
def get_json() -> DataPoint:
    with open("response.json") as file:
        data = json.load(file)
        return DataPoint(**data)


async def create_config(tag: str, timestamp: str, value: float) -> Tuple[bool, str]:
    template_path = CONFIG["piconfig"]["template"]
    config_path = os.path.join(
        os.getcwd(), CONFIG["piconfig"]["config_folder"], f"{tag}.inp"
    )
    PLACEHOLDER_VALUE = -1
    PLACEHOLDER_MODE = "replace"
    created = False
    src = ""
    replace_content = ""

    with open(template_path, "r") as f:
        src = Template(f.read())

    replace_content += f"{tag},{timestamp},{PLACEHOLDER_VALUE},{PLACEHOLDER_MODE}\n"
    replace_content += f"{tag},{timestamp},{value},{PLACEHOLDER_MODE}"

    result = src.substitute(
        tag=tag, timestamp=timestamp, value=value, add_data=replace_content
    )

    if not os.path.exists(CONFIG["piconfig"]["config_folder"]):
        print("Creating configs folder...")
        os.makedirs(CONFIG["piconfig"]["config_folder"])

    with open(config_path, "w") as f:
        f.write(result)
        created = True

    return created, config_path


async def run_piconfig(config_path):
    command = subprocess.run(
        f"piconfig < {config_path}", shell=True, text=True, capture_output=True
    )
    print(command.stdout)


def convert_date(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    formatted_date = date_obj.strftime("%d-%b-%Y %H:%M:%S.%f")

    return formatted_date


if __name__ == "__main__":
    multiprocessing.freeze_support()  # For Windows support
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False, workers=1)
