from typing import List, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta
from string import Template
from loguru import logger

import math
import aiohttp
import asyncio
import os
import requests
import tomli
import time
import subprocess

logger.add("app-interceptor.log", rotation="5 MB")
load_dotenv()

if os.path.exists("config.toml"):
    with open("config.toml", mode="rb") as f:
        CONFIG = tomli.load(f)
else:
    raise FileNotFoundError("config.toml not found")

if CONFIG["verbose"]:
    logger.info("Verbose mode enabled.")
    VERBOSE = True
else:
    VERBOSE = False


async def start_process():
    start_time = time.time()
    rtu_data = await pull_data()
    if not rtu_data:
        logger.error("Failed to pull data from RTU.")

    filtered_data = collect_forced_data(rtu_data)

    if CONFIG["rtu"]["test"]:
        filtered_data = filtered_data[:5]

    tasks = [process_data(obj) for obj in filtered_data]
    await asyncio.gather(*tasks)

    end_time = time.time()
    total_time = end_time - start_time

    logger.debug(f"Total execution time: {total_time:.4f} seconds")


async def pull_data():
    logger.info("Pulling data from RTU...")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            CONFIG["rtu"]["url"],
            auth=aiohttp.BasicAuth(
                CONFIG["rtu"]["username"], CONFIG["rtu"]["password"]
            ),
            ssl=False,
        ) as response:
            logger.info(f"Response status: {response.status}")
            if response.status != 200:
                logger.error("Failed to pull data from RTU.")
            return await response.json()


def collect_forced_data(data) -> List[dict]:
    result = [
        obj
        for obj in data["Points"]
        if obj.get("Status") == CONFIG["rtu"]["status"]["forced"]
    ]
    logger.info(f"Collected {len(result)} forced data.")
    return result


async def process_data(obj):

    value = None

    # To avoid comparing int with float for Binary Input values
    if obj["Type"] == "AI":
        value = obj["Value"]
    elif obj["Type"] == "BI":
        value = obj["ValueRaw"]
    else:
        logger.error("Invalid data type.")
        return

    tag_with_prefix = f"LSO.1.{obj['Name']}"

    created, config_path = await create_config(
        tag_with_prefix, obj["Date"]["Value"], value
    )
    if not created:
        logger.error("Failed to create config file.")
        return

    if CONFIG["piconfig"]["push"]:
        logger.info("Pushing data to PI...")
        await run_piconfig(config_path)


async def create_config(tag: str, timestamp: str, value: float) -> Tuple[bool, str]:
    template_path = CONFIG["piconfig"]["template"]
    config_path = os.path.join(
        os.getcwd(), CONFIG["piconfig"]["config_folder"], f"{tag}.inp"
    )
    archive_time = await get_archive_time(tag, timestamp, value)
    rtu_timestamp, formatted_timestamp = convert_date(archive_time)
    PLACEHOLDER_VALUE = -1
    PLACEHOLDER_MODE = "replace"
    created = False
    src = ""
    replace_content = ""

    with open(template_path, "r") as f:
        src = Template(f.read())

    replace_content += (
        f"{tag},{formatted_timestamp},{PLACEHOLDER_VALUE},{PLACEHOLDER_MODE}\n"
    )
    replace_content += f"{tag},{formatted_timestamp},{value},{PLACEHOLDER_MODE}"

    result = src.substitute(
        tag=tag,
        original_timestamp=rtu_timestamp,
        value=value,
        add_data=replace_content,
    )

    if not os.path.exists(CONFIG["piconfig"]["config_folder"]):
        logger.info("Creating configs folder...")
        os.makedirs(CONFIG["piconfig"]["config_folder"])

    with open(config_path, "w") as f:
        f.write(result)
        created = True

    return created, config_path


async def run_piconfig(config_path):
    # command = subprocess.run(
    #     f"piconfig < {config_path}", shell=True, text=True, capture_output=True
    # )
    command = await asyncio.create_subprocess_shell(
        f"piconfig < {config_path}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await command.communicate()
    if VERBOSE:
        logger.info(stdout.decode())
    if not CONFIG["piconfig"]["export"]:
        os.remove(config_path)


async def get_archive_time(tag: str, time: str, value: float):
    archive_time = None
    start_time, endtime = generate_timerange(time)
    get_recorded_link = f"{CONFIG['piserver']['url']}/piwebapi/points?path=\\\\{CONFIG['piserver']['name']}\\LSO.1.{tag}"

    async with aiohttp.ClientSession() as session:
        async with session.get(
            get_recorded_link,
            auth=aiohttp.BasicAuth(
                CONFIG["piserver"]["username"], CONFIG["piserver"]["password"]
            ),
            ssl=False,
        ) as response:
            tag_response = await response.json()

        if VERBOSE:
            logger.info(f"Get recorded URL: {get_recorded_link}")
            logger.info(f"Tag response: {tag_response}")

        tag_recorded_url = tag_response["Links"]["RecordedData"]
        fetch_timestamp_url = (
            f"{tag_recorded_url}?startTime={start_time}&endTime={endtime}"
        )

        async with session.get(
            fetch_timestamp_url,
            auth=aiohttp.BasicAuth(
                CONFIG["piserver"]["username"], CONFIG["piserver"]["password"]
            ),
            ssl=False,
        ) as response:
            timestamp_response = await response.json()

        if VERBOSE:
            logger.info(f"Fetch timestamp URL: {fetch_timestamp_url}")
            logger.info(f"Timestamp response: {timestamp_response}")

        data = timestamp_response.get("Items", [])
        if not data:
            logger.error("No data found.")
            return start_time

        for item in data:
            item_value = item.get("Value")
            if type(item_value) != type(value):
                logger.warning(
                    f"Skipping item due to type mismatch: {item_value} (type: {type(item_value)})"
                )
                logger.info(
                    f"Expected value type: {type(value)}({value}), Got value type: {type(item_value)}({item_value})"
                )
                continue

            if isinstance(item_value, float) and math.isclose(
                item_value, value, rel_tol=1e-6
            ):
                logger.info("Timestamp found in PIWebAPI. Using the timestamp...")
                archive_time = item["Timestamp"]
                if VERBOSE:
                    logger.info(f"Timestamp: {archive_time}")
                return archive_time
            elif isinstance(item_value, (int, float)) and item_value == value:
                logger.info("Timestamp found in PIWebAPI. Using the timestamp...")
                archive_time = item["Timestamp"]
                if VERBOSE:
                    logger.info(f"Timestamp: {archive_time}")
                return archive_time
            else:
                logger.warning("No timestamp found in PIWebAPI.")
                logger.warning(f"Value {item_value} is not equal to {value}.")
                if VERBOSE:
                    logger.info(f"Timestamp: {archive_time}")
                archive_time = start_time
                return archive_time

        if VERBOSE:
            logger.info(f"Archive time: {archive_time}")
    return archive_time


def old_archive_time(tag: str, time: str, value: float):

    archive_time = None

    start_time, endtime = generate_timerange(time)
    get_recorded_link = f"{CONFIG['piserver']['url']}/piwebapi/points?path=\\\\{CONFIG['piserver']['name']}\{tag}"

    tag_response = requests.get(
        get_recorded_link,
        auth=(CONFIG["piserver"]["username"], CONFIG["piserver"]["password"]),
        verify=False,
    ).json()

    if VERBOSE:
        logger.info(f"Get recorded URL: {get_recorded_link}")
        logger.info(f"Tag response: {tag_response}")

    tag_recorded_url = tag_response["Links"]["RecordedData"]
    fetch_timestamp_url = f"{tag_recorded_url}?startTime={start_time}&endTime={endtime}"

    timestamp_response = requests.get(
        fetch_timestamp_url,
        auth=(CONFIG["piserver"]["username"], CONFIG["piserver"]["password"]),
        verify=False,
    ).json()

    if VERBOSE:
        logger.info(f"Fetch timestamp URL: {fetch_timestamp_url}")
        logger.info(f"Timestamp response: {timestamp_response}")

    data = timestamp_response.get("Items", [])

    if not data or len(data) == 0:
        logger.error("No data found.")
        return start_time

    for item in data:

        if not type(item["Value"]) == float:
            logger.error("Value is not a float. Using the start time...")
            archive_time = start_time
        # Checks if the value is the same as the lookup value
        elif math.isclose(item["Value"], value, rel_tol=1e-6):
            logger.info("Timestamp found in PIWebAPI. Using the timestamp...")
            archive_time = item["Timestamp"]
            if VERBOSE:
                logger.info(f"Timestamp: {archive_time}")
            return archive_time
        else:
            logger.warning("No timestamp found in PIWebAPI.")
            logger.warning(f"Value {item['Value']} is not equal to {value}.")
            if VERBOSE:
                logger.info(f"Timestamp: {archive_time}")
            archive_time = start_time
            return archive_time

    if VERBOSE:
        logger.info(f"Archive time: {archive_time}")
    return archive_time


def generate_timerange(time: str) -> Tuple[str, str]:
    starttime = time

    original_datetime = datetime.fromisoformat(time[:-1])
    new_datetime = original_datetime + timedelta(milliseconds=2)

    # Convert the datetime object back to string format
    endtime = new_datetime.isoformat() + "Z"

    return starttime, endtime


def convert_date(date_str: str) -> Tuple[str, str]:

    date_obj = date_str

    # Ensure the date string has a UTC timezone
    if not date_obj.endswith("Z"):
        raise ValueError(
            "The input date string must end with 'Z' indicating UTC timezone."
        )

    # Remove the 'Z' since datetime.strptime does not expect it
    date_obj = date_obj.rstrip("Z")

    # Split the date string into date and microsecond parts
    date_parts = date_obj.split(".")

    # If there is a microsecond part, handle it
    if len(date_parts) == 2:
        microseconds = date_parts[1]
        # Normalize microseconds to 6 digits
        if len(microseconds) < 6:
            microseconds = microseconds.ljust(6, "0")
        elif len(microseconds) > 6:
            microseconds = microseconds[:6]
        date_obj = f"{date_parts[0]}.{microseconds}"
    else:
        date_obj = f"{date_parts[0]}.000000"

    # Parse the datetime string
    date_obj = datetime.strptime(date_obj, "%Y-%m-%dT%H:%M:%S.%f")

    # Add 8 hours to the datetime object
    date_obj += timedelta(hours=8)

    # Format the datetime object into the desired output format
    end_format = "%d-%b-%Y %H:%M:%S.%f"
    formatted_date = date_obj.strftime(end_format)

    if VERBOSE:
        logger.info(f"Original date: {date_str}")
        logger.info(f"Formatted date: {formatted_date}")

    return date_str, formatted_date


if __name__ == "__main__":
    asyncio.run(start_process())
