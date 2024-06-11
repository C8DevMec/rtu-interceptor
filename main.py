from typing import List, Optional, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta
from string import Template
from loguru import logger

import aiohttp
import asyncio
import os
import requests
import tomli
import json
import subprocess

logger.add("app-interceptor.log", rotation="5 MB")
load_dotenv()

if os.path.exists("config.toml"):
    with open("config.toml", mode="rb") as f:
        CONFIG = tomli.load(f)
else:
    raise FileNotFoundError("config.toml not found")


async def start_process():
    rtu_data = await pull_data()
    if not rtu_data:
        logger.error("Failed to pull data from RTU.")
    with open("response.json", "w") as f:
        json.dump(rtu_data, f, indent=4)
    filtered_data = collect_forced_data(rtu_data)
    for obj in filtered_data:
        created, config_path = await create_config(
            obj["Name"], obj["Date"]["Value"], obj["Value"]
        )
        if not created:
            logger.error("Failed to create config file.")

        if CONFIG["piconfig"]["push"]:
            logger.info("Pushing data to PI...")
            await run_piconfig(config_path)


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


async def create_config(tag: str, timestamp: str, value: float) -> Tuple[bool, str]:
    template_path = CONFIG["piconfig"]["template"]
    config_path = os.path.join(
        os.getcwd(), CONFIG["piconfig"]["config_folder"], f"{tag}.inp"
    )
    original_timestamp, formatted_timestamp = convert_date(timestamp)
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
        original_timestamp=original_timestamp,
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
    command = subprocess.run(
        f"piconfig < {config_path}", shell=True, text=True, capture_output=True
    )
    logger.info(command.stdout)


def convert_date(date_str) -> Tuple:
    date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    date_obj += timedelta(hours=8)  # UTC+8
    formatted_date = date_obj.strftime("%d-%b-%Y %H:%M:%S.%f")

    return date_str, formatted_date


if __name__ == "__main__":
    asyncio.run(start_process())
