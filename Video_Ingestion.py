# Databricks notebook source
# DBTITLE 1,IMPORTS
import os
from azure.eventhub import EventHubProducerClient, EventData
from PIL import Image
from time import sleep

# COMMAND ----------

# DBTITLE 1,VARIABLES
CONNECTION_STRING = "YOUR_EVENT_HUB_CONNECTION_STRING"
EVENT_HUB_NAME = "YOUR_EVENT_HUB_NAME"
VIDEO_FILE_PATH = "path_to_your_video_file"
FRAME_RATE = 0.1  # Time interval between frames in seconds

# COMMAND ----------

# DBTITLE 1,HELPER FUNCTION (Sending Pics to EventHub)
def send_picture_to_event_hub(picture, timestamp):
    producer_client = EventHubProducerClient.from_connection_string(CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)
    event_data = EventData(body=picture)
    event_data.properties = {'timestamp': str(timestamp)}
    producer_client.send(event_data)
    producer_client.close()

# COMMAND ----------

# DBTITLE 1,VIDEO STREAM SIMULATION
def simulate_video_stream():
    video = Image.open(VIDEO_FILE_PATH)
    frame_count = 0
    timestamp = 0

    while True:
        try:
            video.seek(frame_count)
        except EOFError:
            break

        picture = video.convert("RGB")
        picture_path = f"frame_{frame_count}.jpg"
        picture.save(picture_path)

        with open(picture_path, "rb") as file:
            picture_data = file.read()

        send_picture_to_event_hub(picture_data, timestamp)

        os.remove(picture_path)

        frame_count += 1
        timestamp += FRAME_RATE
        sleep(FRAME_RATE)

simulate_video_stream()
