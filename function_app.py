import azure.functions as func
import logging
import requests
import json
import os
from azure.eventhub import EventHubProducerClient, EventData
from tenacity import retry, wait_exponential, stop_after_attempt

app = func.FunctionApp()

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def get_weather():
    # This fetches from the env variables you set in the Azure Portal
    api_key = os.getenv("WEATHER_API_KEY")
    city = "London"
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True)
def WeatherToEventHub(myTimer: func.TimerRequest) -> None:
    try:
        data = get_weather()
        
        connection_str = os.getenv("EVENT_HUB_CONNECTION")
        eventhub_name = "weather-raw"
        
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str, eventhub_name=eventhub_name
        )
        
        with producer:
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(data)))
            producer.send_batch(event_data_batch)
            
        logging.info("Success: Weather data pushed to Event Hub.")
        
    except Exception as e:
        logging.error(f"Error: {e}")