from kafka import KafkaProducer
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime
import json
import time
import math
import random

# Kafka configuration
BROKER_URL = '#################'
USERNAME = '#################'
PASSWORD = '#################'
CA_FILE = '#################'
ENABLE_PRINTING = False
SEND_INTERVAL_MS = 1000

producer = KafkaProducer(
    bootstrap_servers=[BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    ssl_cafile=CA_FILE,
    ssl_check_hostname=True
)

# Weather API setup
cache_session = requests_cache.CachedSession('.cache', expire_after=60)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 44.4274689,
    "longitude": 26.1028208,
    "current": ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code", 
                "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "apparent_temperature", 
                "is_day", "snowfall", "showers", "rain", "cloud_cover", "pressure_msl", "surface_pressure"],
    "timezone": "auto",
}

# Initial API call
responses = openmeteo.weather_api(url, params=params)
response = responses[0]
current = response.Current()

# Extract initial values
current_temperature_2m = current.Variables(0).Value()
current_relative_humidity_2m = current.Variables(1).Value()
current_precipitation = current.Variables(2).Value()
current_weather_code = current.Variables(3).Value()
current_wind_speed_10m = current.Variables(4).Value()
current_wind_direction_10m = current.Variables(5).Value()
current_wind_gusts_10m = current.Variables(6).Value()
current_apparent_temperature = current.Variables(7).Value()
current_is_day = current.Variables(8).Value()
current_snowfall = current.Variables(9).Value()
current_showers = current.Variables(10).Value()
current_rain = current.Variables(11).Value()
current_cloud_cover = current.Variables(12).Value()
current_pressure_msl = current.Variables(13).Value()
current_surface_pressure = current.Variables(14).Value()

# Store baseline values for simulation
baseline_temp = current_temperature_2m
baseline_humidity = current_relative_humidity_2m
baseline_wind_speed = current_wind_speed_10m
last_api_time = current.Time()

# Simulation parameters
temp_oscillation = 0.5
humidity_oscillation = 2.0
wind_oscillation = 1.5
api_check_interval = 60

iteration = 0
last_api_check = time.time()

def fetch_new_api_data():
    """Fetch fresh data from API"""
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    current = response.Current()
    
    data = {
        'response': response,
        'time': current.Time(),
        'temperature_2m': current.Variables(0).Value(),
        'relative_humidity_2m': current.Variables(1).Value(),
        'precipitation': current.Variables(2).Value(),
        'weather_code': current.Variables(3).Value(),
        'wind_speed_10m': current.Variables(4).Value(),
        'wind_direction_10m': current.Variables(5).Value(),
        'wind_gusts_10m': current.Variables(6).Value(),
        'apparent_temperature': current.Variables(7).Value(),
        'is_day': current.Variables(8).Value(),
        'snowfall': current.Variables(9).Value(),
        'showers': current.Variables(10).Value(),
        'rain': current.Variables(11).Value(),
        'cloud_cover': current.Variables(12).Value(),
        'pressure_msl': current.Variables(13).Value(),
        'surface_pressure': current.Variables(14).Value()
    }
    return data

def simulate_value(baseline, oscillation, iteration, noise=True):
    """Simulate realistic oscillation around baseline value"""
    sine_component = math.sin(iteration * 0.1) * oscillation
    noise_component = random.uniform(-oscillation * 0.2, oscillation * 0.2) if noise else 0
    return baseline + sine_component + noise_component

if ENABLE_PRINTING:
    print(f"\n=== Weather Kafka Producer ===")
    print(f"Authenticated to Kafka at {BROKER_URL}")
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"==============================\n")
    print("Starting real-time weather data streaming... (Press Ctrl+C to stop)\n")

try:
    while True:
        # Check if we need to fetch new API data
        current_time_now = time.time()
        if current_time_now - last_api_check >= api_check_interval:
            if ENABLE_PRINTING:
                print("\n[📡 Checking API for new data...]")
            new_data = fetch_new_api_data()
            
            if new_data['time'] != last_api_time:
                if ENABLE_PRINTING:
                    print(f"[✓ New data received! Updating baseline values...]")
                baseline_temp = new_data['temperature_2m']
                baseline_humidity = new_data['relative_humidity_2m']
                baseline_wind_speed = new_data['wind_speed_10m']
                last_api_time = new_data['time']
                response = new_data['response']
                
                # Update static values
                current_precipitation = new_data['precipitation']
                current_weather_code = new_data['weather_code']
                current_wind_direction_10m = new_data['wind_direction_10m']
                current_wind_gusts_10m = new_data['wind_gusts_10m']
                current_apparent_temperature = new_data['apparent_temperature']
                current_is_day = new_data['is_day']
                current_snowfall = new_data['snowfall']
                current_showers = new_data['showers']
                current_rain = new_data['rain']
                current_cloud_cover = new_data['cloud_cover']
                current_pressure_msl = new_data['pressure_msl']
                current_surface_pressure = new_data['surface_pressure']
            else:
                if ENABLE_PRINTING:
                    print(f"[- No new data yet (API still at {datetime.fromtimestamp(last_api_time).strftime('%H:%M:%S')})]")
            
            last_api_check = current_time_now
            if ENABLE_PRINTING:
                print()
        
        # Simulate oscillating values
        simulated_temp = simulate_value(baseline_temp, temp_oscillation, iteration)
        simulated_humidity = simulate_value(baseline_humidity, humidity_oscillation, iteration)
        simulated_wind_speed = simulate_value(baseline_wind_speed, wind_oscillation, iteration)
        
        # Ensure humidity stays within 0-100%
        simulated_humidity = max(0, min(100, simulated_humidity))

        # Ensure wind speed stays positive
        simulated_wind_speed = max(0, simulated_wind_speed)
        
        # Build weather data payload
        current_display_time = datetime.now()
        
        weather_data = {
            "timestamp": current_display_time.isoformat(),
            "location": {
                "latitude": response.Latitude(),
                "longitude": response.Longitude(),
                "timezone": response.Timezone().decode('utf-8') if isinstance(response.Timezone(), bytes) else response.Timezone(),
                "timezone_abbreviation": response.TimezoneAbbreviation().decode('utf-8') if isinstance(response.TimezoneAbbreviation(), bytes) else response.TimezoneAbbreviation()
            },
            "current_conditions": {
                "temperature": {
                    "value": round(simulated_temp, 2),
                    "unit": "celsius",
                    "apparent": round(current_apparent_temperature, 2)
                },
                "humidity": {
                    "value": round(simulated_humidity, 2),
                    "unit": "percent"
                },
                "wind": {
                    "speed": round(simulated_wind_speed, 2),
                    "direction": round(current_wind_direction_10m, 0),
                    "gusts": round(current_wind_gusts_10m, 2),
                    "unit": "km/h"
                },
                "precipitation": {
                    "total": round(current_precipitation, 2),
                    "rain": round(current_rain, 2),
                    "showers": round(current_showers, 2),
                    "snowfall": round(current_snowfall, 2),
                    "unit": "mm"
                },
                "atmosphere": {
                    "cloud_cover": round(current_cloud_cover, 0),
                    "pressure_msl": round(current_pressure_msl, 2),
                    "surface_pressure": round(current_surface_pressure, 2),
                    "unit_pressure": "hPa"
                },
                "weather_code": int(current_weather_code),
                "is_day": bool(current_is_day == 1)
            },
            "metadata": {
                "iteration": iteration,
                "last_api_update": datetime.fromtimestamp(last_api_time).isoformat(),
                "simulation_mode": "oscillating"
            }
        }
        
        # Send to Kafka
        producer.send('sensor_data', value=weather_data)
        producer.flush()
        
        # Display status
        if ENABLE_PRINTING:
            print(f"⏰ {current_display_time.strftime('%Y-%m-%d %H:%M:%S')} | "
                  f"📡 API: {datetime.fromtimestamp(last_api_time).strftime('%H:%M:%S')} | "
                  f"🌡️ {simulated_temp:.1f}°C | 💧 {simulated_humidity:.1f}% | "
                  f"💨 {simulated_wind_speed:.1f} km/h | ✅ Sent to Kafka")
        
        iteration += 1
        time.sleep(SEND_INTERVAL_MS / 1000.0)
        
except KeyboardInterrupt:
    if ENABLE_PRINTING:
        print("\n\n[Program stopped by user]")
        print(f"Total messages sent: {iteration}")
    producer.close()
    if ENABLE_PRINTING:
        print("Kafka producer closed.")