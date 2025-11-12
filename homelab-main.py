#!/usr/bin/env python3
"""
Main script to scan for Govee H5075 thermometers and post data to homelab API.

 - Scans for Govee H5075 Bluetooth thermometers.
 - Emits JSON formatted results to stdout.
 - Posts time-series data for parameters (temperature, humidity, battery) to homelab API.

Environment Variables:
    HOMELAB_API_KEY: API authentication key (required)
    HOMELAB_API_URL: Base URL of the homelab API (default: http://localhost:8080/api)
    BLUETOOTH_DEVICE_TYPE_NAME: Device type name for bluetooth devices (default: "Bluetooth device")

Requirements:
    - python3-bleak (installed globally: `sudo apt install python3-bleak`)
    - requests (installed globally: `sudo apt install python3-requests`)

License:
    MIT License
"""

import asyncio
from bleak import AdvertisementData, BLEDevice, BleakScanner
from datetime import datetime
import json
import os
import requests
import sys
import struct
from typing import Dict, List, Optional

# Import from the govee-h5075 library
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location('govee_h5075', 
                                                   os.path.join(os.path.dirname(__file__), 'govee-h5075.py'))
    
    if spec is None or spec.loader is None:
        raise ImportError('Could not load spec for govee-h5075.py')
    
    govee_h5075 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(govee_h5075)
    GoveeThermometerHygrometer = govee_h5075.GoveeThermometerHygrometer
    Measurement = govee_h5075.Measurement
except Exception as e:
    print(f"Error importing govee-h5075.py: {e}", file = sys.stderr)
    sys.exit(1)

# Attempt to load .env file
def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key not in os.environ:  # Don't override existing env vars
                        os.environ[key] = value
        print('Loaded configuration from .env file', file = sys.stderr)

load_env_file()


# Configuration
API_BASE_URL = os.getenv('HOMELAB_API_URL', 'http://localhost:8080/api')
API_KEY = os.getenv('HOMELAB_API_KEY', '')
BT_DEVICE_TYPE_NAME = os.getenv('BLUETOOTH_DEVICE_TYPE_NAME', 'Bluetooth device')

# Exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# Constants
BT_SCAN_DURATION_SECONDS = 15
API_TIMEOUT_SECONDS = 10
API_DEFAULT_RETRIES = 1


class GoveeDevice:
    """Represents a scanned Govee H5075 device."""
    
    def __init__(self, mac: str, name: str, temperature: float, humidity: float, battery: int):
        self.mac = mac
        self.name = name
        self.temperature = temperature
        self.humidity = humidity
        self.battery = battery
        self.timestamp = datetime.utcnow().replace(microsecond = 0)

    def to_dict(self) -> dict:
        """Convert device data to dictionary."""
        return {
            'mac': self.mac,
            'name': self.name,
            'temperature': self.temperature,
            'humidity': self.humidity,
            'battery': self.battery,
            'timestamp': self.timestamp.isoformat() + 'Z'
        }


class HomelabAPIClient:
    """Client for interacting with the homelab API."""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.timeout_sec = API_TIMEOUT_SECONDS
    
    def get_id_for_device_type(self, type_name: str) -> Optional[int]:
        """Fetch device-type ID for a given device-type name."""
        try:
            response = requests.get(
                f'{self.base_url}/device-types',
                headers = self.headers,
                timeout = self.timeout_sec
            )
            response.raise_for_status()
            data = response.json()
            device_types = data.get('device_types', [])
            
            for device_type in device_types:
                if device_type['name'] == type_name:
                    return device_type['id']
            
            return None
        except Exception as e:
            print(f"Error fetching device-types: {e}", file = sys.stderr)
            raise
    
    def get_devices(self) -> List[dict]:
        """Fetch all devices and attached parameters."""
        try:
            response = requests.get(
                f'{self.base_url}/devices',
                headers = self.headers,
                timeout = self.timeout_sec
            )
            response.raise_for_status()
            data = response.json()
            devices = data.get('devices', [])
            
            # Fetch full details for each device to get serial_number
            detailed_devices = []
            for device in devices:
                try:
                    detail_response = requests.get(
                        f"{self.base_url}/device/{device['id']}",
                        headers = self.headers,
                        timeout = self.timeout_sec
                    )
                    detail_response.raise_for_status()
                    detail_data = detail_response.json()
                    detailed_devices.append(detail_data.get('device', device))
                except Exception as e:
                    print(f"Warning: Could not fetch details for device {device.get('id')}: {e}", file = sys.stderr)
                    detailed_devices.append(device)
            
            return detailed_devices
        except Exception as e:
            print(f"Error fetching devices: {e}", file = sys.stderr)
            raise
    
    def create_device(self, mac: str, name: str, type_id: int) -> Optional[dict]:
        """Create a new device."""
        try:
            payload = {
                'type_id': type_id,
                'name': name,
                'serial_number': mac,
                'location': None,
                'description': f'Govee H5075 thermometer/hygrometer',
                'is_active': True
            }
            response = requests.post(
                f'{self.base_url}/devices',
                json = payload,
                headers = self.headers,
                timeout = self.timeout_sec
            )
            response.raise_for_status()
            data = response.json()
            return data.get('device')
        except Exception as e:
            print(f"Error creating device {mac}: {e}", file = sys.stderr)
            raise
    
    def create_parameter(self, device_id: int, name: str, unit: str) -> Optional[dict]:
        """Create a new parameter for a device."""
        try:
            payload = {
                'device_id': device_id,
                'name': name,
                'unit': unit,
                'alarm_type': 'none'
            }
            response = requests.post(
                f'{self.base_url}/device-parameters',
                json = payload,
                headers = self.headers,
                timeout = self.timeout_sec
            )
            response.raise_for_status()
            data = response.json()
            return data.get('device_parameter')
        except Exception as e:
            print(f"Error creating parameter {name} for device {device_id}: {e}", file = sys.stderr)
            raise
    
    def post_timeseries_data(self, parameter_id: int, data_points: List[dict], retries: int = API_DEFAULT_RETRIES) -> bool:
        """
        Post time-series data.

        Args:
            parameter_id: Parameter ID
            data_points: List shape [{"value": 25.8, "time": "2023-01-30T13:00:00Z"}]
        
        Returns:
            Success Boolean
        """
        attempt = 0
        while attempt <= retries:
            try:
                payload = {'data': data_points}
                response = requests.post(
                    f'{self.base_url}/device-parameters/{parameter_id}/data',
                    json = payload,
                    headers = self.headers,
                    timeout = self.timeout_sec
                )
                if 'errors' in response.json():
                    print(f"Errors occurred while posting data to parameter {parameter_id}: {response.json()['errors']}", file = sys.stderr)
                response.raise_for_status()
                return True
            except Exception as e:
                attempt += 1
                if attempt > retries:
                    print(f"Error posting data to parameter {parameter_id} after {retries} retries: {e}", file = sys.stderr)
                    return False
                print(f"Retry {attempt}/{retries} for parameter {parameter_id}...", file = sys.stderr)
        return False


async def scan_govee_devices(duration: int = BT_SCAN_DURATION_SECONDS) -> List[GoveeDevice]:
    """
    Scan for Govee H5075 devices using Bluetooth.
    
    Args:
        duration: Scan duration in seconds
        
    Returns:
        List of GoveeDevice objects
    """
    found_devices: Dict[str, GoveeDevice] = {}
    
    def decode_5074(bytes_data: bytes) -> tuple:
        """Decode H5074 advertisement data."""
        temperatureC, relHumidity = struct.unpack("<hh", bytes_data[1:5])
        temperatureC /= 100
        relHumidity /= 100
        battery = bytes_data[5]
        return round(temperatureC, 1), round(relHumidity, 1), battery
    
    def decode_5179(mfg_data: bytes) -> tuple:
        """Decode H5179 advertisement data."""
        temp, hum, batt = struct.unpack_from("<HHB", mfg_data, 4)
        # Negative temperature stored as two's complement
        if temp & 0x8000:
            temp = temp - 0x10000
        temperature = float(temp / 100.0)
        humidity = float(hum / 100.0)
        battery = int(batt)
        return round(temperature, 1), round(humidity, 1), battery
    
    def callback(device: BLEDevice, advertising_data: AdvertisementData):
        """Callback for BLE device advertisement."""
        if device.address not in found_devices:
            if device.name and device.address.upper()[0:9] in GoveeThermometerHygrometer.MAC_PREFIX:
                # Check for H5075/H5074 (manufacturer ID 0xec88)
                if 0xec88 in advertising_data.manufacturer_data:
                    mfg_data = advertising_data.manufacturer_data[0xec88]
                    
                    if "H5074" in device.name:
                        temperatureC, relHumidity, battery = decode_5074(mfg_data)
                    else:
                        # H5075 format
                        measurement = Measurement.from_bytes(
                            bytes = mfg_data[1:4],
                            humidityOffset = 0,
                            temperatureOffset = 0
                        )
                        if measurement:
                            temperatureC = measurement.temperatureC
                            relHumidity = measurement.relHumidity
                            battery = mfg_data[4]
                        else:
                            return
                    
                    govee_device = GoveeDevice(
                        mac = device.address,
                        name = device.name,
                        temperature = temperatureC,
                        humidity = relHumidity,
                        battery = battery
                    )
                    found_devices[device.address] = govee_device
                    print(f"Found device: {device.name} ({device.address})", file = sys.stderr)
                
                # Check for H5179 (manufacturer ID 0x8801)
                elif 0x8801 in advertising_data.manufacturer_data:
                    mfg_data = advertising_data.manufacturer_data[0x8801]
                    temperatureC, relHumidity, battery = decode_5179(mfg_data)
                    
                    govee_device = GoveeDevice(
                        mac = device.address,
                        name = device.name,
                        temperature = temperatureC,
                        humidity = relHumidity,
                        battery = battery
                    )
                    found_devices[device.address] = govee_device
                    print(f"Found device: {device.name} ({device.address})", file = sys.stderr)
    
    print(f"Scanning for Govee H5075 devices for {duration} seconds...", file = sys.stderr)
    async with BleakScanner(callback) as scanner:
        await asyncio.sleep(duration)
    
    print(f"Scan complete. Found {len(found_devices)} device(s).", file = sys.stderr)
    return list(found_devices.values())

#
# Entry point
#
def main():
    """Main entry point."""
    if not API_KEY:
        print("Error: HOMELAB_API_KEY environment variable not set", file = sys.stderr)
        return EXIT_FAILURE
    
    try:
        print("\n=== Scanning for Bluetooth devices. ===", file = sys.stderr)
        scanned_devices = asyncio.run(scan_govee_devices())
        
        if not scanned_devices:
            print("No Govee H5075 devices found.", file = sys.stderr)
            return EXIT_SUCCESS
        
        print("\n=== Scanned Devices ===", file = sys.stderr)
        print(json.dumps([device.to_dict() for device in scanned_devices], indent = 2))
        
        print("\n=== Fetching devices from homelab API ===", file = sys.stderr)
        api_client = HomelabAPIClient(API_BASE_URL, API_KEY)
        api_devices = api_client.get_devices()
        
        # Create a mapping of MAC addresses to device IDs
        mac_to_device: Dict[str, dict] = {}
        for device in api_devices:
            mac = device.get('serial_number', '').upper()
            if mac:
                mac_to_device[mac] = device
        
        print(f"Found {len(api_devices)} devices in API database.", file = sys.stderr)

        bt_device_type_name = BT_DEVICE_TYPE_NAME.strip('"')
        bt_device_type_id = None
        try:
            bt_device_type_id = api_client.get_id_for_device_type(type_name = bt_device_type_name)
        except Exception as e:
            print(f"Failed to query device type '{bt_device_type_name}': {e}", file = sys.stderr)
        

        # Insert missing devices
        for scanned in scanned_devices:
            mac_upper = scanned.mac.upper()
            if mac_upper not in mac_to_device:
                print(f"Device {scanned.mac} not found in database. Inserting...", file = sys.stderr)

                if bt_device_type_id is None:
                    print(f"Device type '{bt_device_type_name}' missing. Skipping insert.", file = sys.stderr)
                    continue
                else:
                    try:
                        new_device = api_client.create_device(
                            mac = scanned.mac,
                            name = scanned.name,
                            type_id = bt_device_type_id
                        )
                        if new_device:
                            mac_to_device[mac_upper] = new_device
                            print(f"Successfully inserted device {scanned.mac}", file = sys.stderr)
                    except Exception as e:
                        print(f"Failed to insert device {scanned.mac}: {e}", file = sys.stderr)
                        continue
        
        print("\n=== Checking device parameters ===", file = sys.stderr)
        required_parameters = {
            'temperature': '°C',
            'humidity': '%',
            'battery': '%'
        }
        
        device_parameters: Dict[str, Dict[str, int]] = {}  # MAC -> {param_name: param_id}
        
        for scanned in scanned_devices:
            mac_upper = scanned.mac.upper()
            if mac_upper not in mac_to_device:
                print(f"Skipping {scanned.mac} - not in database", file = sys.stderr)
                continue
            
            print(f"Check parameters for device {mac_upper}", file = sys.stderr)
            
            device = mac_to_device[mac_upper]
            device_id = device['id']
            existing_params = device['parameters']
            
            param_name_to_id: Dict[str, int] = {}
            
            for param in existing_params:
                param_name = param['name'].lower()
                param_name_to_id[param_name] = param['id']
            
            # Insert missing parameters
            for param_name, unit in required_parameters.items():
                if param_name not in param_name_to_id:
                    print(f"Parameter '{param_name}' missing for device {scanned.mac}. Creating...", file = sys.stderr)
                    try:
                        new_param = api_client.create_parameter(
                            device_id = device_id,
                            name = param_name,
                            unit = unit
                        )
                        if new_param:
                            param_name_to_id[param_name] = new_param['id']
                            print(f"Successfully created parameter '{param_name}' for device {scanned.mac}", file = sys.stderr)
                    except Exception as e:
                        print(f"Failed to create parameter '{param_name}' for device {scanned.mac}: {e}", file = sys.stderr)
                        continue
            
            device_parameters[mac_upper] = param_name_to_id
        

        print("\n=== Insert time-series data ===", file = sys.stderr)
        overall_success = True
        
        for scanned in scanned_devices:
            mac_upper = scanned.mac.upper()
            if mac_upper not in device_parameters:
                print(f"Skipping {scanned.mac} - no parameters available", file = sys.stderr)
                continue
            
            param_ids = device_parameters[mac_upper]
            timestamp_iso = scanned.timestamp.isoformat() + 'Z'
            
            # Prepare data for each parameter
            parameter_data = {
                'temperature': scanned.temperature,
                'humidity': scanned.humidity,
                'battery': scanned.battery
            }
            
            for param_name, value in parameter_data.items():
                if param_name not in param_ids:
                    print(f"Parameter '{param_name}' not available for device {scanned.mac}", file = sys.stderr)
                    continue
                
                param_id = param_ids[param_name]
                data_points = [{'value': value, 'time': timestamp_iso}]
                
                success = api_client.post_timeseries_data(param_id, data_points, retries = 1)
                
                if success:
                    print(f"Successfully posted {param_name} data for device {scanned.mac}", file = sys.stderr)
                else:
                    print(f"Failed to post {param_name} data for device {scanned.mac}", file = sys.stderr)
                    overall_success = False
        
        print("\n=== Processing complete ===", file = sys.stderr)
        return EXIT_SUCCESS if overall_success else EXIT_FAILURE
        
    except KeyboardInterrupt:
        print("\nInterrupted by user", file = sys.stderr)
        return EXIT_FAILURE
    except Exception as e:
        print(f"Unexpected error: {e}", file = sys.stderr)
        import traceback
        traceback.print_exc()
        return EXIT_FAILURE


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
