import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AguaInc.IoT.SensorPipeline")

class SmartFilterSensor:
    def __init__(self, sensor_id: str, timeout_ms: int = 5000):
        self.sensor_id = sensor_id
        # FIX: Increased default timeout from 1500ms to 5000ms to account for high-latency Wi-Fi drops
        self.timeout_ms = timeout_ms 
        self.max_retries = 3

    def read_telemetry(self):
        """
        Reads water flow and TDS (Total Dissolved Solids) data from the remote unit.
        Includes a retry mechanism to mitigate intermittent v4.2.1 firmware drops.
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                attempt += 1
                logger.info(f"[{self.sensor_id}] Fetching telemetry data (Attempt {attempt}/{self.max_retries})...")
                
                # Simulating network call to hardware sensor
                start_time = time.time()
                
                # (Dummy logic for network simulation)
                # If network latency > timeout_ms, throw TimeoutError
                
                logger.info(f"[{self.sensor_id}] Success: Telemetry received. Flow Rate: 2.4L/m, TDS: 120ppm")
                return {"status": "success", "flow_rate": 2.4, "tds": 120}

            except TimeoutError as e:
                logger.warning(f"[{self.sensor_id}] Timeout encountered at {self.timeout_ms}ms: {str(e)}")
                if attempt < self.max_retries:
                    time.sleep(1) # Backoff before retry
                else:
                    logger.error(f"[{self.sensor_id}] CRITICAL: Max retries reached. Telemetry stream broken.")
                    return {"status": "error", "reason": "Sensor Timeout"}

# Dummy Execution
if __name__ == "__main__":
    sensor = SmartFilterSensor(sensor_id="AGUA-ST3-9942")
    sensor.read_telemetry()
