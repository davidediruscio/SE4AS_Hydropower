import os
import time
import json

from influxdb_client import InfluxDBClient  # type: ignore
from dotenv import load_dotenv  # type: ignore
import paho.mqtt.client as mqtt  # type: ignore
import requests  # NEW: for LLM HTTP calls

# Carica le variabili dal file .env
load_dotenv()

# Variabili dal .env
DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("DOCKER_INFLUXDB_INIT_ORG")
INFLUXDB_BUCKET = os.getenv("DOCKER_INFLUXDB_INIT_BUCKET")

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
PLANNER_TOPIC_PREFIX = os.getenv("PLANNER_TOPIC_PREFIX")
GATE_TOPIC_PREFIX = os.getenv("GATE_TOPIC_PREFIX")
POWER_GATE_ID = os.getenv("POWER_GATE_ID")
SPILLWAY_GATE_COUNT = int(os.getenv("SPILLWAY_GATE_COUNT"))
DAM_HEIGHT = float(os.getenv("DAM_HEIGHT"))
DAM_CRITICAL_HEIGHT = float(os.getenv("DAM_CRITICAL_HEIGHT"))
DAM_MIN_HEIGHT = float(os.getenv("DAM_MIN_HEIGHT"))
QUERY_INTERVAL = int(os.getenv("QUERY_INTERVAL"))
BUCKET_FLOWS_DATA = os.getenv("BUCKET_FLOWS_DATA")
VOLUME_SENSOR_DATA = os.getenv("VOLUME_SENSOR_DATA")
VOLUME_FIELD = os.getenv("VOLUME_FIELD")
HEIGHT_FIELD = os.getenv("HEIGHT_FIELD")
POWER_GATE_OUTFLOW = float(os.getenv("POWER_GATE_OUTFLOW"))
BUCKET_PREDICTED_DATA = os.getenv("BUCKET_PREDICTED_DATA")

# LLM configuration (Ollama, ecc.)
LLM_URL = os.getenv("LLM_URL", "http://ollama:11434/api/chat")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2")

# Connessione a InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)  # type: ignore
query_api = client.query_api()

# MQTT client
# Print installed package version so it's easy to debug compatibility issues
mqtt_pkg_version = getattr(mqtt, '__version__', None) or getattr(mqtt, 'version', 'unknown')
print(f"[LLM_PLANNER] paho-mqtt package version: {mqtt_pkg_version}")

try:
    # paho-mqtt >=2.0 introduced a callback_api_version argument.
    # Keep backwards-compatible behavior by explicitly selecting VERSION1
    # (older callback signatures). If you prefer the newer callbacks,
    # switch to mqtt.CallbackAPIVersion.VERSION2 and update handlers.
    mqtt_client = mqtt.Client(client_id="FSM_LLM", callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
except Exception:
    # Fall back to the simpler constructor for older paho-mqtt releases.
    mqtt_client = mqtt.Client("FSM_LLM")
if MQTT_USER:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)


class BalanceFSM:
    """
    BALANCE state manager based on an LLM.

    Responsibilities:
    - gather context (current state + predictions),
    - call the LLM to obtain a plan (gate openings + rationale),
    - write actions into parent_fsm.actions and reason into parent_fsm.reason.
    """

    def execute(self, parent_fsm: "DamFSM") -> None:
        # Default: keep spillways closed unless LLM says otherwise
        for i in range(1, parent_fsm.SPILLWAY_GATE_COUNT + 1):
            parent_fsm.actions[f"Spillway_Gate_{i}"] = 0

        # Get predicted inflows (average over different horizons)
        daily_inflow = self._avg(self.get_daily_predictions())
        weekly_inflow = self._avg(self.get_weekly_predictions())
        monthly_inflow = self._avg(self.get_monthly_predictions())
        quarterly_inflow = self._avg(self.get_quarterly_predictions())
        semiannual_inflow = self._avg(self.get_semiannual_predictions())

        # Build LLM prompt
        prompt = self.build_llm_prompt(
            parent_fsm=parent_fsm,
            daily_inflow=daily_inflow,
            weekly_inflow=weekly_inflow,
            monthly_inflow=monthly_inflow,
            quarterly_inflow=quarterly_inflow,
            semiannual_inflow=semiannual_inflow,
        )

        plan = self.call_llm(prompt)

        if plan is None:
            # Fallback: simple rule-based behaviour if LLM fails
            print("BALANCE LLM: no valid plan from LLM, using fallback rule.")
            inflow = parent_fsm.inflow
            target_outflow = min(inflow, parent_fsm.POWER_GATE_OUTFLOW)
            target_percentage = min(
                (target_outflow / parent_fsm.POWER_GATE_OUTFLOW) * 100, 100
            )
            new_percentage = self.interpolate(parent_fsm, target_percentage)
            parent_fsm.actions[parent_fsm.POWER_GATE_ID] = new_percentage
            parent_fsm.reason = "Fallback rule-based control in BALANCE (LLM failure)."
            return

        # Apply LLM plan
        self.apply_llm_plan(parent_fsm, plan)

    # ------------------------------------------------------------------ LLM helpers

    @staticmethod
    def build_llm_prompt(
        parent_fsm: "DamFSM",
        daily_inflow: float,
        weekly_inflow: float,
        monthly_inflow: float,
        quarterly_inflow: float,
        semiannual_inflow: float,
    ) -> str:
        """
        Build the natural-language prompt for the LLM.
        """

        prompt = f"""
You are an autonomous planning agent controlling a hydropower dam.

Current state:
- current_level_m: {parent_fsm.height:.2f}
- min_operational_level_m: {parent_fsm.DAM_MIN_HEIGHT * parent_fsm.DAM_HEIGHT:.2f}
- critical_level_m: {parent_fsm.DAM_CRITICAL_HEIGHT * parent_fsm.DAM_HEIGHT:.2f}
- inflow_m3s: {parent_fsm.inflow:.2f}
- outflow_m3s: {parent_fsm.outflow:.2f}
- spillway_gate_count: {parent_fsm.SPILLWAY_GATE_COUNT}
- max_power_gate_outflow_m3s: {parent_fsm.POWER_GATE_OUTFLOW:.2f}

Predicted inflows (averages):
- daily_inflow_m3s: {daily_inflow:.2f}
- weekly_inflow_m3s: {weekly_inflow:.2f}
- monthly_inflow_m3s: {monthly_inflow:.2f}
- quarterly_inflow_m3s: {quarterly_inflow:.2f}
- semiannual_inflow_m3s: {semiannual_inflow:.2f}

Objectives:
- Keep the water level between the minimum operational level and the critical level.
- Prefer to keep the level stable (avoid oscillations).
- Prevent sudden or excessive changes in gate openings.
- Prefer using spillway gates only when the level is high or future inflows are significant.
- Suggest smooth and safe control actions.

You must propose:
- power_gate: a number between 0 and 100 (percentage opening)
- spillway_gates: a list of {parent_fsm.SPILLWAY_GATE_COUNT} numbers between 0 and 100
- rationale: a short explanation (one sentence)
- risk_level: one of LOW, MEDIUM, HIGH

Output requirements:
Respond ONLY with a valid JSON object, with no extra text, comments, explanations, or markdown.

The output format MUST be exactly:

{{
  "power_gate": <number 0-100>,
  "spillway_gates": [<number 0-100 for each spillway>],
  "rationale": "<short explanation>",
  "risk_level": "<LOW|MEDIUM|HIGH>"
}}
"""
        return prompt.strip()

    @staticmethod
    def call_llm(prompt: str):
        """Call the local LLM and return the decoded JSON plan, or None on error."""
        if not LLM_URL:
            print("BALANCE LLM: LLM_URL not configured.")
            return None

        payload = {
            "model": LLM_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a hydropower dam planning agent. You MUST answer only with valid JSON. Please do not answer with empty content.",
                },
                {"role": "user", "content": prompt},
            ],
            "stream": False,
        }

        try:
            resp = requests.post(LLM_URL, json=payload, timeout=60)
            print(f"BALANCE LLM: LLM response status: {resp.status_code}")
            print(f"BALANCE LLM: LLM response text: {resp.text}")
        except Exception as e:
            print(f"BALANCE LLM: error calling LLM: {e}")
            return None

        if resp.status_code != 200:
            print(f"BALANCE LLM: LLM returned status {resp.status_code}: {resp.text}")
            return None

        try:
            data = resp.json()
        except Exception as e:
            print(f"BALANCE LLM: cannot parse LLM HTTP response as JSON: {e}")
            return None

        content = ""
        if isinstance(data, dict):
            msg = data.get("message")
            if isinstance(msg, dict):
                content = str(msg.get("content", "")).strip()
            else:
                content = str(data.get("content", "")).strip()
        if not content:
            print("BALANCE LLM: empty content from LLM.")
            return None

        # Remove possible markdown fences
        if content.startswith("```"):
            content = content.strip("`")
            start = content.find("{")
            end = content.rfind("}")
            if start != -1 and end != -1 and end > start:
                content = content[start : end + 1]

        try:
            plan = json.loads(content)
            return plan
        except json.JSONDecodeError as e:
            print(f"BALANCE LLM: cannot decode plan JSON: {e}. Raw: {content[:200]}")
            return None

    def apply_llm_plan(self, parent_fsm: "DamFSM", plan: dict) -> None:
        """Apply the LLM plan to parent_fsm.actions with safety checks, and set reason."""
        try:
            power_gate = float(plan.get("power_gate", 0.0))
            spillway_list = plan.get("spillway_gates", [])
            rationale = str(plan.get("rationale", "LLM-based BALANCE decision."))
            risk_level = str(plan.get("risk_level", "UNKNOWN"))
        except Exception as e:
            print(f"BALANCE LLM: error extracting fields from plan: {e}")
            return

        # Basic clipping
        power_gate = max(0.0, min(100.0, power_gate))

        safe_spillways = []
        for i in range(parent_fsm.SPILLWAY_GATE_COUNT):
            val = 0.0
            if i < len(spillway_list):
                try:
                    val = float(spillway_list[i])
                except Exception:
                    val = 0.0
            val = max(0.0, min(100.0, val))
            safe_spillways.append(val)

        # Optionally: limit per-step delta if you want (requires last_actions tracking).
        # For now we apply directly.

        parent_fsm.actions[parent_fsm.POWER_GATE_ID] = power_gate
        for i, val in enumerate(safe_spillways, start=1):
            parent_fsm.actions[f"Spillway_Gate_{i}"] = val

        parent_fsm.reason = f"{rationale} (risk={risk_level})"
        print(f"BALANCE LLM: applied plan. power_gate={power_gate:.2f}, "
              f"spillways={safe_spillways}, risk={risk_level}")

    # ------------------------------------------------------------------ prediction helpers

    @staticmethod
    def _avg(predictions):
        """Compute average of values in [(time, value), ...]."""
        if not predictions:
            return 0.0
        return sum(v for _, v in predictions) / len(predictions)

    def get_daily_predictions(self):
        """Next 24h, aggregated hourly (mean)."""
        return self.query_predictions_simple(duration_seconds=24 * 3600, aggregation="mean")

    def get_weekly_predictions(self):
        """Next 7 days, aggregated daily (mean)."""
        return self.query_predictions_simple(duration_seconds=7 * 24 * 3600, aggregation="mean")

    def get_monthly_predictions(self):
        """Next 30 days, aggregated weekly (mean)."""
        return self.query_predictions_simple(duration_seconds=30 * 24 * 3600, aggregation="mean")

    def get_quarterly_predictions(self):
        """Next 90 days, aggregated weekly (mean)."""
        return self.query_predictions_simple(duration_seconds=90 * 24 * 3600, aggregation="mean")

    def get_semiannual_predictions(self):
        """Next 6 months, aggregated monthly (mean)."""
        return self.query_predictions_simple(duration_seconds=180 * 24 * 3600, aggregation="mean")

    def query_predictions_simple(self, duration_seconds, aggregation="mean"):
        """
        Retrieve aggregated predicted inflows using unix timestamps.
        Returns list of (time, value).
        """
        current_time = int(time.time())
        start_time = current_time
        end_time = current_time + duration_seconds

        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "{BUCKET_PREDICTED_DATA}")
            |> filter(fn: (r) => r._field == "total_inflow")
            |> aggregateWindow(every: {duration_seconds}s, fn: {aggregation}, createEmpty: false)
            |> yield(name: "{aggregation}")
        '''

        try:
            result = query_api.query(org=INFLUXDB_ORG, query=query)
            predictions = [
                (record.get_time(), record.get_value())
                for table in result
                for record in table.records
            ]
            return predictions
        except Exception as e:
            print(f"Error retrieving predictions: {e}")
            return []

    # Simple smoothing used in fallback rule
    def interpolate(self, parent_fsm, target_percentage):
        current_percentage = parent_fsm.actions.get(parent_fsm.POWER_GATE_ID, 0)
        new_percentage = current_percentage + (target_percentage - current_percentage) * 0.1
        return min(max(new_percentage, 0), 100)


# ---------------------------------------------------------------------- Main FSM

class DamFSM:
    def __init__(self):
        self.state = "IDLE"  # Stato iniziale
        self.volume = 0.0
        self.height = 0.0
        self.inflow = 0.0
        self.outflow = 0.0
        self.actions = {}
        self.reason = ""  #  explanation for the current plan

        self.POWER_GATE_ID = POWER_GATE_ID
        self.SPILLWAY_GATE_COUNT = SPILLWAY_GATE_COUNT
        self.DAM_HEIGHT = DAM_HEIGHT
        self.DAM_CRITICAL_HEIGHT = DAM_CRITICAL_HEIGHT
        self.DAM_MIN_HEIGHT = DAM_MIN_HEIGHT
        self.POWER_GATE_OUTFLOW = POWER_GATE_OUTFLOW

        self.balance_fsm: BalanceFSM | None = None

        self.critical_height = DAM_CRITICAL_HEIGHT * DAM_HEIGHT  # Altezza critica assoluta
        print(f"Initial Critical Height: {self.critical_height}")

    def set_state(self, new_state):
        print(f"FSM: Transitioning from {self.state} to {new_state}")
        self.state = new_state

    def fetch_data(self):
        """Recupera i dati da InfluxDB."""
        try:
            self.inflow = self.get_last_value(BUCKET_FLOWS_DATA, "total_inflow")
            self.outflow = self.get_last_value(BUCKET_FLOWS_DATA, "total_outflow")
            self.volume = self.get_last_value(VOLUME_SENSOR_DATA, VOLUME_FIELD)
            self.height = self.get_last_value(VOLUME_SENSOR_DATA, HEIGHT_FIELD)
            print(
                f"FSM: Data - Inflow: {self.inflow}, Outflow: {self.outflow}, "
                f"Volume: {self.volume}, Height: {self.height}"
            )
            print(
                f"Critical Height: {self.critical_height}, "
                f"Min Height: {DAM_MIN_HEIGHT * DAM_HEIGHT}"
            )
        except Exception as e:
            print(f"FSM: Error fetching data: {e}")

    def get_last_value(self, measurement, field):
        """Recupera l'ultimo valore di un campo specifico da una misura InfluxDB."""
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -1m)
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> filter(fn: (r) => r._field == "{field}")
            |> last()
        '''
        try:
            result = query_api.query(org=INFLUXDB_ORG, query=query)
            for table in result:
                for record in table.records:
                    return float(record.get_value())
        except Exception as e:
            print(f"Error retrieving {field} from {measurement}: {e}")
        return 0.0

    def execute_state(self):
        """Esegue la logica associata allo stato corrente."""
        print(f"FSM: Current State: {self.state}")
        if self.state == "IDLE":
            self.idle()
        elif self.state == "FILL":
            self.fill()
        elif self.state == "BALANCE":
            self.balance()
        elif self.state == "EMERGENCY":
            self.emergency()

    def idle(self):
        """Stato IDLE: Nessuna azione, monitoraggio passivo."""
        print("FSM: IDLE - Monitoring conditions...")
        self.reason = "Idle: monitoring reservoir level, no gate changes."

        if self.height < DAM_MIN_HEIGHT * DAM_HEIGHT:
            print(
                f"FSM: Transitioning to FILL - Height below "
                f"{DAM_MIN_HEIGHT * DAM_HEIGHT:.2f}"
            )
            self.set_state("FILL")
            return

        if DAM_MIN_HEIGHT * DAM_HEIGHT <= self.height < DAM_CRITICAL_HEIGHT * DAM_HEIGHT:
            print("FSM: Transitioning to BALANCE - Height within safe range.")
            self.set_state("BALANCE")
            return

        if self.height >= DAM_CRITICAL_HEIGHT * DAM_HEIGHT:
            print(
                f"FSM: Transitioning to EMERGENCY - Height above "
                f"{DAM_CRITICAL_HEIGHT * DAM_HEIGHT:.2f}"
            )
            self.set_state("EMERGENCY")
            return

    def fill(self):
        """Stato FILL: Riempimento del lago."""
        print("FSM: FILL - Increasing lake level...")
        self.actions[POWER_GATE_ID] = 0
        for i in range(1, SPILLWAY_GATE_COUNT + 1):
            self.actions[f"Spillway_Gate_{i}"] = 0
        self.reason = "Fill: keeping all gates closed to increase reservoir level."

        if self.height >= DAM_MIN_HEIGHT * DAM_HEIGHT:
            print("FSM: Height reached minimum threshold, transitioning to BALANCE.")
            self.set_state("BALANCE")
        elif self.height >= self.critical_height:
            print("FSM: Height exceeded critical level, transitioning to EMERGENCY.")
            self.set_state("EMERGENCY")

    def balance(self):
        """Stato BALANCE: the control is delegated to the LLM via BalanceFSM."""
        if not self.balance_fsm:
            self.balance_fsm = BalanceFSM()
            print("FSM: Initialized Balance LLM FSM.")
        self.balance_fsm.execute(self)

    def emergency(self):
        """Stato EMERGENCY: Livello critico, abbassare rapidamente."""
        print("FSM: EMERGENCY - Reducing lake level!")
        self.actions[POWER_GATE_ID] = 100
        for i in range(1, SPILLWAY_GATE_COUNT + 1):
            self.actions[f"Spillway_Gate_{i}"] = 100
        self.reason = "Emergency: opening all gates to quickly reduce reservoir level."

        if self.height < self.critical_height:
            print("FSM: Height dropped below critical level, transitioning to BALANCE.")
            self.set_state("BALANCE")

    def publish_actions(self):
        """
        Pubblica le azioni correnti come comando MQTT.

        Il payload ora include anche un campo 'reason' che spiega la decisione.
        """
        try:
            topic = f"{DAM_UNIQUE_ID}/{PLANNER_TOPIC_PREFIX}"
            payload = {
                "actions": self.actions,
                "reason": self.reason,
            }
            mqtt_client.publish(topic, json.dumps(payload), qos=2)
            print(f"FSM: Published actions: {payload}")
        except Exception as e:
            print(f"FSM: Error publishing actions: {e}")


# Riconnessione MQTT
def reconnect_mqtt():
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            break
        except Exception as e:
            print(f"FSM: MQTT Reconnection failed: {e}")
            time.sleep(5)


# Main
if __name__ == "__main__":
    fsm = DamFSM()
    reconnect_mqtt()
    try:
        while True:
            fsm.fetch_data()
            fsm.execute_state()
            fsm.publish_actions()
            time.sleep(QUERY_INTERVAL)
    except KeyboardInterrupt:
        print("FSM: Stopping...")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
