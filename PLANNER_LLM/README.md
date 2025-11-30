# PLANNER_LLM

LLM-driven planner for the SE4AS hydropower project.

- Reads measurements from InfluxDB (same schema as the existing planner).
- Publishes gate actions to MQTT on the same topic as the classic planner.
- Uses a finite-state machine:
  - IDLE, FILL, EMERGENCY -> rule-based
  - BALANCE -> delegates gate planning to a local LLM (e.g., Ollama).

Environment variables are read from the project root `.env` file
(the same used by the other services). Additional variables:

- `LLM_URL`   (default: `http://ollama:11434/api/chat`)
- `LLM_MODEL` (default: `llama3.2`)

To build and run in Docker Compose, add a service like:

  planner_llm:
    build:
      context: ./PLANNER_LLM
    container_name: my_se4as_pr_PLANNER_LLM
    env_file:
      - .env
    networks:
      - network
    depends_on:
      mqtt:
        condition: service_healthy
      influxdb:
        condition: service_healthy
      ollama:
        condition: service_started
    restart: always

Remember to also define an `ollama` service providing the LLM.

## Compatibility note — paho-mqtt 2.x ⚠️

Starting with paho-mqtt 2.0 the Client constructor changed: you may need to pass
an explicit `callback_api_version` to select how callback functions receive
parameters. This repository opts to be backwards-compatible by selecting
`mqtt.CallbackAPIVersion.VERSION1` when creating the client. If you upgrade your
code to use the newer callback signatures, you can switch the client to
`mqtt.CallbackAPIVersion.VERSION2` (and update any callback signatures accordingly).

If you encounter an error like:

```text
ValueError: Unsupported callback API version: version 2.0 added a callback_api_version
```

then either upgrade your callback handlers to the new signature or ensure the
service uses the older callback API by keeping the `VERSION1` setting as shown
in `main.py`.
