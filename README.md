<div style="display: flex; align-items: center;">
  <img src="https://dobiss.com/wp-content/themes/Comith-Wordpress-Theme/assets/logo_dobiss.svg" alt="Dobiss Logo" height="96">&nbsp; &nbsp; &nbsp;
  <img src="https://mqtt.org/assets/img/mqtt-logo-ver.jpg" alt="MQTT Logo" height="96">
</div>

# dobiss-can2mqtt

A small Python bridge between a Dobiss CAN installation and MQTT. It publishes observed relay states as retained MQTT messages and converts validated MQTT commands into Dobiss CAN frames. It can also serve the local light mapping to the companion [Homey app](https://github.com/skeltavik/be.bramruttens.dobisscanconverter).

> [!CAUTION]
> This service can control physical lighting. Keep the CAN interface, MQTT broker and configuration endpoint on a trusted network. Configure broker authentication and ACLs before allowing network access.

## Data flow

```text
MQTT dobiss/light/<address>/state/set
                  │
                  ▼
       topic + payload validation
                  │
                  ▼
           Dobiss CAN SET
                  │
                  ▼
       CAN SET/GET observation
                  │
                  ▼
MQTT dobiss/light/<address>/state (retained)
```

GET replies do not contain their source address. The bridge therefore correlates them with recently observed GET requests through a bounded, expiring FIFO. Stale requests are discarded rather than risking an indefinitely shifted state mapping.

## Requirements

- Linux with Python 3.10 or newer
- a SocketCAN-compatible CAN adapter and configured interface, normally `can0`
- an MQTT broker
- read/write permission on the CAN interface

## Installation

```bash
git clone https://github.com/skeltavik/dobiss-can2mqtt.git
cd dobiss-can2mqtt
uv venv --python 3.10
uv pip install --python .venv/bin/python --require-hashes -r requirements.txt
cp config.example.yaml config.yaml
```

`requirements.txt` is generated from `requirements.in`, contains exact versions and SHA-256 hashes, and must be installed with `--require-hashes`.

## Configure the lights

`config.yaml` is deliberately ignored by Git because room names and CAN addresses describe a real installation. Start from the neutral example:

```yaml
- name: Example Light 1
  address: "0100"
- name: Example Light 2
  address: "0101"
```

Each address must be a unique four-character hexadecimal string. Addresses are normalized to uppercase for both MQTT topics and the Homey configuration endpoint. Invalid structures, missing names, malformed addresses and duplicates make startup fail closed.

## Runtime settings

Settings are read from environment variables. Defaults retain the historical local-only setup.

| Variable | Default | Purpose |
|---|---:|---|
| `DOBISS_CONFIG_PATH` | `config.yaml` | Local light mapping |
| `DOBISS_CAN_INTERFACE` | `socketcan` | python-can interface |
| `DOBISS_CAN_CHANNEL` | `can0` | CAN channel |
| `DOBISS_MQTT_HOST` | `localhost` | MQTT broker hostname |
| `DOBISS_MQTT_PORT` | `1883` | MQTT broker port |
| `DOBISS_MQTT_USERNAME` | unset | Broker username |
| `DOBISS_MQTT_PASSWORD` | unset | Broker password; inject at runtime |
| `DOBISS_MQTT_TLS` | `false` | Enable the default TLS context |
| `DOBISS_HTTP_HOST` | `127.0.0.1` | Configuration HTTP bind address |
| `DOBISS_HTTP_PORT` | `8000` | Configuration HTTP port |

Example with an authenticated TLS broker:

```bash
export DOBISS_MQTT_HOST=mqtt.example.lan
export DOBISS_MQTT_PORT=8883
export DOBISS_MQTT_USERNAME=dobiss-bridge
export DOBISS_MQTT_PASSWORD='[REDACTED]'
export DOBISS_MQTT_TLS=true
python can2mqtt.py
```

Do not put MQTT credentials in Git, shell history or `config.yaml`.

## Homey configuration endpoint

The companion Homey app fetches:

```text
http://<converter-address>:<port>/config.yaml
```

The secure default, `127.0.0.1:8000`, is not reachable from another device. To pair a physical Homey, bind explicitly to the converter's trusted LAN address:

```bash
export DOBISS_HTTP_HOST=192.0.2.10   # replace with the converter's LAN address
export DOBISS_HTTP_PORT=8000
python can2mqtt.py
```

Prefer a specific LAN address over `0.0.0.0`, and restrict the port with a host firewall so only Homey can connect. The endpoint exposes names and addresses from `config.yaml`; it must not be internet-facing.

## MQTT topics and ACLs

For address `0100`:

| Direction | Topic | Payload |
|---|---|---|
| MQTT → CAN | `dobiss/light/0100/state/set` | `ON`, `OFF`, `1` or `0` |
| CAN → MQTT | `dobiss/light/0100/state` | retained `ON` or `OFF` |

A dedicated broker identity should have only:

```text
subscribe: dobiss/light/+/state/set
publish:   dobiss/light/+/state
```

Unknown topics, malformed CAN frames and ambiguous state bytes are ignored rather than translated into physical commands or false state.

## Run

```bash
python can2mqtt.py
```

Before starting, configure SocketCAN for your adapter. A typical setup resembles:

```bash
sudo ip link set can0 type can bitrate 125000
sudo ip link set can0 up
```

Adapter-specific configuration may differ.

## Development

Install the hash-pinned development environment:

```bash
uv pip install --python .venv/bin/python --require-hashes -r requirements-dev.txt
```

Run the complete suite:

```bash
python -m pytest --cov=can2mqtt --cov-fail-under=85
python -m bandit -q -r can2mqtt.py
python -m pip_audit -r requirements.txt --require-hashes --disable-pip --progress-spinner off
python scripts/check_pinned_dependencies.py
```

The tests include a virtual Dobiss controller on python-can's virtual bus, so MQTT↔CAN round trips can be validated without physical hardware.

## Updating dependencies

Edit `requirements.in` or `requirements-dev.in`, then regenerate both lockfiles with a trusted `uv` installation:

```bash
uv pip compile requirements.in --python-version 3.10 --generate-hashes --output-file requirements.txt
uv pip compile requirements-dev.in --python-version 3.10 --generate-hashes --output-file requirements-dev.txt
python scripts/check_pinned_dependencies.py
```

External GitHub Actions must use complete 40-character commit SHAs. CI rejects mutable Action tags, unhashed Python dependencies and dependency installation without `--require-hashes`.

## Security model and limitations

- Dobiss CAN itself does not authenticate senders; protect physical and network access to the bus.
- MQTT is an authorization boundary because valid messages produce physical actions.
- TLS protects MQTT transport but does not replace broker ACLs.
- The HTTP endpoint has no application-level authentication and should be restricted at the network layer.
- GET reply correlation assumes Dobiss replies follow observed requests in order. Entries are bounded and expire, but the protocol cannot provide cryptographic attribution.
- MQTT and CAN failures are logged by their underlying libraries; full service supervision and graceful restart should be provided by the deployment environment.

## License and credits

This project is licensed under the GNU General Public License v3.0; see [`LICENSE`](LICENSE).

Many thanks to [Dries](https://github.com/dries007) for reverse-engineering and documenting the Dobiss protocol:
<https://gist.github.com/dries007/436fcd0549a52f26137bca942fef771a>
