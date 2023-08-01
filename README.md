<div style="display: flex; align-items: center;">
  <img src="https://dobiss.com/wp-content/themes/Comith-Wordpress-Theme/assets/logo_dobiss.svg" alt="Dobiss Logo" height="96">&nbsp; &nbsp; &nbsp;
  <img src="https://mqtt.org/assets/img/mqtt-logo-ver.jpg" alt="MQTT Logo" height="96">
</div>

# dobiss-can2mqtt

This repository contains the code for the Dobiss CAN to MQTT Converter. This application allows you to control your Dobiss lights using MQTT messages. It is a prerequisite for the Dobiss MQTT Converter for Homey and needs to be running first.

## Features

- Connects to a CAN bus and an MQTT broker.
- Listens for CAN messages and publishes corresponding MQTT messages.
- Listens for MQTT messages and sends corresponding CAN messages.
- Serves the configuration file over HTTP.

## How to Use

1. Clone this repository.
2. Run the application using `python3 can2mqtt.py`.

## Files

- `can2mqtt.py`: This is the main application file. It connects to the CAN bus and the MQTT broker, listens for messages, and sends corresponding messages on the other bus.
- `config.yaml`: This file contains the configuration for the lights. Each light has a name and an address.

## Dependencies

- `can`: This library is used to connect to the CAN bus and send and receive CAN messages.
- `paho-mqtt`: This library is used to connect to the MQTT broker and send and receive MQTT messages.
- `yaml`: This library is used to load the configuration file.
- `logging`: This library is used for logging.
- `http.server`: This library is used to serve the configuration file over HTTP.
- `threading`: This library is used to run the HTTP server in a separate thread.

## License

This project is licensed under the terms of the license found in the `LICENSE` file.

## Contact

If you have any questions or issues, please open an issue on this repository.

## Credits

Many thanks to [Dries](https://github.com/dries007) for his splendid reverse engineering of the Dobiss protocol and documenting this!
