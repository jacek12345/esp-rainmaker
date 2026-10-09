#!/usr/bin/env python3
"""Forward HUSKYLENS 2 recognition results to an Adafruit IO feed."""

import json
import os
import threading
import time
import uuid
from urllib.parse import urljoin

import requests
from paho.mqtt import client as mqtt_client


CAMERA_URL = os.getenv("HUSKYLENS_URL", "http://192.168.1.135:3000").rstrip("/")
AIO_USERNAME = os.getenv("ADAFRUIT_IO_USERNAME", "jacekAF")
AIO_KEY = os.getenv("ADAFRUIT_IO_KEY")
FEED_TOPIC = f"{AIO_USERNAME}/feeds/tablice"
POLL_INTERVAL = 1.0
MIN_PUBLISH_INTERVAL = 2.1
MAX_FEED_PAYLOAD_BYTES = 1024
IMAGE_KEYS = {"image", "image_data", "image_data_base64", "frame", "photo"}


class HuskyLensMCPClient:
    def __init__(self, server_url):
        self.server_url = server_url.rstrip("/")
        self._session = requests.Session()
        self._sse_session = requests.Session()
        self._message_url = None
        self._pending = {}
        self._lock = threading.Lock()
        self._endpoint_ready = threading.Event()
        self._stop = threading.Event()
        self._sse_error = None
        self._request_id = 0
        self._sse_thread = None
        self._sse_response = None

    def connect(self):
        self._sse_thread = threading.Thread(target=self._listen_sse, daemon=True)
        self._sse_thread.start()

        if not self._endpoint_ready.wait(timeout=10):
            if self._sse_error:
                raise RuntimeError(f"Nie udało się otworzyć strumienia MCP: {self._sse_error}")
            raise RuntimeError("HuskyLens nie przekazał adresu sesji MCP przez SSE.")

        response = self._send_request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "HuskyLens-Adafruit", "version": "1.0.0"},
            },
        )
        if "error" in response:
            raise RuntimeError(f"Inicjalizacja MCP nie powiodła się: {response['error']}")
        self._send_notification("notifications/initialized")

    def _listen_sse(self):
        try:
            with self._sse_session.get(
                f"{self.server_url}/sse",
                headers={"Accept": "text/event-stream"},
                stream=True,
                timeout=(5, None),
            ) as response:
                response.raise_for_status()
                self._sse_response = response
                data_lines = []

                for line in response.iter_lines(decode_unicode=True):
                    if self._stop.is_set():
                        break
                    if isinstance(line, bytes):
                        line = line.decode("utf-8", errors="replace")
                    if not line:
                        if data_lines:
                            self._handle_sse_data("\n".join(data_lines))
                            data_lines.clear()
                        continue
                    if line.startswith("data:"):
                        data_lines.append(line[5:].lstrip())
        except (requests.RequestException, OSError) as exc:
            self._sse_error = exc
            self._endpoint_ready.set()

    def _handle_sse_data(self, data):
        if data == "[DONE]":
            return

        try:
            message = json.loads(data)
        except ValueError:
            if data.startswith("/message") or "/message?" in data:
                self._message_url = urljoin(f"{self.server_url}/", data)
                self._endpoint_ready.set()
            return

        if isinstance(message, dict) and "id" in message:
            with self._lock:
                pending = self._pending.get(message["id"])
            if pending:
                pending["response"] = message
                pending["event"].set()

    def _send_request(self, method, params=None, timeout=15):
        if not self._message_url:
            raise RuntimeError("Brak połączenia z sesją MCP.")

        with self._lock:
            self._request_id += 1
            request_id = self._request_id
            pending = {"event": threading.Event(), "response": None}
            self._pending[request_id] = pending

        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {},
        }
        try:
            response = self._session.post(
                self._message_url,
                json=payload,
                headers={"Accept": "application/json, text/event-stream"},
                timeout=10,
            )
            response.raise_for_status()
            if not pending["event"].wait(timeout=timeout):
                raise TimeoutError(f"Brak odpowiedzi MCP dla metody {method}.")
            return pending["response"] or {}
        finally:
            with self._lock:
                self._pending.pop(request_id, None)

    def _send_notification(self, method, params=None):
        response = self._session.post(
            self._message_url,
            json={"jsonrpc": "2.0", "method": method, "params": params or {}},
            timeout=10,
        )
        response.raise_for_status()

    def get_recognition_result(self):
        return self._send_request(
            "tools/call",
            {
                "name": "get_recognition_result",
                "arguments": {"operation": "get_result"},
            },
        )

    def close(self):
        self._stop.set()
        if self._sse_response is not None:
            self._sse_response.close()
        if self._sse_thread is not None:
            self._sse_thread.join(timeout=2)
        self._session.close()
        self._sse_session.close()


def without_image_data(value):
    if isinstance(value, dict):
        return {
            key: without_image_data(item)
            for key, item in value.items()
            if key.lower() not in IMAGE_KEYS
        }
    if isinstance(value, list):
        return [without_image_data(item) for item in value]
    return value


def connect_mqtt():
    if not AIO_KEY:
        raise RuntimeError("Ustaw klucz w zmiennej środowiskowej ADAFRUIT_IO_KEY.")

    connected = threading.Event()
    connection_error = []
    client = mqtt_client.Client(
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
        client_id=f"huskylens-{uuid.uuid4().hex[:8]}",
    )
    client.username_pw_set(AIO_USERNAME, AIO_KEY)
    client.tls_set()

    def on_connect(_client, _userdata, _flags, reason_code, _properties):
        if reason_code.is_failure:
            connection_error.append(reason_code)
        else:
            connected.set()

    client.on_connect = on_connect
    client.connect("io.adafruit.com", 8883, keepalive=60)
    client.loop_start()

    if not connected.wait(timeout=10):
        client.loop_stop()
        client.disconnect()
        if connection_error:
            raise RuntimeError(f"Połączenie z Adafruit IO nie powiodło się: {connection_error[0]}")
        raise RuntimeError("Przekroczono czas oczekiwania na połączenie z Adafruit IO.")
    return client


def main():
    mqtt = None
    camera = HuskyLensMCPClient(CAMERA_URL)

    try:
        mqtt = connect_mqtt()
        camera.connect()
        print(f"Połączono. Odczyt wyników z {CAMERA_URL}")

        last_payload = None
        last_publish = 0.0
        while True:
            response = camera.get_recognition_result()

            if "error" in response:
                print(f"Błąd MCP: {response['error']}")
            else:
                result = response.get("result", {})
                payload = json.dumps(
                    without_image_data(result),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                now = time.monotonic()

                if payload != last_payload and now - last_publish >= MIN_PUBLISH_INTERVAL:
                    if len(payload.encode("utf-8")) > MAX_FEED_PAYLOAD_BYTES:
                        print("Wynik przekracza limit feedu Adafruit IO (1024 bajty); pominięto.")
                    else:
                        info = mqtt.publish(FEED_TOPIC, payload, qos=0)
                        if info.rc == mqtt_client.MQTT_ERR_SUCCESS:
                            info.wait_for_publish(timeout=10)
                            if info.is_published():
                                print(f"Wysłano do {FEED_TOPIC}: {payload}")
                                last_payload = payload
                                last_publish = time.monotonic()
                            else:
                                print("Nie potwierdzono publikacji MQTT.")
                        else:
                            print(f"Błąd publikacji MQTT: {info.rc}")

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\nZatrzymano.")
    except (OSError, requests.RequestException, RuntimeError, TimeoutError) as exc:
        print(f"Błąd: {exc}")
    finally:
        camera.close()
        if mqtt is not None:
            mqtt.loop_stop()
            mqtt.disconnect()


if __name__ == "__main__":
    main()
