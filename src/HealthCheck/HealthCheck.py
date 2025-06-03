import zmq
import time
import threading
import socket
import json
from src.utils.config import CONFIG


class HealthMonitor:
    """Health Checker para monitorear el Broker y los DTIs."""

    def __init__(self, broker_host="127.0.0.1"):
        self.context = zmq.Context()
        self.broker_host = broker_host
        self.health_port = CONFIG["PUERTO_HEALTH"]
        self.broker_health_port = CONFIG["BROKER_HEALTH_CHECK"]
        self.timeout = CONFIG.get("TIMEOUT_QUORUM", 15)
        self.last_heartbeat_time = None
        self.running = True
        self.health_socket = None
        self.dti_activo = None  # ID del DTI activo actual

    def stop(self):
        print("[HealthCheck] Deteniendo monitoreo...")
        self.running = False
        if self.health_socket:
            self.health_socket.close()
        self.context.term()

    def _notify_dti_change(self, message):
    
        discovery_socket = self.context.socket(zmq.PUSH)
        try:
            endpoint = f"tcp://{self.broker_host}:{self.broker_health_port}"
            discovery_socket.connect(endpoint)
            discovery_socket.send_json(message)
            print(f"[HealthCheck] ⏩ Mensaje enviado al servicio de descubrimiento: {message}")
        except Exception as e:
            print(f"[HealthCheck] ❌ Error notificando al servicio de descubrimiento: {e}")
        finally:
            discovery_socket.close()

    def monitor_health(self):
        self.health_socket = self.context.socket(zmq.PULL)

        try:
            endpoint = f"tcp://*:{self.health_port}"
            self.health_socket.bind(endpoint)
            print(f"[HealthCheck] ✅ Escuchando en puerto {self.health_port}...")

            while self.running:
                try:
                    if self.health_socket.poll(timeout=1000):
                        msg = self.health_socket.recv_json()
                        status = msg.get("status")
                        worker_id = msg.get("worker_id", "desconocido")

                        if status == "ready":
                            if not self.dti_activo:
                                self.dti_activo = worker_id
                                self.last_heartbeat_time = time.time()
                                print(f"[HealthCheck] ✅ DTI activo establecido: {worker_id}")
                                self._notify_broker(msg)
                            elif worker_id == self.dti_activo:
                                self.last_heartbeat_time = time.time()
                                print(f"[HealthCheck] 🔄 Ready del DTI actual ({worker_id}) recibido.")
                            else:
                                print(f"[HealthCheck] ℹ️ {worker_id} está listo pero se ignora. DTI activo: {self.dti_activo}")

                        elif status == "heartbeat":
                            if worker_id == self.dti_activo:
                                self.last_heartbeat_time = time.time()
                                print(f"[HealthCheck] 🔄 Heartbeat del DTI activo: {worker_id}")
                            else:
                                print(f"[HealthCheck] ℹ️ Heartbeat de {worker_id} ignorado (DTI activo: {self.dti_activo})")

                        else:
                            print(f"[HealthCheck] ❓ Mensaje desconocido: {msg}")

                    # Verificación de timeout
                    if self.dti_activo and self.last_heartbeat_time:
                        if time.time() - self.last_heartbeat_time > self.timeout:
                            print(f"[HealthCheck] ⚠️ Timeout del DTI {self.dti_activo}. Iniciando failover...")
                            replicas = CONFIG.get("DTI_REPLICA_PORTS", [])
                            if replicas:
                                nuevo_puerto = replicas[0]
                                nuevo_dti = f"tcp://{self.broker_host}:{nuevo_puerto}"
                                failover_msg = {
                                    "tipo": "cambio_dti",
                                    "nuevo_dti": nuevo_dti
                                }
                                self._notify_broker(failover_msg)
                                self.dti_activo = f"REPLICA-{nuevo_puerto}"
                                self.last_heartbeat_time = time.time()
                                print(f"[HealthCheck] ✅ Failover a réplica en puerto {nuevo_puerto}")
                            else:
                                print("[HealthCheck] ❌ No hay réplicas disponibles para failover.")
                except Exception as e:
                    print(f"[HealthCheck] ❌ Error en el monitoreo: {e}")
                    self.running = False
        finally:
            self.stop()

    def start_monitoring(self):
        monitor_thread = threading.Thread(target=self.monitor_health, daemon=True)
        monitor_thread.start()
        return monitor_thread


if __name__ == "__main__":
    health_monitor = HealthMonitor()
    print("[HealthCheck] Iniciando monitoreo...")
    try:
        monitor_thread = health_monitor.start_monitoring()
        while monitor_thread.is_alive():
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("[HealthCheck] Monitoreo detenido manualmente.")
    finally:
        health_monitor.stop()
