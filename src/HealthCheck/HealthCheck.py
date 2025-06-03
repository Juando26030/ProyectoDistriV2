import zmq
import time
import threading
import socket
import json
from src.utils.config import CONFIG


class HealthMonitor:
    def __init__(self, broker_host="127.0.0.1"):
        self.context = zmq.Context()
        self.health_socket = None
        self.running = True
        self.dti_activo = None
        self.last_heartbeat_time = None
        self.broker_host = broker_host
        self.health_port = CONFIG["PUERTO_HEALTH"]
        self.timeout = CONFIG.get("TIMEOUT_QUORUM", 15)  # 15 segundos por defecto

        # Mapeo de puertos de las facultades para notificación directa
        self.facultades_ports = {
            'Ciencias Sociales': 12000,
            'Ciencias Naturales': 3000,
            'Ingenieria': 4000,
            'Medicina': 5000,
            'Derecho': 6000,
            'Artes': 7000,
            'Educacion': 8000,
            'Ciencias Economicas': 9000,
            'Arquitectura': 10000,
            'Tecnologia': 11000
        }

        # Puertos para notificaciones de failover
        self.notificacion_ports = {nombre: puerto + 1000 for nombre, puerto in self.facultades_ports.items()}

    def obtenerIPFacultad(self, facultad):
        # Para pruebas locales, todas las facultades están en localhost
        return "127.0.0.1"

    def stop(self):
        print("[HealthCheck] Deteniendo monitoreo...")
        self.running = False
        if self.health_socket:
            self.health_socket.close()
        self.context.term()

    def _notify_failover(self, nuevo_dti):
        """Notifica directamente a todas las facultades sobre el cambio de DTI"""
        for facultad, puerto in self.notificacion_ports.items():
            notif_socket = self.context.socket(zmq.REQ)
            try:
                # Cambiar 127.0.0.1 por la IP real del nodo donde está la facultad
                ip_facultad = self.obtenerIPFacultad(facultad)
                endpoint = f"tcp://{ip_facultad}:{puerto}"
                notif_socket.connect(endpoint)
                notif_socket.send_json({
                    "tipo": "failover_notification",
                    "nuevo_dti": nuevo_dti
                })
                # Esperar respuesta con timeout
                if notif_socket.poll(timeout=1000):
                    notif_socket.recv_json()  # Consumir respuesta
                    print(f"[HealthCheck] ✅ Notificado failover a {facultad}")
            except Exception as e:
                print(f"[HealthCheck] ❌ Error notificando a {facultad}: {e}")
            finally:
                notif_socket.close()

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
                                # Si es el primer DTI activo, notificar a las facultades
                                puerto_dti = CONFIG["DTI_CENTRAL_PORT"]
                                nuevo_dti = f"tcp://{self.broker_host}:{puerto_dti}"
                                self._notify_failover(nuevo_dti)
                            elif worker_id == self.dti_activo:
                                self.last_heartbeat_time = time.time()
                                print(f"[HealthCheck] 🔄 Ready del DTI actual ({worker_id}) recibido.")
                            else:
                                print(
                                    f"[HealthCheck] ℹ️ {worker_id} está listo pero se ignora. DTI activo: {self.dti_activo}")

                        elif status == "heartbeat":
                            if worker_id == self.dti_activo:
                                self.last_heartbeat_time = time.time()
                                print(f"[HealthCheck] 🔄 Heartbeat del DTI activo: {worker_id}")
                            else:
                                print(
                                    f"[HealthCheck] ℹ️ Heartbeat de {worker_id} ignorado (DTI activo: {self.dti_activo})")

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
                                self._notify_failover(nuevo_dti)
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