import zmq
import threading
import time
import json
from src.utils.config import CONFIG

class ServicioDescubrimiento:
    def __init__(self):
        self.ctx = zmq.Context()
        self.puerto_servicio = CONFIG["BROKER_FRONTEND"]  # Reutilizamos este puerto
        self.dti_activo = None
        self.dtis_disponibles = {
            "central": f"tcp://127.0.0.1:{CONFIG['DTI_CENTRAL_PORT']}",
            "replicas": [f"tcp://127.0.0.1:{puerto}" for puerto in CONFIG["DTI_REPLICA_PORTS"]]
        }
        self.health_port = CONFIG["BROKER_HEALTH_CHECK"]
        self.running = True

    def servicio_descubrimiento(self):
        socket_servicio = self.ctx.socket(zmq.REP)
        socket_servicio.bind(f"tcp://*:{self.puerto_servicio}")
        
        print(f"[DESCUBRIMIENTO] 🚀 Servicio de descubrimiento escuchando en puerto {self.puerto_servicio}")
        
        while self.running:
            try:
                msg = socket_servicio.recv_json()
                tipo = msg.get("tipo", "")
                
                if tipo == "descubrimiento":
                    respuesta = {
                        "status": "ok",
                        "dti_activo": self.dti_activo or self.dtis_disponibles["central"],
                        "dtis_disponibles": self.dtis_disponibles
                    }
                    socket_servicio.send_json(respuesta)
                    print(f"[DESCUBRIMIENTO] 📤 Enviando información de DTIs disponibles")
                else:
                    socket_servicio.send_json({"status": "error", "mensaje": "Comando desconocido"})
            except Exception as e:
                print(f"[DESCUBRIMIENTO] ❌ Error en servicio: {e}")
                if socket_servicio:
                    socket_servicio.send_json({"status": "error", "mensaje": str(e)})

    def health_check(self):
        monitor = self.ctx.socket(zmq.PULL)
        monitor.bind(f"tcp://*:{self.health_port}")
        print(f"[DESCUBRIMIENTO] 🩺 Health-check escuchando en puerto {self.health_port}")

        while self.running:
            try:
                msg = monitor.recv_json()
                tipo = msg.get("tipo")
                
                if tipo == "cambio_dti":
                    nuevo_dti = msg.get("nuevo_dti")
                    if nuevo_dti:
                        self.dti_activo = nuevo_dti
                        print(f"[DESCUBRIMIENTO] 🔁 DTI principal actualizado: {nuevo_dti}")
                elif msg.get("status") == "ready":
                    worker_id = msg.get("worker_id", "desconocido")
                    print(f"[DESCUBRIMIENTO] ✅ Worker registrado: {worker_id}")

            except Exception as e:
                print(f"[DESCUBRIMIENTO] ❌ Error en health-check: {e}")

    def start(self):
        threading.Thread(target=self.health_check, daemon=True).start()
        threading.Thread(target=self.servicio_descubrimiento, daemon=True).start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[DESCUBRIMIENTO] Deteniendo servicio...")
            self.running = False

if __name__ == "__main__":
    servicio = ServicioDescubrimiento()
    servicio.start()