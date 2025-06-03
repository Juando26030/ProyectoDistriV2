import zmq
import threading
import os
import time
import json

from src.dti.jsonUtils import cargar_registro, guardar_registro, cargar_recursos, guardar_recursos
from src.utils.config import CONFIG  # Importar configuración global


class GestorDTI:
    def __init__(self,
                 id_instancia,
                 broker_host='127.0.0.1',
                 es_central=False):
        # Identidad y puertos
        self.id = id_instancia
        self.broker_host = broker_host
        self.frontend_port = CONFIG["BROKER_FRONTEND"]  # Desde configuración
        self.backend_port = CONFIG["BROKER_BACKEND"]  # Desde configuración
        self.health_port = CONFIG["PUERTO_HEALTH"]  # Desde configuración
        self.es_central = es_central

        # Persistencia: JSON de registro y recursos
        os.makedirs('data', exist_ok=True)
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.data_file = os.path.join(BASE_DIR, '..', 'data', f'registro_{self.id}.json')
        # Cargar registros y recursos
        self.programas_registrados = cargar_registro(self.data_file)
        self.recursos_file = os.path.join(BASE_DIR, '..', 'data', f'recursos_{self.id}.json')

        self.recursos_disponibles = cargar_recursos(self.recursos_file)

        # Contexto ZMQ y locking de recursos
        self.context = zmq.Context()
        self.lock = threading.Lock()

    def imprimir_estado(self):
        """Imprime el estado inicial del DTI."""
        rol = 'CENTRAL' if self.es_central else 'RÉPLICA'
        print(f"[DTI-{self.id}] Iniciado como {rol} conectando a broker {self.broker_host}:{self.backend_port}")

    def guardar_estado(self):
        """Guarda el estado actual del registro en archivos JSON."""
        guardar_registro(self.data_file, self.programas_registrados)
        guardar_recursos(self.recursos_file, self.recursos_disponibles)
        print(f"[DTI-{self.id}] Estado guardado en {self.data_file} y {self.recursos_file}")

    def manejar_registro(self, facultad, datos_programa):
        """Manejo de registro de programas a nivel de facultad."""
        with self.lock:
            if facultad not in self.programas_registrados:
                self.programas_registrados[facultad] = []
            if datos_programa:
                self.programas_registrados[facultad].append(datos_programa)
                print(f"[DTI-{self.id}] Registrado programa '{datos_programa.get('programa')}' en '{facultad}'")
            else:
                print(f"[DTI-{self.id}] Facultad '{facultad}' registrada sin programas.")
            self.guardar_estado()

    def enviar_estado_healthcheck(self):
        """
        Envía mensajes al HealthCheck:
        - Estado inicial `ready`.
        - Mensajes periódicos `heartbeat` cada 5 segundos.
        """
        health_socket = self.context.socket(zmq.PUSH)
        endpoint_health = f"tcp://{self.broker_host}:{self.health_port}"
        health_socket.connect(endpoint_health)

        # Enviar mensaje 'ready'
        msg_ready = {'worker_id': f'DTI-{self.id}', 'status': 'ready'}
        print(f"[DTI-{self.id}] Enviando estado 'ready' al HealthCheck: {msg_ready}")
        health_socket.send_json(msg_ready)

        # Iniciar envío periódico de 'heartbeat'
        def heartbeat():
            while True:
                time.sleep(5)
                msg_heartbeat = {'worker_id': f'DTI-{self.id}', 'status': 'heartbeat'}
                print(f"[DTI-{self.id}] Enviando heartbeat al HealthCheck: {msg_heartbeat}")
                health_socket.send_json(msg_heartbeat)

        threading.Thread(target=heartbeat, daemon=True).start()

    
    def iniciar_como_worker(self):
        """Inicializa el DTI como servidor directo para las facultades."""
        # Socket REP para comunicarse directamente con las facultades
        servidor = self.context.socket(zmq.REP)
        
        if self.es_central:
            puerto = CONFIG["DTI_CENTRAL_PORT"]
        else:
            # Seleccionar el puerto de la réplica según el ID
            idx = int(self.id) - 1  # Asumiendo que las réplicas tienen ID 1, 2, etc.
            if idx < 0 or idx >= len(CONFIG["DTI_REPLICA_PORTS"]):
                print(f"[DTI-{self.id}] Error: ID de réplica fuera de rango")
                return
            puerto = CONFIG["DTI_REPLICA_PORTS"][idx]
        
        servidor.bind(f"tcp://*:{puerto}")
        
        # Enviar estado HealthCheck
        self.enviar_estado_healthcheck()
        
        self.imprimir_estado()
        print(f"[DTI-{self.id}] Servidor escuchando en puerto {puerto}")
        
        while True:
            try:
                # Recibir mensaje de la facultad
                start_time = time.time()
                mensaje = servidor.recv_json()
                
                print(f"[DTI-{self.id}] 📥 Mensaje recibido: {mensaje}")
                tipo = mensaje.get('tipo')
                
                # Procesar el mensaje
                if tipo == 'descubrimiento':
                    rol = 'central' if self.es_central else 'replica'
                    reply = {'rol': rol}
                    
                elif tipo == 'ping':
                    reply = {'status': 'ok'}
                    
                elif tipo == 'registro_facultad':
                    facultad = mensaje.get('facultad')
                    if facultad:
                        self.manejar_registro(facultad, None)
                        reply = {'status': f"Facultad '{facultad}' registrada."}
                    else:
                        reply = {'status': 'Error: datos incompletos'}
                        
                elif tipo == 'programa':
                    fac = mensaje.get('facultad')
                    info = mensaje.get('programa_info')
                    if fac and info:
                        self.manejar_registro(fac, info)
                        reply = {'status': 'Programa registrado'}
                    else:
                        reply = {'status': 'Error: datos incompletos'}
                        
                elif tipo == 'solicitudPrograma':
                    fac = mensaje.get('facultad')
                    if fac:
                        aulas = mensaje.get("numeroAulas", 0)
                        labs = mensaje.get("numeroLaboratorios", 0)
                        aceptado, detalle = self.evaluar_solicitud_recursos(aulas, labs)
                        if aceptado:
                            self.manejar_registro(fac, {
                                "programa": mensaje.get("nombreProgramaSolicitante"),
                                "aulas": aulas,
                                "laboratorios": labs
                            })
                            reply = {"status": "Aceptado", "detalle": detalle}
                        else:
                            reply = {"status": "Rechazado", "razon": detalle}
                    else:
                        reply = {'status': 'Rechazado', 'razon': 'Facultad no especificada'}
                        
                elif tipo == 'registro':
                    datos = mensaje.get('registro', {})
                    for f, programas in datos.items():
                        for p in programas:
                            self.manejar_registro(f, p)
                    reply = {'status': 'Réplicas sincronizadas'}
                    
                else:
                    reply = {'status': 'Tipo desconocido'}
                    
                # Enviar respuesta
                servidor.send_json(reply)
                end_time = time.time()
                print(f"[DTI-{self.id}] ⏱️ Tiempo de procesamiento: {end_time - start_time:.3f} segundos")
                
            except Exception as e:
                print(f"[DTI-{self.id}] ❌ Error inesperado: {e}")
                try:
                    servidor.send_json({'status': 'Error', 'mensaje': str(e)})
                except:
                    pass


    def iniciar(self):
        """Inicia el DTI como worker."""
        self.iniciar_como_worker()

    def evaluar_solicitud_recursos(self, aulas_solicitadas, labs_solicitados):
        """Evalúa si hay suficientes recursos para una solicitud."""
        with self.lock:
            aulas_disp = self.recursos_disponibles["aulas"]
            labs_disp = self.recursos_disponibles["laboratorios"]

            if aulas_solicitadas + labs_solicitados > aulas_disp + labs_disp:
                return False, "No hay suficientes recursos en total"

            if labs_solicitados <= labs_disp:
                self.recursos_disponibles["aulas"] -= aulas_solicitadas
                self.recursos_disponibles["laboratorios"] -= labs_solicitados
            elif labs_solicitados > labs_disp and (labs_solicitados - labs_disp) <= aulas_disp:
                faltan = labs_solicitados - labs_disp
                self.recursos_disponibles["laboratorios"] = 0
                self.recursos_disponibles["aulas"] -= (aulas_solicitadas + faltan)
            else:
                return False, "No hay suficientes laboratorios ni aulas móviles"

            guardar_recursos(self.recursos_file, self.recursos_disponibles)
            return True, "Recursos asignados"


def iniciar_dti_pool(broker_host='127.0.0.1'):
    """
    Inicializa un pool de DTIs con una instancia central y dos réplicas.
    """
    instancias = []
    for i in range(3):
        gestor = GestorDTI(
            id_instancia=i,
            broker_host=broker_host,
            es_central=(i == 0)
        )
        instancias.append(gestor)

    for g in instancias:
        threading.Thread(target=g.iniciar, daemon=True).start()

    print("[DTI] Pool de instancias iniciado. Ctrl+C para salir.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[DTI] Terminando...")


if __name__ == '__main__':
    iniciar_dti_pool()