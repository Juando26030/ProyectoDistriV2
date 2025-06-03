# src/facultades/gestorFacultades.py
import zmq
import socket
import time
import json
import threading
from multiprocessing import Process
from src.utils.config import CONFIG


def obtenerIPLocal():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def procesoFacultad(nombre, puerto_facultad):
    context = zmq.Context()

    try:
        # Socket REP para recibir solicitudes de programas
        socketProgramas = context.socket(zmq.REP)
        socketProgramas.bind(f"tcp://*:{puerto_facultad}")

        # Socket REP para recibir notificaciones del HealthCheck
        socketNotificaciones = context.socket(zmq.REP)
        puerto_notificaciones = puerto_facultad + 1000  # Puerto adicional para notificaciones
        socketNotificaciones.bind(f"tcp://*:{puerto_notificaciones}")

        # Conectar directamente al DTI central sin consultar al broker
        socketDTI = context.socket(zmq.REQ)
        socketDTI.setsockopt(zmq.RCVTIMEO, 30000)  # Timeout de 30 segundos
        dti_activo = f"tcp://127.0.0.1:{CONFIG['DTI_CENTRAL_PORT']}"
        socketDTI.connect(dti_activo)

        print(f"La facultad {nombre} está escuchando a sus programas en puerto {puerto_facultad}")
        print(f"La facultad {nombre} está escuchando notificaciones en puerto {puerto_notificaciones}")
        print(f"La facultad {nombre} está conectada al DTI en {dti_activo}")

        # Thread para recibir notificaciones de failover
        def recibir_notificaciones():
            while True:
                try:
                    msg = socketNotificaciones.recv_json()
                    if msg.get("tipo") == "failover_notification":
                        nuevo_dti = msg.get("nuevo_dti")
                        if nuevo_dti and nuevo_dti != dti_activo:
                            print(f"La facultad {nombre} recibió notificación de cambio de DTI a {nuevo_dti}")
                            nonlocal dti_activo
                            dti_activo = nuevo_dti
                            # Reconectar al nuevo DTI
                            socketDTI.close()
                            socketDTI = context.socket(zmq.REQ)
                            socketDTI.setsockopt(zmq.RCVTIMEO, 30000)
                            socketDTI.connect(dti_activo)
                        socketNotificaciones.send_json({"status": "OK"})
                except Exception as e:
                    print(f"Error en recepción de notificaciones: {e}")
                    time.sleep(1)  # Evitar CPU alta en caso de error continuo

        threading.Thread(target=recibir_notificaciones, daemon=True).start()

        while True:
            # Recibimos la solicitud del programa académico
            solicitud = socketProgramas.recv_json()
            prog = solicitud.get('nombrePrograma')
            print(f"\nLa facultad {nombre} ha recibido una solicitud de {prog}")

            # Empaquetamos la solicitud para el DTI
            solicitudDTI = {
                'tipo': 'solicitudPrograma',
                'facultad': nombre,
                'IPFacultad': obtenerIPLocal(),
                'puertoFacultad': puerto_facultad,
                'nombreProgramaSolicitante': prog,
                'IPPrograma': solicitud.get('IPPrograma'),
                'puertoPrograma': solicitud.get('puertoPrograma'),
                'numeroAulas': solicitud.get('numeroAulas'),
                'numeroLaboratorios': solicitud.get('numeroLaboratorios'),
                'timestampSolicitudPrograma': solicitud.get('timestampSolicitudPrograma'),
                'timestampSolicitudFacultad': time.time()
            }

            # Enviar directamente al DTI
            try:
                socketDTI.send_json(solicitudDTI)
                print(f"La facultad {nombre} envió la solicitud al DTI, esperando respuesta...")

                respuestaDTI = socketDTI.recv_json()
                print(f"La facultad {nombre} obtuvo la respuesta del DTI: {respuestaDTI}")
            except zmq.Again:
                print(f"⏱️ La facultad {nombre} no recibió respuesta del DTI (timeout).")
                respuestaDTI = {"status": "Error", "razon": "Timeout esperando DTI"}
                # Si hay timeout, esperar notificación del HealthCheck

            # Enviar respuesta al programa
            socketProgramas.send_json(respuestaDTI)

    except KeyboardInterrupt:
        print(f"\nSe interrumpió la facultad {nombre}")
    except Exception as e:
        print(f"La facultad {nombre} presenta un error inesperado: {e}")
    finally:
        socketProgramas.close()
        if socketDTI:
            socketDTI.close()
        if socketNotificaciones:
            socketNotificaciones.close()
        context.term()


def inicializarFacultades():
    # Configuración de puertos por facultad
    facultades = {
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

    procesos = []
    print("\nGestor de Facultades Inicializado")
    print(f"Total de facultades a iniciar: {len(facultades)}\n")

    try:
        for nombre, puerto in facultades.items():
            p = Process(target=procesoFacultad, args=(nombre, puerto))
            p.start()
            procesos.append(p)
            print(f"La Facultad {nombre} ha sido iniciada en PID: {p.pid}")
            time.sleep(0.1)

        print("\nSe han iniciado todas las facultades. Esperando solicitudes de programas...\n")
        for p in procesos:
            p.join()

    except KeyboardInterrupt:
        print("\nSe interrumpió el gestor de facultades. Terminando procesos.")
    finally:
        for p in procesos:
            if p.is_alive():
                p.terminate()
                p.join()
        print("Todos los procesos han sido terminados.")
        print("Gestor de facultades finalizado.\n")


if __name__ == "__main__":
    inicializarFacultades()