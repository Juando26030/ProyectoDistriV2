# src/facultades/gestorFacultades.py

import zmq
import socket
import time
import json
from multiprocessing import Process

# Parámetros del broker
BROKER_HOST = "25.54.14.132"
FRONTEND_PORT = 5555  # puerto ROUTER del broker


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

        # Socket REQ para el servicio de descubrimiento
        socketDescubrimiento = context.socket(zmq.REQ)
        socketDescubrimiento.connect(f"tcp://{BROKER_HOST}:{FRONTEND_PORT}")  # Reutilizamos estas constantes
        
        # Solicitar DTI activo
        socketDescubrimiento.send_json({"tipo": "descubrimiento"})
        respuesta_descubrimiento = socketDescubrimiento.recv_json()
        dti_activo = respuesta_descubrimiento.get("dti_activo")
        
        if not dti_activo:
            print(f"La facultad {nombre} no puede obtener información de DTI activo")
            return
            
        # Socket REQ para comunicarse directamente con el DTI
        socketDTI = context.socket(zmq.REQ)
        socketDTI.setsockopt(zmq.RCVTIMEO, 30000)  # Timeout de 30 segundos
        socketDTI.connect(dti_activo)
        
        print(f"La facultad {nombre} está escuchando a sus programas en puerto {puerto_facultad}")
        print(f"La facultad {nombre} está conectada al DTI en {dti_activo}")

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
                
                # Intentar reconectar con el servicio de descubrimiento para obtener un nuevo DTI
                try:
                    socketDescubrimiento.send_json({"tipo": "descubrimiento"})
                    respuesta_descubrimiento = socketDescubrimiento.recv_json()
                    nuevo_dti = respuesta_descubrimiento.get("dti_activo")
                    
                    if nuevo_dti and nuevo_dti != dti_activo:
                        dti_activo = nuevo_dti
                        socketDTI.close()
                        socketDTI = context.socket(zmq.REQ)
                        socketDTI.setsockopt(zmq.RCVTIMEO, 30000)
                        socketDTI.connect(dti_activo)
                        print(f"La facultad {nombre} reconectó con DTI en {dti_activo}")
                except Exception as e:
                    print(f"Error al reconectar: {e}")

            # Enviar respuesta al programa
            socketProgramas.send_json(respuestaDTI)

    except KeyboardInterrupt:
        print(f"\nSe interrumpió la facultad {nombre}")
    except Exception as e:
        print(f"La facultad {nombre} presenta un error inesperado: {e}")
    finally:
        socketProgramas.close()
        socketDTI.close()
        socketDescubrimiento.close()
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