# gestorProgramas.py
import zmq
import time
import socket
import random
from multiprocessing import Process

programas = {
    'Psicologia': {'puerto': 12001, 'facultad': 'Ciencias Sociales', 'puertoFacultad': 12000,
                   'IPFacultad': '127.0.0.1'},
    'Sociologia': {'puerto': 12002, 'facultad': 'Ciencias Sociales', 'puertoFacultad': 12000,
                   'IPFacultad': '127.0.0.1'},
    'Trabajo Social': {'puerto': 12003, 'facultad': 'Ciencias Sociales', 'puertoFacultad': 12000,
                       'IPFacultad': '127.0.0.1'},
    'Antropologia': {'puerto': 12004, 'facultad': 'Ciencias Sociales', 'puertoFacultad': 12000,
                     'IPFacultad': '127.0.0.1'},
    'Comunicacion': {'puerto': 12005, 'facultad': 'Ciencias Sociales', 'puertoFacultad': 12000,
                     'IPFacultad': '127.0.0.1'},

    'Biologia': {'puerto': 3001, 'facultad': 'Ciencias Naturales', 'puertoFacultad': 3000, 'IPFacultad': '127.0.0.1'},
    'Quimica': {'puerto': 3002, 'facultad': 'Ciencias Naturales', 'puertoFacultad': 3000, 'IPFacultad': '127.0.0.1'},
    'Fisica': {'puerto': 3003, 'facultad': 'Ciencias Naturales', 'puertoFacultad': 3000, 'IPFacultad': '127.0.0.1'},
    'Geologia': {'puerto': 3004, 'facultad': 'Ciencias Naturales', 'puertoFacultad': 3000, 'IPFacultad': '127.0.0.1'},
    'Ciencias Ambientales': {'puerto': 3005, 'facultad': 'Ciencias Naturales', 'puertoFacultad': 3000,
                             'IPFacultad': '127.0.0.1'},

    'Ingenieria Civil': {'puerto': 4001, 'facultad': 'Ingenieria', 'puertoFacultad': 4000, 'IPFacultad': '127.0.0.1'},
    'Ingenieria Electronica': {'puerto': 4002, 'facultad': 'Ingenieria', 'puertoFacultad': 4000,
                               'IPFacultad': '127.0.0.1'},
    'Ingenieria de Sistemas': {'puerto': 4003, 'facultad': 'Ingenieria', 'puertoFacultad': 4000,
                               'IPFacultad': '127.0.0.1'},
    'Ingenieria Mecanica': {'puerto': 4004, 'facultad': 'Ingenieria', 'puertoFacultad': 4000,
                            'IPFacultad': '127.0.0.1'},
    'Ingenieria Industrial': {'puerto': 4005, 'facultad': 'Ingenieria', 'puertoFacultad': 4000,
                              'IPFacultad': '127.0.0.1'},

    'Medicina General': {'puerto': 5001, 'facultad': 'Medicina', 'puertoFacultad': 5000, 'IPFacultad': '25.54.13.249'},
    'Enfermeria': {'puerto': 5002, 'facultad': 'Medicina', 'puertoFacultad': 5000, 'IPFacultad': '25.54.13.249'},
    'Odontologia': {'puerto': 5003, 'facultad': 'Medicina', 'puertoFacultad': 5000, 'IPFacultad': '25.54.13.249'},
    'Farmacia': {'puerto': 5004, 'facultad': 'Medicina', 'puertoFacultad': 5000, 'IPFacultad': '25.54.13.249'},
    'Terapia Fisica': {'puerto': 5005, 'facultad': 'Medicina', 'puertoFacultad': 5000, 'IPFacultad': '25.54.13.249'},

    'Derecho Penal': {'puerto': 6001, 'facultad': 'Derecho', 'puertoFacultad': 6000, 'IPFacultad': '127.0.0.1'},
    'Derecho Civil': {'puerto': 6002, 'facultad': 'Derecho', 'puertoFacultad': 6000, 'IPFacultad': '127.0.0.1'},
    'Derecho Internacional': {'puerto': 6003, 'facultad': 'Derecho', 'puertoFacultad': 6000, 'IPFacultad': '127.0.0.1'},
    'Derecho Laboral': {'puerto': 6004, 'facultad': 'Derecho', 'puertoFacultad': 6000, 'IPFacultad': '127.0.0.1'},
    'Derecho Constitucional': {'puerto': 6005, 'facultad': 'Derecho', 'puertoFacultad': 6000,
                               'IPFacultad': '127.0.0.1'},

    'Bellas Artes': {'puerto': 7001, 'facultad': 'Artes', 'puertoFacultad': 7000, 'IPFacultad': '25.54.13.249'},
    'Musica': {'puerto': 7002, 'facultad': 'Artes', 'puertoFacultad': 7000, 'IPFacultad': '25.54.13.249'},
    'Teatro': {'puerto': 7003, 'facultad': 'Artes', 'puertoFacultad': 7000, 'IPFacultad': '25.54.13.249'},
    'Danza': {'puerto': 7004, 'facultad': 'Artes', 'puertoFacultad': 7000, 'IPFacultad': '25.54.13.249'},
    'Diseño Grafico': {'puerto': 7005, 'facultad': 'Artes', 'puertoFacultad': 7000, 'IPFacultad': '25.54.13.249'},

    'Educacion Primaria': {'puerto': 8001, 'facultad': 'Educacion', 'puertoFacultad': 8000, 'IPFacultad': '127.0.0.1'},
    'Educacion Secundaria': {'puerto': 8002, 'facultad': 'Educacion', 'puertoFacultad': 8000,
                             'IPFacultad': '127.0.0.1'},
    'Educacion Especial': {'puerto': 8003, 'facultad': 'Educacion', 'puertoFacultad': 8000, 'IPFacultad': '127.0.0.1'},
    'Psicopedagogia': {'puerto': 8004, 'facultad': 'Educacion', 'puertoFacultad': 8000, 'IPFacultad': '127.0.0.1'},
    'Administracion Educativa': {'puerto': 8005, 'facultad': 'Educacion', 'puertoFacultad': 8000,
                                 'IPFacultad': '127.0.0.1'},

    'Administracion de Empresas': {'puerto': 9001, 'facultad': 'Ciencias Economicas', 'puertoFacultad': 9000,
                                   'IPFacultad': '127.0.0.1'},
    'Contabilidad': {'puerto': 9002, 'facultad': 'Ciencias Economicas', 'puertoFacultad': 9000,
                     'IPFacultad': '127.0.0.1'},
    'Economia': {'puerto': 9003, 'facultad': 'Ciencias Economicas', 'puertoFacultad': 9000, 'IPFacultad': '127.0.0.1'},
    'Mercadotecnia': {'puerto': 9004, 'facultad': 'Ciencias Economicas', 'puertoFacultad': 9000,
                      'IPFacultad': '127.0.0.1'},
    'Finanzas': {'puerto': 9005, 'facultad': 'Ciencias Economicas', 'puertoFacultad': 9000, 'IPFacultad': '127.0.0.1'},

    'Arquitectura': {'puerto': 10001, 'facultad': 'Arquitectura', 'puertoFacultad': 10000, 'IPFacultad': '127.0.0.1'},
    'Urbanismo': {'puerto': 10002, 'facultad': 'Arquitectura', 'puertoFacultad': 10000, 'IPFacultad': '127.0.0.1'},
    'Diseño de Interiores': {'puerto': 10003, 'facultad': 'Arquitectura', 'puertoFacultad': 10000,
                             'IPFacultad': '127.0.0.1'},
    'Paisajismo': {'puerto': 10004, 'facultad': 'Arquitectura', 'puertoFacultad': 10000, 'IPFacultad': '127.0.0.1'},
    'Restauracion de Patrimonio': {'puerto': 10005, 'facultad': 'Arquitectura', 'puertoFacultad': 10000,
                                   'IPFacultad': '127.0.0.1'},

    'Desarrollo de Software': {'puerto': 11001, 'facultad': 'Tecnologia', 'puertoFacultad': 11000,
                               'IPFacultad': '127.0.0.1'},
    'Redes y Telecomunicaciones': {'puerto': 11002, 'facultad': 'Tecnologia', 'puertoFacultad': 11000,
                                   'IPFacultad': '127.0.0.1'},
    'Ciberseguridad': {'puerto': 11003, 'facultad': 'Tecnologia', 'puertoFacultad': 11000, 'IPFacultad': '127.0.0.1'},
    'Inteligencia Artificial': {'puerto': 11004, 'facultad': 'Tecnologia', 'puertoFacultad': 11000,
                                'IPFacultad': '127.0.0.1'},
    'Big Data': {'puerto': 11005, 'facultad': 'Tecnologia', 'puertoFacultad': 11000, 'IPFacultad': '127.0.0.1'}
}



def generarSolicitud():
    # Se genera un número aleatorio de laboratorios entre 2 y 4
    numLaboratorios = random.randint(2, 4)

    # Se genera un número total de solicitudes entre 7 y 10
    totalSolicitudes = random.randint(7, 10)

    # A partir de ese número total se obtienen la cantidad de aulas
    numAulas = totalSolicitudes - numLaboratorios

    return numAulas, numLaboratorios


def obtenerIPLocal():
    # Simplemente se obtiene la dirección actual por el DNS de Google
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def ejecutarPrograma(nombre, config):
    # La idea es que se responda a cada programa en su mismo socket, pero
    # que se comuniquen con la facultad por medio del socket en común

    # Configuración para recibir respuestas
    contextRespuesta = zmq.Context()
    socketRespuesta = contextRespuesta.socket(zmq.REP)
    socketRespuesta.bind(f"tcp://*:{config['puerto']}")

    # Configuración para enviar solicitudes
    contextSolicitud = zmq.Context()
    socketSolicitud = contextSolicitud.socket(zmq.REQ)
    socketSolicitud.connect(f"tcp://{config['IPFacultad']}:{config['puertoFacultad']}")

    try:
        print(f"El programa de {nombre} está escuchando respuestas en puerto {config['puerto']}")
        print(
            f"El programa de {nombre} está conectado a su facultad en en {config['IPFacultad']}:{config['puertoFacultad']}")

        # Acá se genera la solicitud
        aulas, laboratorios = generarSolicitud()
        solicitud = {
            'tipo': 'solicitud',
            'nombrePrograma': nombre,
            'semestre': '2025-1',
            'IPPrograma': obtenerIPLocal(),
            'puertoPrograma': config['puerto'],
            'nombreFacultad': config['facultad'],
            'IPFacultad': config['IPFacultad'],
            'puertoFacultad': config['puertoFacultad'],
            'numeroAulas': aulas,
            'numeroLaboratorios': laboratorios,
            'timestampSolicitudPrograma': time.time()
        }

        print(f"El programa {nombre} está solicitando {aulas} aulas y {laboratorios} laboratorios")
        socketSolicitud.send_json(solicitud)

        # Esperar respuesta de la facultad con una espera de 5000
        if socketRespuesta.poll(5000):
            respuesta = socketRespuesta.recv_json()
            print(f"El programa {nombre}, obtuvo una respuesta {respuesta.get('mensaje', 'Sin mensaje')}")

            # Confirmar recepción a la facultad
            socketRespuesta.send_json({'status': 'OK'})
        else:
            print(f"El programa {nombre} se fue a timeout esperando una respuesta")

    except zmq.ZMQError as e:
        print(f"El programa {nombre} tiene un error: {str(e)}")
    except Exception as e:
        print(f"El programa {nombre} presentó un error inesperado: {str(e)}")
    finally:
        socketSolicitud.close()
        contextSolicitud.term()
        socketRespuesta.close()
        contextRespuesta.term()

        print(f"El proceso asociado al programa {nombre} ha sido terminado")


def inicializarGestor():
    procesos = []

    print("\nGestor de Programas Inicializado")
    print(f"Total de programas a ejecutar: {len(programas)}\n")

    try:
        for nombre, config in programas.items():
            p = Process(target=ejecutarPrograma, args=(nombre, config))
            p.start()
            procesos.append(p)

            print(f"\nSe ha iniciado el programa {nombre} (PID: {p.pid}) en puerto {config['puerto']}")
            time.sleep(0.1)

            # Esperar a que todos los programas terminen
        print("\nEsperando que todos los programas terminen")
        for p in procesos:
            p.join(timeout=10)

    except KeyboardInterrupt:
        print("\nSe ha interrumpido al testor, finalizando todos los procesos")
    finally:
        for p in procesos:
            if p.is_alive():
                p.terminate()
                p.join()

        print("\nFinalización exitosa")


if __name__ == "__main__":
    inicializarGestor()