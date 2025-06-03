# utils/config.py
CONFIG = {
    "TOTAL_NODOS": 13,  # 10 facultades + 3 DTIs
    "TIMEOUT_QUORUM": 15,   # Segundos
    "PUERTO_HEALTH": 5557,  # Para latidos PUSH
    "BROKER_FRONTEND": 5555,  # Broker ↔ Facultades
    "BROKER_BACKEND": 5556,  # Broker ↔ DTI
    "BROKER_HEALTH_CHECK": 5558,  # HealthMonitor → Broker
    "DTI_CENTRAL_PORT": 5550,      # Puerto del DTI central
    "DTI_REPLICA_PORTS": [5551, 5552],  # Puertos réplicas
    "ESTADO_COMPARTIDO": "data/registro_compartido.json",  # Registro compartido por DTIs
}