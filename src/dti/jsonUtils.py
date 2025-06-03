#jsonUtils.py
import json
import os

def cargar_registro(ruta):
    if os.path.exists(ruta):
        try:
            with open(ruta, 'r') as f:
                contenido = f.read().strip()
                if not contenido:
                    return {}
                return json.loads(contenido)
        except Exception as e:
            print(f"Error al cargar JSON desde {ruta}: {e}")
            return {}
    return {}

def guardar_registro(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def cargar_recursos(path):
    if not os.path.exists(path):
        return {"aulas": 599, "laboratorios": 499}
    with open(path, 'r') as f:
        return json.load(f)

def guardar_recursos(path, recursos):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(recursos, f, indent=4)