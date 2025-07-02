import requests

def enviar_json(nombre: str, contenido: list):
    url = "http://localhost:8081/api/atencion/v1/archivosjson"

    payload = {
        "nombre": nombre,
        "contenido": contenido
    }

    try:
        res = requests.post(url, json=payload)
        print("Código de respuesta:", res.status_code)
        print("Respuesta:", res.text)
        return res.status_code, res.text
    except requests.exceptions.RequestException as e:
        print("Error al hacer la solicitud:", e)
        return None, str(e)
