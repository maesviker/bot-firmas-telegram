import os
import json
import base64
import time
import threading

import requests
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURACIÓN (desde variables de entorno)
# ============================================================

# TOKEN DE TU BOT (dado por BotFather)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # <-- la pondrás en Railway/Docker

if not TELEGRAM_TOKEN:
    raise RuntimeError("La variable de entorno TELEGRAM_TOKEN no está definida")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Datos de la API de Hércules
API_BASE = "https://solutechherculesazf.azurewebsites.net"
API_TOKEN = os.getenv("API_TOKEN", "mvk")  # por si luego te cambian el token

# Webhook: usaremos el propio token como ruta "secreta"
WEBHOOK_SECRET_PATH = TELEGRAM_TOKEN

# Polling a /resultados
RESULTADOS_TIMEOUT = 180       # tiempo máximo total para esperar (segundos)
RESULTADOS_INTERVALO = 3       # cada cuánto reintentar (segundos)

app = Flask(__name__)


# ============================================================
# FUNCIONES PARA TELEGRAM
# ============================================================

def enviar_mensaje(chat_id, texto, parse_mode="Markdown"):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": parse_mode
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print("Error enviando mensaje:", e)


def enviar_foto(chat_id, img_bytes, filename="firma.jpg", caption=None):
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    files = {
        "photo": (filename, img_bytes)
    }
    data = {
        "chat_id": str(chat_id)
    }
    if caption:
        data["caption"] = caption
    try:
        resp = requests.post(url, data=data, files=files, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print("Error enviando foto:", e)


# ============================================================
# FUNCIONES PARA LA API DE HÉRCULES
# ============================================================

def iniciar_consulta(tipo_doc: str, num_doc: str) -> str:
    """
    Paso 1:
      POST /api/IniciarConsulta?token=mvk&mensaje=KVR408
    Body JSON:
      { "token": "mvk", "tipo": 8, "mensaje": "<tipo_doc>,<num_doc>" }
    """
    url = f"{API_BASE}/api/IniciarConsulta"

    params = {
        "token": API_TOKEN,
        "mensaje": "KVR408"   # según tu Postman
    }

    payload = {
        "token": API_TOKEN,
        "tipo": 8,
        "mensaje": f"{tipo_doc},{num_doc}"
    }

    resp = requests.post(url, params=params, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Ajusta si el campo tiene otro nombre
    id_peticion = (
        data.get("idPeticion")
        or data.get("IdPeticion")
        or data.get("id")
    )

    if not id_peticion:
        raise ValueError(
            f"No se encontró idPeticion en la respuesta de IniciarConsulta: {data}"
        )

    return id_peticion


def obtener_resultado(id_peticion: str) -> dict:
    """
    Paso 2:
      GET /api/resultados/{token}/{idPeticion}
    """
    url = f"{API_BASE}/api/resultados/{API_TOKEN}/{id_peticion}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def resultado_listo(data: dict) -> bool:
    """
    Lógica para decidir si ya hay datos finales.
    Aquí asumimos:
      - Tipo == 0  => OK
      - Mensaje no vacío
    """
    tipo = data.get("Tipo")
    mensaje = data.get("Mensaje")
    if tipo == 0 and mensaje:
        return True
    return False


def llamar_api(tipo_doc: str, num_doc: str) -> dict:
    """
    Orquesta todo:
      1) Llama a IniciarConsulta -> idPeticion
      2) Hace polling a /resultados hasta que:
         - esté listo, o
         - se alcance RESULTADOS_TIMEOUT
    """
    id_peticion = iniciar_consulta(tipo_doc, num_doc)

    deadline = time.time() + RESULTADOS_TIMEOUT
    ultimo_data = None

    while time.time() < deadline:
        data = obtener_resultado(id_peticion)
        ultimo_data = data

        # Log de debug opcional:
        print("Respuesta resultados:", json.dumps(data, ensure_ascii=False))

        if data.get("Error") is True:
            raise RuntimeError(f"API devolvió Error en resultados: {data}")

        if resultado_listo(data):
            print("Resultado marcado como LISTO")
            return data

        time.sleep(RESULTADOS_INTERVALO)

    raise TimeoutError(
        f"La consulta con idPeticion {id_peticion} no estuvo lista en "
        f"{RESULTADOS_TIMEOUT} segundos. Última respuesta: {ultimo_data}"
    )


# ============================================================
# PROCESAR RESPUESTA (JSON CON FIRMA)
# ============================================================

def procesar_respuesta_api(data: dict):
    mensaje_raw = data.get("Mensaje", "")

    # Arreglar posibles "\+" que rompen JSON
    if isinstance(mensaje_raw, str):
        mensaje_raw = mensaje_raw.replace("\\+", "+")
        inner = json.loads(mensaje_raw)
    else:
        inner = mensaje_raw

    nombres = inner.get("nombres", "").strip()
    apellidos = inner.get("apellidos", "").strip()
    grupo = inner.get("grupoSanguineo", "").strip()
    sexo = inner.get("sexo", "").strip()
    fecha_nac = inner.get("fechaNacimiento", "").strip()
    lugar_nac = inner.get("lugarNacimiento", "").strip()

    texto = (
        f"👤 *Datos de la persona*\n"
        f"Nombre: {nombres} {apellidos}\n"
        f"Grupo sanguíneo: {grupo}\n"
        f"Sexo: {sexo}\n"
        f"Lugar de nacimiento: {lugar_nac}\n"
        f"Fecha de nacimiento: {fecha_nac}"
    )

    firma_b64 = inner.get("firma")

    if not firma_b64:
        return texto, None, None

    # Quitar prefijo tipo "data:image/xxx;base64,..." si lo hubiera
    if "," in firma_b64:
        firma_b64 = firma_b64.split(",", 1)[1]

    firma_b64 = "".join(firma_b64.split())
    missing_padding = len(firma_b64) % 4
    if missing_padding:
        firma_b64 += "=" * (4 - missing_padding)

    img_bytes = base64.b64decode(firma_b64)

    # Detectar extensión solo por cabecera
    ext = ".bin"
    if img_bytes.startswith(b"GIF8"):
        ext = ".gif"
    elif img_bytes.startswith(b"\x89PNG"):
        ext = ".png"
    elif img_bytes.startswith(b"\xFF\xD8"):
        ext = ".jpg"

    nombre_archivo = (nombres + "_" + apellidos).strip().replace(" ", "_") or "firma"
    nombre_archivo += ext

    return texto, img_bytes, nombre_archivo


# ============================================================
# LÓGICA DEL BOT (TRABAJO PESADO EN UN HILO)
# ============================================================

def procesar_consulta_en_hilo(chat_id: int, tipo_doc: str, num_doc: str):
    try:
        data = llamar_api(tipo_doc, num_doc)
        texto, img_bytes, filename = procesar_respuesta_api(data)

        enviar_mensaje(chat_id, texto)

        if img_bytes is not None:
            enviar_foto(chat_id, img_bytes, filename, caption="Firma asociada")

    except TimeoutError:
        enviar_mensaje(
            chat_id,
            "⏱ La consulta tardó más de 180 segundos y se considera perdida.\n"
            "Por favor vuelve a intentarlo enviando de nuevo el tipo y número de documento."
        )
    except requests.HTTPError as e:
        enviar_mensaje(chat_id, f"❌ Error HTTP al consultar la API:\n`{e}`")
    except Exception as e:
        enviar_mensaje(chat_id, f"❌ Ocurrió un error procesando la solicitud.\n`{e}`")


# ============================================================
# ENDPOINT DEL WEBHOOK DE TELEGRAM
# ============================================================

@app.route(f"/webhook/{WEBHOOK_SECRET_PATH}", methods=["GET", "POST"])
def telegram_webhook():
    # GET: útil para probar en navegador que la ruta existe
    if request.method == "GET":
        return "Webhook OK (GET) - Telegram debería usar POST", 200

    # POST: lo que usa Telegram
    update = request.get_json(force=True)

    message = update.get("message")
    if not message:
        return jsonify(ok=True)

    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    if not text:
        enviar_mensaje(
            chat_id,
            "Por favor envía: `<tipoDocumento> <documento>`\nEjemplo: `CC 71389323`"
        )
        return jsonify(ok=True)

    parts = text.split()
    if len(parts) < 2:
        enviar_mensaje(
            chat_id,
            "Formato inválido.\nUsa: `<tipoDocumento> <documento>`\nEjemplo: `CC 71389323`"
        )
        return jsonify(ok=True)

    tipo_doc = parts[0]
    num_doc = parts[1]

    enviar_mensaje(
        chat_id,
        "🔍 Procesando tu consulta, esto puede tardar hasta 3 minutos..."
    )

    hilo = threading.Thread(
        target=procesar_consulta_en_hilo,
        args=(chat_id, tipo_doc, num_doc),
        daemon=True
    )
    hilo.start()

    return jsonify(ok=True)


@app.route("/", methods=["GET"])
def index():
    return "Bot de consultas de firmas funcionando ✅", 200


# ============================================================
# MAIN LOCAL / PRODUCCIÓN
# ============================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print("Iniciando bot Flask ...")
    print("Ruta de webhook esperada:", f"/webhook/{WEBHOOK_SECRET_PATH}")
    app.run(host="0.0.0.0", port=port, debug=False)
