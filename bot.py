# ------------------------------------------------------------------
# TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "404610113:AAHOPC7xEcuTPtBqQUBdgG09o0iPdCdPFts")
# API_TOKEN = os.getenv("HERCULES_TOKEN", "mvk")  # cambia "mvk" por tu token real si es diferente

# ============================================================
#  BOT TELEGRAM + HERCULES + MySQL (Railway)
#  - Manejo de usuarios y créditos
#  - Registro de mensajes (consultas)
# ============================================================

import os
import json
import base64
import time
import threading
import re
import traceback
from datetime import datetime

import requests
from flask import Flask, request, jsonify

# ------------------ IMPORTS PARA BASE DE DATOS ------------------
from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, String,
    Text, DateTime, ForeignKey, JSON
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
# ----------------------------------------------------------------

# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

# Token del bot de Telegram (es mejor cargarlos desde variables de entorno)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "404610113:AAHOPC7xEcuTPtBqQUBdgG09o0iPdCdPFts")  # pon uno por defecto si quieres

# Token de la API de Hércules (ej: "mvk")
API_TOKEN = os.getenv("HERCULES_TOKEN", "mvk")

# URL base de la API de Telegram
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# URL base de la API de Hércules
API_BASE = "https://solutechherculesazf.azurewebsites.net"

# Ruta "secreta" del webhook; por defecto usamos el token del bot.
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", TELEGRAM_TOKEN)

# Parámetros para el polling de resultados de Hércules
RESULTADOS_TIMEOUT = 180       # segundos máximos esperando la respuesta
RESULTADOS_INTERVALO = 3       # segundos entre cada consulta a /resultados

# Diccionario global para manejar el estado de cada chat (paso a paso)
ESTADOS = {}

# Crear app Flask
app = Flask(__name__)


# ============================================================
# CONFIGURACIÓN BASE DE DATOS (MySQL en Railway)
# ============================================================

# Obtenemos la URL de conexión:
# - Normalmente en Railway se define DATABASE_URL referenciando a MYSQL_URL.
DATABASE_URL = (
    os.getenv("DATABASE_URL")          # referencia a MYSQL_URL
    or os.getenv("MYSQL_URL")          # por si algún día la usas directo
    or "sqlite:///bot_hercules.db"     # fallback local (útil para pruebas)
)

# Si la URL comienza con "mysql://", la convertimos a "mysql+pymysql://"
# para que SQLAlchemy sepa que debe usar el driver pymysql.
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

# Forzamos el charset utf8mb4 (para caracteres especiales)
if "charset=" not in DATABASE_URL:
    separador = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{separador}charset=utf8mb4"

# Crear engine y sesión de SQLAlchemy
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


# ------------------ MODELOS DE BASE DE DATOS ------------------

class Usuario(Base):
    """
    Tabla de usuarios del bot.

    - telegram_id: id del chat de Telegram (único).
    - creditos_total: créditos asignados en total.
    - creditos_usados: cuántos créditos ha gastado.
    """
    __tablename__ = "usuarios"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    username = Column(String(50))
    first_name = Column(String(100))
    last_name = Column(String(100))

    rol = Column(String(20), default="usuario")  # 'usuario', 'admin', etc.
    creditos_total = Column(Integer, default=100)  # créditos iniciales
    creditos_usados = Column(Integer, default=0)

    creado_en = Column(DateTime, default=datetime.utcnow)
    ultima_consulta = Column(DateTime)

    mensajes = relationship("Mensaje", back_populates="usuario")


class Mensaje(Base):
    """
    Tabla de mensajes / consultas realizadas.

    - tipo_servicio: 'firma', 'persona', 'vehiculo_placa', 'propietario_placa', etc.
    - parametros: JSON con los parámetros consultados (doc, placa, etc.)
    - estado: 'pendiente', 'exito', 'error'
    - respuesta_bruta: JSON crudo devuelto por Hércules (opcional)
    """
    __tablename__ = "mensajes"

    id = Column(Integer, primary_key=True, index=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)

    tipo_servicio = Column(String(30), nullable=False)
    parametros = Column(JSON)  # requiere MySQL 5.7+; si no, se puede cambiar a Text
    fecha_peticion = Column(DateTime, default=datetime.utcnow)

    estado = Column(String(20), default="pendiente")
    respuesta_bruta = Column(Text)
    mensaje_error = Column(Text)

    creditos_costo = Column(Integer, default=1)

    usuario = relationship("Usuario", back_populates="mensajes")


def init_db():
    """Crea las tablas si no existen."""
    Base.metadata.create_all(bind=engine)


# Inicializamos la BD al arrancar el módulo
init_db()


# ------------------ HELPERS DE BD / CRÉDITOS ------------------

def get_db():
    """Devuelve una sesión de base de datos."""
    return SessionLocal()


def get_or_create_usuario_from_message(message) -> Usuario:
    """
    A partir del message de Telegram, busca el usuario por telegram_id.
    Si no existe, lo crea con créditos iniciales.
    """
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    username = chat.get("username")
    first_name = chat.get("first_name")
    last_name = chat.get("last_name")

    db = get_db()
    try:
        usuario = db.query(Usuario).filter_by(telegram_id=chat_id).first()
        if not usuario:
            usuario = Usuario(
                telegram_id=chat_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                creditos_total=100,  # créditos iniciales que tendrá cada usuario
            )
            db.add(usuario)
            db.commit()
            db.refresh(usuario)
        return usuario
    finally:
        db.close()


def usuario_tiene_creditos(usuario: Usuario, costo: int = 1) -> bool:
    """
    Verifica si el usuario tiene suficientes créditos disponibles.
    """
    disponibles = usuario.creditos_total - usuario.creditos_usados
    return disponibles >= costo


def registrar_mensaje(usuario: Usuario, tipo_servicio: str, parametros: dict, costo: int = 1):
    """
    Crea un registro en la tabla mensajes en estado 'pendiente'
    y descuenta créditos del usuario.

    Devuelve el id del mensaje. Si no hay créditos suficientes, devuelve None.
    """
    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()

        if not usuario_tiene_creditos(usuario_db, costo):
            return None

        # Descontamos créditos y actualizamos última consulta
        usuario_db.creditos_usados += costo
        usuario_db.ultima_consulta = datetime.utcnow()

        msg = Mensaje(
            usuario_id=usuario_db.id,
            tipo_servicio=tipo_servicio,
            parametros=parametros,
            creditos_costo=costo,
            estado="pendiente",
        )
        db.add(msg)
        db.commit()
        db.refresh(msg)
        return msg.id
    finally:
        db.close()


def actualizar_mensaje_exito(mensaje_id: int, respuesta: dict):
    """Marca un mensaje como 'exito' y guarda la respuesta cruda."""
    db = get_db()
    try:
        msg = db.query(Mensaje).filter_by(id=mensaje_id).first()
        if msg:
            msg.estado = "exito"
            msg.respuesta_bruta = json.dumps(respuesta, ensure_ascii=False)
            db.commit()
    finally:
        db.close()


def actualizar_mensaje_error(mensaje_id: int, error: str):
    """Marca un mensaje como 'error' y guarda el texto del error."""
    db = get_db()
    try:
        msg = db.query(Mensaje).filter_by(id=mensaje_id).first()
        if msg:
            msg.estado = "error"
            msg.mensaje_error = error
            db.commit()
    finally:
        db.close()


def enviar_saldo_usuario(chat_id, usuario: Usuario):
    """
    Envía al usuario su saldo actual de créditos.
    """
    disponibles = usuario.creditos_total - usuario.creditos_usados
    texto = (
        "💰 *Tu saldo de créditos*\n\n"
        f"Créditos totales: *{usuario.creditos_total}*\n"
        f"Créditos usados: *{usuario.creditos_usados}*\n"
        f"Créditos disponibles: *{disponibles}*"
    )
    enviar_mensaje(chat_id, texto)


# ============================================================
# AUXILIARES TELEGRAM
# ============================================================

def enviar_mensaje(chat_id, texto, parse_mode="Markdown", reply_markup=None):
    """
    Envía un mensaje de texto al chat indicado.
    """
    url = f"{TELEGRAM_API_URL}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": parse_mode,
    }

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[ERROR] No se pudo enviar mensaje a Telegram: {e}")


def enviar_foto(chat_id, img_bytes, filename="imagen.jpg", caption=None, parse_mode="Markdown"):
    """
    Envía una foto al chat indicado.
    """
    url = f"{TELEGRAM_API_URL}/sendPhoto"

    files = {
        "photo": (filename, img_bytes),
    }

    data = {
        "chat_id": chat_id,
    }

    if caption:
        data["caption"] = caption
        data["parse_mode"] = parse_mode

    try:
        requests.post(url, data=data, files=files, timeout=20)
    except Exception as e:
        print(f"[ERROR] No se pudo enviar la foto a Telegram: {e}")


def responder_callback_query(callback_query_id, text=None, show_alert=False):
    """
    Confirma a Telegram que el botón fue procesado.
    """
    url = f"{TELEGRAM_API_URL}/answerCallbackQuery"

    payload = {
        "callback_query_id": callback_query_id,
        "show_alert": show_alert,
    }

    if text:
        payload["text"] = text

    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[ERROR] No se pudo responder callback_query: {e}")


# ============================================================
# AUXILIARES HÉRCULES
# ============================================================

def _get_field(data, *names):
    """
    Devuelve el primer campo encontrado en 'data' de la lista de nombres.
    """
    for name in names:
        if name in data:
            return data[name]
    return None


def limpiar_json_anidado(mensaje_raw: str) -> str:
   # """
   # Limpia el string JSON anidado que viene en 'Mensaje' desde Hércules.
   #
   # - Elimina backslashes inválidos como: \ó, \Ñ, \ía, \+, etc.
   # - Mantiene escapes válidos de JSON: \", \\, \n, \t, \uXXXX, etc.
   # """
    if not isinstance(mensaje_raw, str):
        return mensaje_raw

    texto = mensaje_raw

    # Caso especial conocido: O\+  ->  O+
    texto = texto.replace("\\+", "+")

    # Quitamos cualquier barra invertida que NO forme parte
    # de un escape JSON válido.
    texto = re.sub(r'\\(?!["\\/bfnrtu])', '', texto)

    return texto


def iniciar_consulta_hercules(tipo_peticion, mensaje) -> str:
    """
    Paso 1: Iniciar una consulta en Hércules.

    Devuelve:
      - idPeticion (string) si todo fue bien.
      - O levanta RuntimeError con el texto del error.
    """
    url = f"{API_BASE}/api/IniciarConsulta"

    # Estos parámetros (querystring) son obligatorios aunque el body ya tenga el token.
    params = {
        "token": API_TOKEN,
        "mensaje": "KVR408",
    }

    # Cuerpo de la petición (JSON)
    payload = {
        "token": API_TOKEN,
        "tipo": tipo_peticion,
        "mensaje": mensaje,
    }

    resp = requests.post(url, params=params, json=payload, timeout=20)
    resp.raise_for_status()

    data = resp.json()
    print("[DEBUG] Respuesta IniciarConsulta:", data)

    # Buscamos el id de petición en los posibles campos
    id_peticion = (
        data.get("idPeticion")
        or data.get("IdPeticion")
        or data.get("id")
    )

    if id_peticion:
        return id_peticion

    msg = _get_field(data, "mensaje", "Mensaje")
    if msg:
        return msg

    raise RuntimeError(
        f"Error al iniciar petición en Hércules: {data}"
    )


def obtener_resultado_hercules(id_peticion) -> dict:
    """
    Paso 2: Consultar el estado de una petición en Hércules.
    """
    url = f"{API_BASE}/api/resultados/{API_TOKEN}/{id_peticion}"

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    return resp.json()


def llamar_api_generico(tipo_peticion, mensaje) -> dict:
    """
    Orquesta la consulta a Hércules para cualquier tipo de servicio.

    - Llama a IniciarConsulta.
    - Hace polling a /resultados hasta que:
        Tipo 0 -> éxito
        Tipo 1 -> error
        Tipo 2 -> procesando (sigue esperando)
    """
    id_peticion = iniciar_consulta_hercules(tipo_peticion, mensaje)

    deadline = time.time() + RESULTADOS_TIMEOUT
    ultimo_data = None

    while time.time() < deadline:
        data = obtener_resultado_hercules(id_peticion)
        print(f"[DEBUG] Resultado parcial (tipo={tipo_peticion}, mensaje='{mensaje}'):", data)
        ultimo_data = data

        tipo = str(_get_field(data, "tipo", "Tipo"))
        msg = _get_field(data, "mensaje", "Mensaje")
        error_flag = data.get("Error", False)

        if tipo == "0" and msg:
            if error_flag:
                raise RuntimeError(f"Hércules devolvió Error=True: {msg}")
            return data

        if tipo == "1":
            raise RuntimeError(f"Error en la consulta de Hércules: {msg}")

        time.sleep(RESULTADOS_INTERVALO)

    raise TimeoutError(f"Timeout esperando respuesta de Hércules. Último dato: {ultimo_data}")


# --------------- WRAPPERS POR TIPO DE SERVICIO ---------------

def servicio_firma(tipo_doc, num_doc) -> dict:
    """Consulta tipo 8: Firma."""
    tipo_peticion = 8
    mensaje = f"{tipo_doc},{num_doc}"
    return llamar_api_generico(tipo_peticion, mensaje)


def servicio_persona(tipo_doc, num_doc) -> dict:
    """Consulta tipo 5: Persona."""
    tipo_peticion = 5
    mensaje = f"{tipo_doc},{num_doc}"
    return llamar_api_generico(tipo_peticion, mensaje)


def servicio_vehiculo_por_placa(placa, solo_vehiculo=False) -> dict:
    """
    Consulta tipo 1 ó 3:
      - 1: Placa (vehículo + persona).
      - 3: Placa (solo vehículo).
    """
    tipo_peticion = 3 if solo_vehiculo else 1
    mensaje = placa
    return llamar_api_generico(tipo_peticion, mensaje)


def servicio_vehiculo_por_chasis(chasis) -> dict:
    """Consulta tipo 2: Vehículo por chasis."""
    tipo_peticion = 2
    mensaje = chasis
    return llamar_api_generico(tipo_peticion, mensaje)


def servicio_propietario_por_placa(placa) -> dict:
    """Consulta tipo 4: Propietario por placa."""
    tipo_peticion = 4
    mensaje = placa
    return llamar_api_generico(tipo_peticion, mensaje)


# ============================================================
# PROCESAMIENTO / FORMATEO RESPUESTAS
# ============================================================

def procesar_respuesta_firma(data: dict):
    """
    Procesa la respuesta de una consulta de firma.

    Devuelve:
      - texto: descripción en Markdown.
      - img_bytes: bytes de la imagen (si existe).
      - filename: nombre sugerido para la imagen.
    """
    mensaje_raw = _get_field(data, "Mensaje", "mensaje")

    if not mensaje_raw:
        return "No se recibió información de la firma en la respuesta.", None, None

    # Puede venir como string JSON anidado o como dict
    if isinstance(mensaje_raw, str):
        mensaje_limpio = limpiar_json_anidado(mensaje_raw).strip()
        try:
            inner = json.loads(mensaje_limpio)
        except Exception:
            texto = f"Respuesta de la API (sin poder parsear JSON):\n{mensaje_raw}"
            return texto, None, None
    elif isinstance(mensaje_raw, dict):
        inner = mensaje_raw
    else:
        texto = f"Formato inesperado de Mensaje en la respuesta de la API: {mensaje_raw}"
        return texto, None, None

    nombres = inner.get("nombres", "").strip()
    apellidos = inner.get("apellidos", "").strip()
    grupo = inner.get("grupoSanguineo", "").strip()
    sexo = inner.get("sexo", "").strip()
    fecha_nac = inner.get("fechaNacimiento", "").strip()
    lugar_nac = inner.get("lugarNacimiento", "").strip()

    texto = (
        "👤 *Datos de la persona*\n"
        f"Nombre: {nombres} {apellidos}\n"
        f"Grupo sanguíneo: {grupo}\n"
        f"Sexo: {sexo}\n"
        f"Lugar de nacimiento: {lugar_nac}\n"
        f"Fecha de nacimiento: {fecha_nac}"
    )

    firma_b64 = inner.get("firma")
    if not firma_b64:
        return texto, None, None

    if "base64," in firma_b64:
        firma_b64 = firma_b64.split("base64,", 1)[1]

    try:
        img_bytes = base64.b64decode(firma_b64)
    except Exception:
        texto += "\n\n⚠️ No se pudo decodificar la imagen de la firma."
        return texto, None, None

    extension = "jpg"
    mime = (inner.get("mimeType") or "").lower()
    if "png" in mime:
        extension = "png"
    elif "gif" in mime:
        extension = "gif"

    filename = f"firma.{extension}"

    return texto, img_bytes, filename


def procesar_respuesta_generica(data: dict, titulo="📄 Resultado de la consulta") -> str:
    """
    Procesamiento genérico (simple) para respuestas de Hércules.
    """
    mensaje_raw = _get_field(data, "Mensaje", "mensaje")

    if not mensaje_raw:
        return f"{titulo}:\n\n(No se recibió contenido en 'mensaje')."

    inner = None

    if isinstance(mensaje_raw, str):
        mensaje_limpio = limpiar_json_anidado(mensaje_raw)
        try:
            inner = json.loads(mensaje_limpio)
        except Exception:
            inner = None
    elif isinstance(mensaje_raw, (dict, list)):
        inner = mensaje_raw

    if inner is not None:
        pretty = json.dumps(inner, ensure_ascii=False, indent=2)
    else:
        pretty = str(mensaje_raw)

    if len(pretty) > 3500:
        pretty = pretty[:3500] + "\n...\n(Respuesta truncada)"

    texto = f"{titulo}:\n\n{pretty}"
    return texto


def procesar_respuesta_vehiculo(data: dict, titulo="🚗 Información del vehículo") -> str:
    """
    Procesa y formatea la respuesta de las consultas de vehículo.
    """
    mensaje_raw = _get_field(data, "Mensaje", "mensaje")
    if not mensaje_raw:
        return f"{titulo}:\n\n(No se recibió información en 'mensaje')."

    if isinstance(mensaje_raw, str):
        mensaje_limpio = limpiar_json_anidado(mensaje_raw)
        try:
            inner = json.loads(mensaje_limpio)
        except Exception:
            return procesar_respuesta_generica(data, titulo=titulo)
    elif isinstance(mensaje_raw, dict):
        inner = mensaje_raw
    else:
        return procesar_respuesta_generica(data, titulo=titulo)

    if "vehiculo" in inner:
        vehiculo_block = inner.get("vehiculo", {}) or {}
        datos = vehiculo_block.get("datos", {}) or {}
        adicional = vehiculo_block.get("adicional", {}) or {}
    else:
        datos = inner.get("datos", {}) or {}
        adicional = inner.get("adicional", {}) or {}
        vehiculo_block = {"datos": datos, "adicional": adicional}

    placa = datos.get("placaNumeroUnicoIdentificacion", "")
    clase = datos.get("claseVehiculo", "")
    marca = datos.get("marcaVehiculo", "")
    linea = datos.get("lineaVehiculo", "")
    modelo = datos.get("modelo", "")
    color = datos.get("color", "")
    servicio = datos.get("servicio", "")
    estado = datos.get("estadoRegistroVehiculo", "")
    soat_vigente = datos.get("seguroObligatorioVigente", "")
    rtm_vigente = datos.get("revisionTecnicoMecanicaVigente", "")

    lista_rtm = (adicional.get("listaRtm") or [])
    lista_polizas = (adicional.get("listaPolizas") or [])

    ultima_rtm = lista_rtm[0] if lista_rtm else None
    ultima_soat = None
    for pol in lista_polizas:
        if pol.get("tipoPoliza") == "SOAT":
            ultima_soat = pol
            break

    persona_block = inner.get("persona", {}) or {}

    nombre_prop = ""
    doc_prop = ""
    estado_prop = ""

    if "person" in persona_block:
        person = persona_block.get("person") or {}
        doc_prop = person.get("nroDocumento", "")
        nombre_prop = " ".join(
            x for x in [
                person.get("nombre1", ""),
                person.get("nombre2", ""),
                person.get("apellido1", ""),
                person.get("apellido2", ""),
            ] if x
        )
        estado_prop = person.get("estadoPersona", "")

    elif "datosEmpresa" in persona_block:
        empresa = persona_block.get("datosEmpresa") or {}
        doc_prop = empresa.get("numeroDocumentoEmpresa", "")
        nombre_prop = empresa.get("razonSocial", "")
        estado_prop = empresa.get("estado") or persona_block.get("estadoPnj", "")

    if (not nombre_prop or not doc_prop) and "vehiculo" in inner:
        adicionales_veh = (vehiculo_block.get("adicional") or {})
        lista_comp = adicionales_veh.get("listaComparendos") or []
        if lista_comp:
            comp0 = lista_comp[0]
            doc_prop = doc_prop or comp0.get("numeroIdentidadPropietario", "")
            nombre_prop = nombre_prop or comp0.get("nombrePropietario", "")

    lineas = []

    encabezado = f"{titulo}\n\n"
    if placa:
        encabezado += f"*Placa:* `{placa}`\n"
    lineas.append(encabezado)

    lineas.append(
        f"*Clase:* {clase}\n"
        f"*Marca / Línea:* {marca} {linea}\n"
        f"*Modelo:* {modelo}\n"
        f"*Color:* {color}\n"
        f"*Servicio:* {servicio}\n"
        f"*Estado registro:* {estado}\n"
    )

    lineas.append(
        f"*SOAT vigente (bandera):* {soat_vigente}\n"
        f"*RTM vigente (bandera):* {rtm_vigente}\n"
    )

    if ultima_soat:
        lineas.append(
            "\n*Último SOAT registrado:*\n"
            f"- Aseguradora: {ultima_soat.get('aseguradora', '')}\n"
            f"- Inicio: {ultima_soat.get('fechaInicio', '')}\n"
            f"- Vencimiento: {ultima_soat.get('fechaVencimiento', '')}\n"
        )

    if ultima_rtm:
        lineas.append(
            "\n*Última RTM registrada:*\n"
            f"- CDA: {ultima_rtm.get('nombreCda', '')}\n"
            f"- Tipo: {ultima_rtm.get('tipoRevision', '')}\n"
            f"- Expedición: {ultima_rtm.get('fechaExpedicion', '')}\n"
            f"- Vigencia: {ultima_rtm.get('fechaVigencia', '')}\n"
        )

    if nombre_prop or doc_prop:
        lineas.append(
            "\n*Propietario (según registro):*\n"
            f"- Nombre / Razón social: {nombre_prop}\n"
            f"- Documento: {doc_prop}\n"
            f"- Estado: {estado_prop}\n"
        )

    texto = "".join(lineas)

    if len(texto) > 3500:
        texto = texto[:3500] + "\n...\n(Respuesta truncada)"

    return texto


def procesar_respuesta_propietario(data: dict, titulo="👤 Información del propietario") -> str:
    """
    Procesa la respuesta de la consulta de tipo 4 (Propietario por placa).
    """
    mensaje_raw = _get_field(data, "Mensaje", "mensaje")
    if not mensaje_raw:
        return f"{titulo}:\n\n(No se recibió información en 'mensaje')."

    if isinstance(mensaje_raw, str):
        mensaje_limpio = limpiar_json_anidado(mensaje_raw)
        try:
            inner = json.loads(mensaje_limpio)
        except Exception:
            return procesar_respuesta_generica(data, titulo=titulo)
    elif isinstance(mensaje_raw, dict):
        inner = mensaje_raw
    else:
        return procesar_respuesta_generica(data, titulo=titulo)

    empresa = inner.get("datosEmpresa") or {}
    direccion = inner.get("direccion") or {}
    representantes = inner.get("representantes") or []

    if not empresa:
        return procesar_respuesta_generica(data, titulo=titulo)

    razon_social = empresa.get("razonSocial", "")
    nit = empresa.get("numeroDocumentoEmpresa", "")
    estado = empresa.get("estado", "") or inner.get("estadoPnj", "")
    municipio = empresa.get("municipio", "")
    departamento = empresa.get("departamento", "")
    tipo_sociedad = empresa.get("tipoSociedad", "") or empresa.get("tipoSociedadId", "")
    tipo_entidad = empresa.get("tipoEntidad", "") or empresa.get("tipoEntidadId", "")

    dir_texto = ""
    tel = ""
    email = ""
    if direccion:
        dir_texto = direccion.get("direccion", "") or ""
        tel = direccion.get("telefono") or direccion.get("celular") or ""
        email = direccion.get("email") or ""

    rep_nombre = ""
    rep_doc = ""
    if representantes:
        rep0 = representantes[0]
        rep_nombre = rep0.get("nombreCompleto", "")
        rep_doc = rep0.get("nroDocumento", "")

    lineas = []

    encabezado = f"{titulo}\n\n"
    if razon_social:
        encabezado += f"*Razón social:* {razon_social}\n"
    if nit:
        encabezado += f"*NIT:* `{nit}`\n"
    lineas.append(encabezado)

    lineas.append(
        f"*Estado:* {estado}\n"
        f"*Tipo de entidad:* {tipo_entidad}\n"
        f"*Tipo de sociedad:* {tipo_sociedad}\n"
        f"*Municipio:* {municipio}\n"
        f"*Departamento:* {departamento}\n"
    )

    if dir_texto or tel or email:
        lineas.append("\n*Dirección / contacto:*\n")
        if dir_texto:
            lineas.append(f"- Dirección: {dir_texto}\n")
        if tel:
            lineas.append(f"- Teléfono: {tel}\n")
        if email:
            lineas.append(f"- Email: {email}\n")

    if rep_nombre or rep_doc:
        lineas.append("\n*Representante legal (principal):*\n")
        lineas.append(f"- Nombre: {rep_nombre}\n")
        lineas.append(f"- Documento: {rep_doc}\n")

    texto = "".join(lineas)

    if len(texto) > 3500:
        texto = texto[:3500] + "\n...\n(Respuesta truncada)"

    return texto


# ============================================================
# LÓGICA EN HILOS (PARA NO BLOQUEAR WEBHOOK)
# ============================================================

def ejecutar_consulta_en_hilo(chat_id, accion, **kwargs):
    """
    Lanza un hilo para ejecutar la consulta sin bloquear el webhook.

    kwargs puede incluir:
      - tipo_doc, num_doc, placa, chasis
      - mensaje_id: id del registro en la BD (para marcar exito/error)
    """
    mensaje_id = kwargs.pop("mensaje_id", None)

    def _worker():
        try:
            if accion == "firma":
                enviar_mensaje(chat_id, "⏳ Procesando tu consulta de *firma*...")
                data = servicio_firma(kwargs["tipo_doc"], kwargs["num_doc"])
                if mensaje_id is not None:
                    actualizar_mensaje_exito(mensaje_id, data)

                texto, img_bytes, filename = procesar_respuesta_firma(data)
                if img_bytes:
                    enviar_foto(chat_id, img_bytes, filename=filename, caption=texto)
                else:
                    enviar_mensaje(chat_id, texto)

            elif accion == "persona":
                enviar_mensaje(chat_id, "⏳ Procesando tu consulta de *persona*...")
                data = servicio_persona(kwargs["tipo_doc"], kwargs["num_doc"])
                if mensaje_id is not None:
                    actualizar_mensaje_exito(mensaje_id, data)

                texto = procesar_respuesta_generica(data, titulo="👤 Información de la persona")
                enviar_mensaje(chat_id, texto)

            elif accion == "vehiculo_placa":
                enviar_mensaje(chat_id, "⏳ Procesando tu consulta de *vehículo por placa*...")
                data = servicio_vehiculo_por_placa(
                    kwargs["placa"], solo_vehiculo=kwargs.get("solo_vehiculo", False)
                )
                if mensaje_id is not None:
                    actualizar_mensaje_exito(mensaje_id, data)

                titulo = "🚗 Información del vehículo"
                if not kwargs.get("solo_vehiculo", False):
                    titulo += " y del propietario"
                texto = procesar_respuesta_vehiculo(data, titulo=titulo)
                enviar_mensaje(chat_id, texto)

            elif accion == "vehiculo_chasis":
                enviar_mensaje(chat_id, "⏳ Procesando tu consulta de *vehículo por chasis*...")
                data = servicio_vehiculo_por_chasis(kwargs["chasis"])
                if mensaje_id is not None:
                    actualizar_mensaje_exito(mensaje_id, data)

                texto = procesar_respuesta_vehiculo(
                    data, titulo="🚗 Información del vehículo (por chasis)"
                )
                enviar_mensaje(chat_id, texto)

            elif accion == "propietario_placa":
                enviar_mensaje(chat_id, "⏳ Procesando tu consulta de *propietario por placa*...")
                data = servicio_propietario_por_placa(kwargs["placa"])
                if mensaje_id is not None:
                    actualizar_mensaje_exito(mensaje_id, data)

                texto = procesar_respuesta_propietario(
                    data, titulo="👤 Información del propietario"
                )
                enviar_mensaje(chat_id, texto)

            else:
                enviar_mensaje(chat_id, f"Acción desconocida: {accion}")

        except TimeoutError as e:
            if mensaje_id is not None:
                actualizar_mensaje_error(mensaje_id, str(e))
            enviar_mensaje(
                chat_id,
                "⚠️ Lo siento, la consulta está tardando más de lo esperado y se ha cancelado.\n"
                "Por favor, intenta de nuevo más tarde."
            )
        except requests.HTTPError as e:
            traceback.print_exc()
            if mensaje_id is not None:
                actualizar_mensaje_error(mensaje_id, str(e))
            enviar_mensaje(chat_id, f"⚠️ Error HTTP al consultar el servicio Hércules: {e}")
        except Exception as e:
            traceback.print_exc()
            if mensaje_id is not None:
                actualizar_mensaje_error(mensaje_id, str(e))
            enviar_mensaje(chat_id, f"⚠️ Ocurrió un error inesperado: {e}")
        finally:
            # Siempre devolvemos al usuario al menú principal
            enviar_menu_principal(chat_id)

    hilo = threading.Thread(target=_worker, daemon=True)
    hilo.start()


# ============================================================
# TECLADOS / MENÚS EN TELEGRAM
# ============================================================

def teclado_menu_principal():
    """Teclado principal con los tipos de consulta."""
    return {
        "inline_keyboard": [
            [
                {"text": "🖊️ Firma", "callback_data": "menu_firma"},
                {"text": "👤 Persona", "callback_data": "menu_persona"},
            ],
            [
                {"text": "🚗 Vehículo", "callback_data": "menu_vehiculo"},
                {"text": "🚙 Propietario por placa", "callback_data": "menu_propietario"},
            ],
            [
                {"text": "ℹ️ Ayuda", "callback_data": "menu_ayuda"},
            ],
        ]
    }


def teclado_menu_tipos_documento(prefijo_accion="firma"):
    """Teclado con CC, TI, NIT."""
    return {
        "inline_keyboard": [
            [
                {"text": "CC - Cédula", "callback_data": f"{prefijo_accion}_doc_CC"},
                {"text": "TI - Tarjeta de Identidad", "callback_data": f"{prefijo_accion}_doc_TI"},
            ],
            [
                {"text": "NIT", "callback_data": f"{prefijo_accion}_doc_NIT"},
            ],
            [
                {"text": "⬅️ Volver", "callback_data": "menu_principal"},
            ],
        ]
    }


def teclado_menu_vehiculo():
    """Teclado con las opciones de vehículo."""
    return {
        "inline_keyboard": [
            [
                {"text": "Placa (Vehículo + Persona)", "callback_data": "vehiculo_placa_completo"},
            ],
            [
                {"text": "Placa (Solo vehículo)", "callback_data": "vehiculo_placa_solo"},
            ],
            [
                {"text": "Chasis", "callback_data": "vehiculo_chasis"},
            ],
            [
                {"text": "⬅️ Volver", "callback_data": "menu_principal"},
            ],
        ]
    }


def enviar_menu_principal(chat_id):
    """Envía el menú principal."""
    texto = (
        "👋 *Bienvenido a Bot_Telegram_Version_1.1*\n\n"
        "Elige el tipo de consulta que deseas realizar usando los botones de abajo.\n\n"
        "También puedes usar el modo rápido escribiendo directamente por ejemplo:\n"
        "`CC 123456789`\n"
        "para realizar una consulta de *firma*.\n\n"
        "Puedes usar `/saldo` para ver tus créditos disponibles."
    )
    enviar_mensaje(chat_id, texto, reply_markup=teclado_menu_principal())


# ============================================================
# MANEJO DE MENSAJES Y CALLBACKS
# ============================================================

def manejar_mensaje(message: dict):
    """
    Maneja mensajes de texto recibidos desde Telegram.
    """
    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    if chat_id is None:
        return

    # Obtenemos/creamos el usuario en BD
    usuario = get_or_create_usuario_from_message(message)

    texto = (message.get("text") or "").strip()

    if not texto:
        enviar_mensaje(chat_id, "Por favor envía un mensaje de texto.")
        return

    # Comando /start
    if texto.startswith("/start"):
        ESTADOS.pop(chat_id, None)
        enviar_menu_principal(chat_id)
        return

    # Comando /saldo -> muestra créditos
    if texto.startswith("/saldo"):
        enviar_saldo_usuario(chat_id, usuario)
        return

    # Si el usuario está en algún estado (esperando que escriba algo)
    estado = ESTADOS.get(chat_id)

    if estado:
        accion = estado.get("accion")

        # ------- Consulta de firma -------
        if accion == "firma":
            tipo_doc = estado.get("tipo_doc")
            num_doc = texto.replace(" ", "")

            # Registrar mensaje y descontar crédito
            mensaje_id = registrar_mensaje(
                usuario,
                tipo_servicio="firma",
                parametros={"tipo_doc": tipo_doc, "num_doc": num_doc},
                costo=1,
            )
            if mensaje_id is None:
                enviar_mensaje(chat_id, "⚠️ No tienes créditos suficientes para esta consulta.")
                ESTADOS.pop(chat_id, None)
                return

            ESTADOS.pop(chat_id, None)
            ejecutar_consulta_en_hilo(
                chat_id, "firma",
                tipo_doc=tipo_doc,
                num_doc=num_doc,
                mensaje_id=mensaje_id,
            )
            return

        # ------- Consulta de persona -------
        if accion == "persona":
            tipo_doc = estado.get("tipo_doc")
            num_doc = texto.replace(" ", "")

            mensaje_id = registrar_mensaje(
                usuario,
                tipo_servicio="persona",
                parametros={"tipo_doc": tipo_doc, "num_doc": num_doc},
                costo=1,
            )
            if mensaje_id is None:
                enviar_mensaje(chat_id, "⚠️ No tienes créditos suficientes para esta consulta.")
                ESTADOS.pop(chat_id, None)
                return

            ESTADOS.pop(chat_id, None)
            ejecutar_consulta_en_hilo(
                chat_id, "persona",
                tipo_doc=tipo_doc,
                num_doc=num_doc,
                mensaje_id=mensaje_id,
            )
            return

        # ------- Consulta de vehículo por placa -------
        if accion == "vehiculo_placa":
            placa = texto.strip().upper()
            solo_vehiculo = estado.get("solo_vehiculo", False)

            mensaje_id = registrar_mensaje(
                usuario,
                tipo_servicio="vehiculo_placa_solo" if solo_vehiculo else "vehiculo_placa",
                parametros={"placa": placa, "solo_vehiculo": solo_vehiculo},
                costo=1,
            )
            if mensaje_id is None:
                enviar_mensaje(chat_id, "⚠️ No tienes créditos suficientes para esta consulta.")
                ESTADOS.pop(chat_id, None)
                return

            ESTADOS.pop(chat_id, None)
            ejecutar_consulta_en_hilo(
                chat_id, "vehiculo_placa",
                placa=placa,
                solo_vehiculo=solo_vehiculo,
                mensaje_id=mensaje_id,
            )
            return

        # ------- Consulta de vehículo por chasis -------
        if accion == "vehiculo_chasis":
            chasis = texto.strip().upper()

            mensaje_id = registrar_mensaje(
                usuario,
                tipo_servicio="vehiculo_chasis",
                parametros={"chasis": chasis},
                costo=1,
            )
            if mensaje_id is None:
                enviar_mensaje(chat_id, "⚠️ No tienes créditos suficientes para esta consulta.")
                ESTADOS.pop(chat_id, None)
                return

            ESTADOS.pop(chat_id, None)
            ejecutar_consulta_en_hilo(
                chat_id, "vehiculo_chasis",
                chasis=chasis,
                mensaje_id=mensaje_id,
            )
            return

        # ------- Consulta de propietario por placa -------
        if accion == "propietario_placa":
            placa = texto.strip().upper()

            mensaje_id = registrar_mensaje(
                usuario,
                tipo_servicio="propietario_placa",
                parametros={"placa": placa},
                costo=1,
            )
            if mensaje_id is None:
                enviar_mensaje(chat_id, "⚠️ No tienes créditos suficientes para esta consulta.")
                ESTADOS.pop(chat_id, None)
                return

            ESTADOS.pop(chat_id, None)
            ejecutar_consulta_en_hilo(
                chat_id, "propietario_placa",
                placa=placa,
                mensaje_id=mensaje_id,
            )
            return

    # ---------------- Modo rápido para firma ----------------
    # Ejemplo: "CC 123456789" -> firma con CC
    partes = texto.split()
    if len(partes) == 2:
        tipo_doc = partes[0].upper()
        num_doc = partes[1].strip()
        if tipo_doc in ("CC", "TI", "NIT"):
            mensaje_id = registrar_mensaje(
                usuario,
                tipo_servicio="firma",
                parametros={"tipo_doc": tipo_doc, "num_doc": num_doc},
                costo=1,
            )
            if mensaje_id is None:
                enviar_mensaje(chat_id, "⚠️ No tienes créditos suficientes para esta consulta.")
                return

            ejecutar_consulta_en_hilo(
                chat_id, "firma",
                tipo_doc=tipo_doc,
                num_doc=num_doc,
                mensaje_id=mensaje_id,
            )
            return

    # Si no se reconoció el mensaje:
    ayuda = (
        "No he podido entender tu mensaje.\n\n"
        "Puedes usar el menú con /start o escribir por ejemplo:\n"
        "`CC 123456789`\n"
        "para hacer una consulta de firma.\n\n"
        "También puedes usar `/saldo` para ver tus créditos."
    )
    enviar_mensaje(chat_id, ayuda)


def manejar_callback_query(callback_query: dict):
    """
    Maneja las interacciones con botones (inline keyboard).
    """
    callback_id = callback_query.get("id")
    message = callback_query.get("message") or {}
    data = callback_query.get("data") or ""

    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    if chat_id is None:
        responder_callback_query(callback_id)
        return

    # Confirmamos a Telegram que el callback fue procesado
    responder_callback_query(callback_id)

    # ----- Menú principal -----
    if data == "menu_principal":
        ESTADOS.pop(chat_id, None)
        enviar_menu_principal(chat_id)
        return

    # ----- Menú firma -----
    if data == "menu_firma":
        ESTADOS.pop(chat_id, None)
        enviar_mensaje(
            chat_id,
            "Has elegido *Firma*.\n\nSelecciona el tipo de documento:",
            reply_markup=teclado_menu_tipos_documento(prefijo_accion="firma"),
        )
        return

    # ----- Menú persona -----
    if data == "menu_persona":
        ESTADOS.pop(chat_id, None)
        enviar_mensaje(
            chat_id,
            "Has elegido *Persona*.\n\nSelecciona el tipo de documento:",
            reply_markup=teclado_menu_tipos_documento(prefijo_accion="persona"),
        )
        return

    # ----- Menú vehículo -----
    if data == "menu_vehiculo":
        ESTADOS.pop(chat_id, None)
        enviar_mensaje(
            chat_id,
            "Has elegido *Vehículo*.\n\nElige el tipo de consulta:",
            reply_markup=teclado_menu_vehiculo(),
        )
        return

    # ----- Menú propietario por placa -----
    if data == "menu_propietario":
        ESTADOS[chat_id] = {
            "accion": "propietario_placa",
        }
        enviar_mensaje(
            chat_id,
            "Has elegido *Propietario por placa*.\n\n"
            "👉 Escribe ahora la placa del vehículo (ejemplo: ABC123).",
        )
        return

    # ----- Menú ayuda -----
    if data == "menu_ayuda":
        texto = (
            "ℹ️ *Ayuda*\n\n"
            "1️⃣ Pulsa /start para ver el menú principal.\n"
            "2️⃣ Elige el tipo de consulta (Firma, Persona, Vehículo, etc.).\n"
            "3️⃣ Selecciona el tipo de documento o parámetro que se te pida.\n"
            "4️⃣ Escribe el número correspondiente cuando el bot te lo solicite.\n\n"
            "También puedes usar el modo rápido para *firma* escribiendo directamente:\n"
            "`CC 123456789`\n\n"
            "Y con `/saldo` puedes ver tus créditos disponibles."
        )
        enviar_mensaje(chat_id, texto)
        return

    # ----- Selección de tipo de documento para FIRMA -----
    if data.startswith("firma_doc_"):
        tipo_doc = data.replace("firma_doc_", "")
        ESTADOS[chat_id] = {
            "accion": "firma",
            "tipo_doc": tipo_doc,
        }
        enviar_mensaje(
            chat_id,
            f"Has elegido *Firma* con documento tipo *{tipo_doc}*.\n\n"
            "👉 Escribe ahora el número de documento."
        )
        return

    # ----- Selección de tipo de documento para PERSONA -----
    if data.startswith("persona_doc_"):
        tipo_doc = data.replace("persona_doc_", "")
        ESTADOS[chat_id] = {
            "accion": "persona",
            "tipo_doc": tipo_doc,
        }
        enviar_mensaje(
            chat_id,
            f"Has elegido *Persona* con documento tipo *{tipo_doc}*.\n\n"
            "👉 Escribe ahora el número de documento."
        )
        return

    # ----- Vehículo por placa (vehículo + persona) -----
    if data == "vehiculo_placa_completo":
        ESTADOS[chat_id] = {
            "accion": "vehiculo_placa",
            "solo_vehiculo": False,
        }
        enviar_mensaje(
            chat_id,
            "Has elegido *Vehículo por placa (vehículo + persona)*.\n\n"
            "👉 Escribe ahora la placa del vehículo (ejemplo: ABC123).",
        )
        return

    # ----- Vehículo por placa (solo vehículo) -----
    if data == "vehiculo_placa_solo":
        ESTADOS[chat_id] = {
            "accion": "vehiculo_placa",
            "solo_vehiculo": True,
        }
        enviar_mensaje(
            chat_id,
            "Has elegido *Vehículo por placa (solo vehículo)*.\n\n"
            "👉 Escribe ahora la placa del vehículo (ejemplo: ABC123).",
        )
        return

    # ----- Vehículo por chasis -----
    if data == "vehiculo_chasis":
        ESTADOS[chat_id] = {
            "accion": "vehiculo_chasis",
        }
        enviar_mensaje(
            chat_id,
            "Has elegido *Vehículo por chasis*.\n\n"
            "👉 Escribe ahora el número de chasis.",
        )
        return

    # Si llega algún callback desconocido
    enviar_mensaje(chat_id, f"No se reconoce la acción del botón: {data}")


# ============================================================
# RUTAS FLASK (WEBHOOK TELEGRAM)
# ============================================================

@app.route(f"/webhook/{WEBHOOK_SECRET_PATH}", methods=["GET", "POST"])
def telegram_webhook():
    """
    Endpoint que Telegram llama cuando llega un mensaje / botón (webhook).
    """
    if request.method == "GET":
        return "Webhook OK (GET) - Telegram debería usar POST", 200

    update = request.get_json(force=True, silent=True) or {}

    if "callback_query" in update:
        manejar_callback_query(update["callback_query"])
    elif "message" in update:
        manejar_mensaje(update["message"])

    return jsonify(ok=True)


@app.route("/", methods=["GET"])
def index():
    """
    Endpoint básico para comprobar que el bot está vivo.
    """
    return "Bot de consultas (firma, persona, vehículo, propietario) funcionando ✅", 200


# ============================================================
# MAIN LOCAL (para pruebas con ngrok)
# ============================================================

if __name__ == "__main__":
    print("Iniciando bot Flask en http://0.0.0.0:5000/ ...")
    print("Ruta de webhook esperada:", f"/webhook/{WEBHOOK_SECRET_PATH}")
    app.run(host="0.0.0.0", port=5000, debug=True)
