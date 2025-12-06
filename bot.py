import os
import json
import time
import threading
from datetime import datetime
from typing import Optional, Dict, Any

import requests
from flask import Flask, request, jsonify

# SQLAlchemy para base de datos
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# =====================================================================
# CONFIGURACIÓN GENERAL DEL BOT Y SERVICIOS EXTERNOS
# =====================================================================

# ---------------------------------------------------------------------
# 1. TOKEN DEL BOT DE TELEGRAM (OBLIGATORIO)
# ---------------------------------------------------------------------

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError(
        "Falta la variable de entorno TELEGRAM_TOKEN. "
        "Configúrala en tu entorno local y en Railway."
    )

# Ruta “secreta” del webhook. Si no defines nada, se usa el propio token.
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", TELEGRAM_TOKEN)

# URL base de la API de Telegram
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# ---------------------------------------------------------------------
# 2. CONFIGURACIÓN API HÉRCULES (TOKEN + URL BASE)
# ---------------------------------------------------------------------

# Intentamos primero HERCULES_TOKEN y si no, API_TOKEN
API_TOKEN = os.getenv("HERCULES_TOKEN") or os.getenv("API_TOKEN")
if not API_TOKEN:
    raise RuntimeError(
        "Falta la variable de entorno HERCULES_TOKEN (o API_TOKEN). "
        "Configúrala con el token de la API Hércules."
    )

API_BASE = os.getenv(
    "HERCULES_BASE_URL",
    "https://solutechherculesazf.azurewebsites.net",
)

# ---------------------------------------------------------------------
# 3. PARÁMETROS DE POLLING A /resultados DE HÉRCULES
# ---------------------------------------------------------------------

RESULTADOS_INTERVALO = int(os.getenv("RESULTADOS_INTERVALO", "4"))
RESULTADOS_TIMEOUT = int(os.getenv("RESULTADOS_TIMEOUT", "60"))

# ---------------------------------------------------------------------
# 4. CONFIGURACIÓN DE BASE DE DATOS (SQLAlchemy)
# ---------------------------------------------------------------------

Base = declarative_base()

# Nombre de la BD local (SQLite) para pruebas
local_db_name = os.getenv("LOCAL_DB_NAME", "bot_hercules.db")
local_sqlite_url = f"sqlite:///{local_db_name}"

# Prioridad:
#  1) DATABASE_URL  (Railway)
#  2) MYSQL_URL
#  3) sqlite local
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("MYSQL_URL")
    or local_sqlite_url
)

# Si viene “mysql://” lo cambiamos a “mysql+pymysql://”
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

# Aseguramos charset para MySQL
if DATABASE_URL.startswith("mysql+pymysql://") and "charset=" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}charset=utf8mb4"

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# =====================================================================
# MODELOS DE BASE DE DATOS
# =====================================================================

class Usuario(Base):
    """
    Usuario de Telegram que usa el bot.
    """

    __tablename__ = "usuarios"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String(64), unique=True, nullable=False, index=True)
    username = Column(String(64), nullable=True)
    first_name = Column(String(64), nullable=True)
    last_name = Column(String(64), nullable=True)

    rol = Column(String(32), default="user", nullable=False)

    # Créditos totales y usados
    creditos_total = Column(Integer, default=10, nullable=False)
    creditos_usados = Column(Integer, default=0, nullable=False)

    ultima_consulta = Column(DateTime, nullable=True)

    mensajes = relationship("Mensaje", back_populates="usuario")


class Mensaje(Base):
    """
    Cada consulta realizada por el usuario.
    """

    __tablename__ = "mensajes"

    id = Column(Integer, primary_key=True, index=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)

    tipo_consulta = Column(Integer, nullable=False)      # 1..8
    nombre_servicio = Column(String(50), nullable=False) # "firma", "persona", etc.

    parametros = Column(Text, nullable=True)            # JSON serializado
    creditos_costo = Column(Integer, default=0, nullable=False)

    # pendiente / exito / error / sin_datos
    estado = Column(String(20), default="pendiente", nullable=False)

    respuesta_bruta = Column(Text, nullable=True)
    mensaje_error = Column(Text, nullable=True)

    fecha_creacion = Column(DateTime, default=datetime.utcnow, nullable=False)

    usuario = relationship("Usuario", back_populates="mensajes")


class ConsultaConfig(Base):
    """
    Configuración por tipo de consulta (precio y estado).
    """

    __tablename__ = "consultas_config"

    id = Column(Integer, primary_key=True, index=True)
    tipo_consulta = Column(Integer, unique=True, nullable=False)
    nombre_servicio = Column(String(50), nullable=False)

    valor_consulta = Column(Integer, default=5000, nullable=False)
    estado_consulta = Column(String(20), default="ACTIVA", nullable=False)


# =====================================================================
# CONSTANTES PARA TIPOS DE CONSULTA
# =====================================================================

TIPO_CONSULTA_VEHICULO_PERSONA = 1
TIPO_CONSULTA_VEHICULO_CHASIS = 2
TIPO_CONSULTA_VEHICULO_SOLO = 3
TIPO_CONSULTA_PROPIETARIO_POR_PLACA = 4
TIPO_CONSULTA_PERSONA = 5
TIPO_CONSULTA_FIRMA = 8

# =====================================================================
# INICIALIZACIÓN DE BD
# =====================================================================

def init_db() -> None:
    """
    Crea tablas si no existen y precarga consultas_config si está vacío.
    """
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        existing = db.query(ConsultaConfig).first()
        if existing:
            return

        configs = [
            ConsultaConfig(
                tipo_consulta=TIPO_CONSULTA_VEHICULO_SOLO,
                nombre_servicio="vehiculo_placa",
                valor_consulta=5000,
                estado_consulta="ACTIVA",
            ),
            ConsultaConfig(
                tipo_consulta=TIPO_CONSULTA_PROPIETARIO_POR_PLACA,
                nombre_servicio="propietario_placa",
                valor_consulta=5000,
                estado_consulta="ACTIVA",
            ),
            ConsultaConfig(
                tipo_consulta=TIPO_CONSULTA_PERSONA,
                nombre_servicio="persona",
                valor_consulta=5000,
                estado_consulta="ACTIVA",
            ),
            ConsultaConfig(
                tipo_consulta=TIPO_CONSULTA_FIRMA,
                nombre_servicio="firma",
                valor_consulta=5000,
                estado_consulta="ACTIVA",
            ),
        ]
        db.add_all(configs)
        db.commit()
    finally:
        db.close()


init_db()

# =====================================================================
# AUXILIARES DE BD
# =====================================================================

def get_db():
    return SessionLocal()


def get_or_create_usuario_from_update(update: dict) -> Usuario:
    """
    Obtiene (o crea) el Usuario a partir del update de Telegram.
    """
    message = update.get("message") or update.get("edited_message")
    if not message:
        raise ValueError("Update sin 'message' ni 'edited_message'.")

    from_user = message["from"]
    telegram_id = str(from_user["id"])
    username = from_user.get("username")
    first_name = from_user.get("first_name")
    last_name = from_user.get("last_name")

    db = get_db()
    try:
        usuario = db.query(Usuario).filter_by(telegram_id=telegram_id).one_or_none()
        if usuario:
            usuario.username = username
            usuario.first_name = first_name
            usuario.last_name = last_name
            db.commit()
            db.refresh(usuario)
            return usuario

        usuario = Usuario(
            telegram_id=telegram_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            rol="user",
            creditos_total=10,
            creditos_usados=0,
        )
        db.add(usuario)
        db.commit()
        db.refresh(usuario)
        return usuario
    finally:
        db.close()


def get_consulta_config(tipo_consulta: int) -> Optional[ConsultaConfig]:
    db = get_db()
    try:
        return (
            db.query(ConsultaConfig)
            .filter_by(tipo_consulta=tipo_consulta)
            .one_or_none()
        )
    finally:
        db.close()


def usuario_creditos_disponibles(usuario: Usuario) -> int:
    return max(usuario.creditos_total - usuario.creditos_usados, 0)


def registrar_mensaje_pendiente(
    usuario: Usuario,
    tipo_consulta: int,
    nombre_servicio: str,
    parametros: Dict[str, Any],
    valor_consulta: int,
) -> int:
    """
    Registra un mensaje en estado 'pendiente' (sin cobrar aún).
    """
    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()

        msg = Mensaje(
            usuario_id=usuario_db.id,
            tipo_consulta=tipo_consulta,
            nombre_servicio=nombre_servicio,
            parametros=json.dumps(parametros, ensure_ascii=False),
            creditos_costo=valor_consulta,
            estado="pendiente",
        )
        db.add(msg)
        db.commit()
        db.refresh(msg)
        return msg.id
    finally:
        db.close()


def marcar_mensaje_exito_y_cobrar(mensaje_id: int, respuesta_bruta: dict) -> None:
    """
    Marca mensaje como 'exito', guarda la respuesta y descuenta créditos.
    """
    db = get_db()
    try:
        msg = db.query(Mensaje).filter_by(id=mensaje_id).one_or_none()
        if not msg:
            return

        usuario = db.query(Usuario).filter_by(id=msg.usuario_id).one()
        msg.estado = "exito"
        msg.respuesta_bruta = json.dumps(respuesta_bruta, ensure_ascii=False)

        usuario.creditos_usados += msg.creditos_costo
        usuario.ultima_consulta = datetime.utcnow()

        db.commit()
    finally:
        db.close()


def marcar_mensaje_error_o_sin_datos(
    mensaje_id: int,
    estado: str,
    mensaje_error: str = "",
    respuesta_bruta: Optional[dict] = None,
) -> None:
    """
    Marca mensaje como 'error' o 'sin_datos'. No cobra créditos.
    """
    db = get_db()
    try:
        msg = db.query(Mensaje).filter_by(id=mensaje_id).one_or_none()
        if not msg:
            return

        msg.estado = estado
        msg.mensaje_error = mensaje_error or estado
        if respuesta_bruta is not None:
            msg.respuesta_bruta = json.dumps(respuesta_bruta, ensure_ascii=False)

        db.commit()
    finally:
        db.close()


# =====================================================================
# TEXTOS Y TECLADOS DE TELEGRAM
# =====================================================================

# Intentamos importar textos.py
try:
    import textos
except ImportError:
    class textos:
        MENSAJE_BIENVENIDA = (
            "👋 *Bienvenido al bot de consultas*\n\n"
            "Estoy listo para tus consultas ✅\n\n"
            "Usa los botones de abajo o escribe `CC 123456789` para una consulta rápida de firma."
        )
        MENSAJE_SIN_CREDITOS = "⚠️ No tienes créditos suficientes para realizar esta consulta."
        MENSAJE_ERROR_GENERICO = "❌ Ocurrió un error realizando la consulta. Inténtalo más tarde."
        MENSAJE_SIN_DATOS = "ℹ️ No se encontraron datos para los parámetros enviados."
        MENSAJE_SALDO = (
            "💰 *Tu saldo de créditos*\n\n"
            "Totales: {total}\n"
            "Usados: {usados}\n"
            "Disponibles: {disponibles}\n"
        )
        FIRMA_ELEGIDA_TEXTO = (
            "✍️ Has elegido *Consulta de firma*.\n\n"
            "Primero selecciona el *tipo de documento* 👇"
        )
        FIRMA_PEDIR_NUMERO = (
            "✍️ Escribe ahora el número de documento para *firma* ({tipo_doc}), "
            "sin puntos ni comas:"
        )
        PERSONA_INSTRUCCIONES = (
            "🧍 Envía el documento de la persona en el formato:\n"
            "`CC 123456789`"
        )
        VEHICULO_INSTRUCCIONES = (
            "🚗 Envía la placa del vehículo (sin espacios), por ejemplo:\n`ABC123`"
        )
        PROPIETARIO_INSTRUCCIONES = (
            "👤 Envía la placa del vehículo (sin espacios) para consultar el propietario:"
        )
        MENSAJE_NO_ENTENDI = (
            "No entendí tu mensaje. Usa el menú o el formato rápido `CC 123456789`."
        )


def enviar_mensaje(chat_id: int, texto: str, reply_markup: Optional[dict] = None):
    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": "Markdown",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        resp = requests.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Enviando mensaje a Telegram: {e}")


def teclado_menu_principal():
    return {
        "keyboard": [
            ["📝 Consulta de firma", "🧍 Consulta de persona"],
            ["🚗 Consulta de vehículo", "👤 Propietario por placa"],
            ["/saldo"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def teclado_tipos_documento_firma():
    """
    Botones de tipo de documento para consulta de firma.
    """
    return {
        "keyboard": [
            ["CC - Cédula", "TI - Tarjeta de Identidad"],
            ["CE - Cédula de Extranjería", "NIT"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


# Mapa texto del botón -> código de tipo de documento
MAPA_BOTON_TIPO_DOC = {
    "CC - Cédula": "CC",
    "TI - Tarjeta de Identidad": "TI",
    "CE - Cédula de Extranjería": "CE",
    "NIT": "NIT",
}

# =====================================================================
# ESTADO EN MEMORIA POR USUARIO
# =====================================================================

user_states: Dict[int, Dict[str, Any]] = {}


def set_user_state(chat_id: int, estado: Optional[str], datos: Optional[Dict[str, Any]] = None):
    if estado is None:
        user_states.pop(chat_id, None)
    else:
        user_states[chat_id] = {"estado": estado, "datos": datos or {}}


def get_user_state(chat_id: int) -> Dict[str, Any]:
    return user_states.get(chat_id, {"estado": None, "datos": {}})

# =====================================================================
# API HÉRCULES
# =====================================================================

def llamar_iniciar_consulta(payload: dict) -> str:
    """
    Llama a IniciarConsulta y devuelve IdPeticion.
    Con logs detallados en caso de error HTTP.
    """
    url = f"{API_BASE}/api/Hercules/Consulta/Inicio/IniciarConsulta"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }

    print(f"[DEBUG] IniciarConsulta payload: {payload}")
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "N/A"
        body = e.response.text if e.response is not None else "N/A"
        print(f"[ERROR] HTTP IniciarConsulta status={status}, body={body}")
        raise
    except Exception as e:
        print(f"[ERROR] IniciarConsulta error genérico: {e}")
        raise

    data = resp.json()
    print(f"[DEBUG] Respuesta IniciarConsulta: {data}")

    id_peticion = data.get("IdPeticion") or data.get("idPeticion")
    if not id_peticion:
        raise RuntimeError("La respuesta de IniciarConsulta no trae IdPeticion.")
    return id_peticion


def llamar_resultados(tipo_consulta: int, mensaje: str, id_peticion: str) -> dict:
    url = f"{API_BASE}/api/Hercules/Consulta/Inicio/Resultados"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }
    payload = {
        "Tipo": tipo_consulta,
        "Mensaje": mensaje,
        "IdPeticion": id_peticion,
    }

    print(f"[DEBUG] Resultados payload: {payload}")
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    print(f"[DEBUG] Respuesta Resultados: {data}")
    return data


def es_respuesta_exitosa_hercules(data: dict) -> bool:
    """
    Determina si la respuesta cuenta como exitosa para COBRAR.
    """
    try:
        if data.get("Tipo") != 0:
            return False

        mensaje_str = data.get("Mensaje", "")
        if not mensaje_str:
            return False

        mensaje_json = json.loads(mensaje_str)

        if isinstance(mensaje_json, dict) and mensaje_json.get("Error") is True:
            return False

        codigo = None
        if isinstance(mensaje_json, dict):
            codigo = mensaje_json.get("codigoResultado") or mensaje_json.get("codigo")

        if codigo and str(codigo).upper() != "EXITOSO":
            return False

        return True
    except Exception as e:
        print(f"[ERROR] Analizando respuesta de Hércules: {e}")
        return False


def ejecutar_consulta_en_hilo(
    chat_id: int,
    usuario: Usuario,
    mensaje_id: int,
    tipo_consulta: int,
    mensaje_parametro: str,
    texto_pendiente: str,
    formateador_respuesta,
):
    """
    Hilo que hace polling a /Resultados, decide éxito / error, actualiza BD
    y envía el mensaje formateado.
    """

    def _run():
        try:
            deadline = time.time() + RESULTADOS_TIMEOUT
            ultimo_data = None

            while time.time() < deadline:
                data = llamar_resultados(tipo_consulta, mensaje_parametro, texto_pendiente)
                ultimo_data = data

                tipo = data.get("Tipo")
                mensaje = data.get("Mensaje")
                print(
                    f"[DEBUG] Resultado parcial (tipo={tipo_consulta}, mensaje='{mensaje_parametro}'): {data}"
                )

                if tipo == 2:
                    time.sleep(RESULTADOS_INTERVALO)
                    continue

                break  # tipo 0 u otro -> salimos del bucle

            if not ultimo_data:
                marcar_mensaje_error_o_sin_datos(
                    mensaje_id,
                    estado="error",
                    mensaje_error="Sin respuesta de resultados (timeout).",
                )
                enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
                return

            if es_respuesta_exitosa_hercules(ultimo_data):
                marcar_mensaje_exito_y_cobrar(mensaje_id, ultimo_data)
                texto_respuesta = formateador_respuesta(ultimo_data)
                enviar_mensaje(chat_id, texto_respuesta)
            else:
                marcar_mensaje_error_o_sin_datos(
                    mensaje_id,
                    estado="sin_datos",
                    mensaje_error="Consulta sin datos o no exitosa.",
                    respuesta_bruta=ultimo_data,
                )
                enviar_mensaje(chat_id, textos.MENSAJE_SIN_DATOS)

        except Exception as e:
            print(f"[ERROR] ejecutando consulta en hilo: {e}")
            marcar_mensaje_error_o_sin_datos(
                mensaje_id,
                estado="error",
                mensaje_error=str(e),
            )
            enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)

    th = threading.Thread(target=_run, daemon=True)
    th.start()

# =====================================================================
# FORMATEADORES DE RESPUESTA
# =====================================================================

def formatear_respuesta_firma(data: dict) -> str:
    try:
        mensaje_str = data.get("Mensaje", "")
        info = json.loads(mensaje_str)

        person = info.get("person", {}) or info.get("persona", {}) or {}
        nombre = " ".join(
            [
                person.get("nombre1", ""),
                person.get("nombre2", ""),
                person.get("apellido1", ""),
                person.get("apellido2", ""),
            ]
        ).strip()

        tipo_doc = person.get("idTipoDoc") or person.get("tipoDocumento") or ""
        nro_doc = person.get("nroDocumento") or person.get("nroDoc") or ""

        return (
            f"📝 *Resultado de consulta de firma*\n\n"
            f"*Nombre:* {nombre or '-'}\n"
            f"*Documento:* {tipo_doc} {nro_doc}\n"
        )
    except Exception as e:
        print(f"[ERROR] formateando respuesta de firma: {e}")
        return textos.MENSAJE_ERROR_GENERICO


def formatear_respuesta_persona(data: dict) -> str:
    try:
        mensaje_str = data.get("Mensaje", "")
        info = json.loads(mensaje_str)

        person = info.get("person") or info.get("persona") or info.get("personDTO") or {}
        nombre = " ".join(
            [
                person.get("nombre1", ""),
                person.get("nombre2", ""),
                person.get("apellido1", ""),
                person.get("apellido2", ""),
            ]
        ).strip()
        tipo_doc = person.get("idTipoDoc") or person.get("tipoDocumento") or ""
        nro_doc = person.get("nroDocumento") or person.get("nroDoc") or ""

        return (
            f"🧍 *Consulta de persona*\n\n"
            f"*Nombre:* {nombre or '-'}\n"
            f"*Documento:* {tipo_doc} {nro_doc}\n"
        )
    except Exception as e:
        print(f"[ERROR] formateando respuesta de persona: {e}")
        return textos.MENSAJE_ERROR_GENERICO


def formatear_respuesta_vehiculo(data: dict) -> str:
    try:
        mensaje_str = data.get("Mensaje", "")
        info = json.loads(mensaje_str)
        veh = info.get("vehiculo", {}) or {}
        datos = veh.get("datos", {}) or {}

        placa = datos.get("placaNumeroUnicoIdentificacion") or datos.get("placa") or "-"
        marca = datos.get("marcaVehiculo") or "-"
        linea = datos.get("lineaVehiculo") or "-"
        modelo = datos.get("modelo") or "-"
        color = datos.get("color") or "-"
        servicio = datos.get("servicio") or "-"
        clase = datos.get("claseVehiculo") or "-"

        return (
            f"🚗 *Consulta de vehículo*\n\n"
            f"*Placa:* {placa}\n"
            f"*Marca:* {marca}\n"
            f"*Línea:* {linea}\n"
            f"*Modelo:* {modelo}\n"
            f"*Color:* {color}\n"
            f"*Clase:* {clase}\n"
            f"*Servicio:* {servicio}\n"
        )
    except Exception as e:
        print(f"[ERROR] formateando respuesta de vehículo: {e}")
        return textos.MENSAJE_ERROR_GENERICO


def formatear_respuesta_propietario(data: dict) -> str:
    try:
        mensaje_str = data.get("Mensaje", "")
        info = json.loads(mensaje_str)

        persona = info.get("persona") or {}
        datos_empresa = persona.get("datosEmpresa") or info.get("datosEmpresa") or {}

        person = persona.get("person") or {}
        nombre_persona = " ".join(
            [
                person.get("nombre1", ""),
                person.get("nombre2", ""),
                person.get("apellido1", ""),
                person.get("apellido2", ""),
            ]
        ).strip()
        tipo_doc = person.get("idTipoDoc") or person.get("tipoDocumento") or ""
        nro_doc = person.get("nroDocumento") or person.get("nroDoc") or ""

        if not nombre_persona and datos_empresa:
            nombre_persona = datos_empresa.get("razonSocial", "")
            tipo_doc = datos_empresa.get("tipoDocumentoEmpresa") or "NIT"
            nro_doc = datos_empresa.get("numeroDocumentoEmpresa") or ""

        if not nombre_persona:
            nombre_persona = "-"

        return (
            f"👤 *Propietario del vehículo*\n\n"
            f"*Nombre / Razón social:* {nombre_persona}\n"
            f"*Documento:* {tipo_doc} {nro_doc}\n"
        )
    except Exception as e:
        print(f"[ERROR] formateando respuesta de propietario: {e}")
        return textos.MENSAJE_ERROR_GENERICO

# =====================================================================
# LÓGICA DE NEGOCIO: INICIAR CONSULTAS
# =====================================================================

def iniciar_consulta_firma(usuario: Usuario, chat_id: int, tipo_doc: str, num_doc: str):
    """
    Consulta de firma (tipo 8).
    No se asume tipo_doc: viene de los botones o del texto del usuario.
    """
    config = get_consulta_config(TIPO_CONSULTA_FIRMA)
    if not config or config.estado_consulta != "ACTIVA":
        enviar_mensaje(chat_id, "⚠️ La consulta de firma está deshabilitada.")
        return

    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
        disponibles = usuario_creditos_disponibles(usuario_db)
        if disponibles < config.valor_consulta:
            enviar_mensaje(chat_id, textos.MENSAJE_SIN_CREDITOS)
            return
    finally:
        db.close()

    # Enviamos ambas variantes de campos por compatibilidad
    payload_msg = {
        "tipoDocumento": tipo_doc,
        "numeroDocumento": num_doc,
        "tipo_doc": tipo_doc,
        "num_doc": num_doc,
    }

    payload = {
        "Tipo": TIPO_CONSULTA_FIRMA,
        "Mensaje": json.dumps(payload_msg, ensure_ascii=False),
    }

    try:
        id_peticion = llamar_iniciar_consulta(payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_firma -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_FIRMA,
        nombre_servicio="firma",
        parametros=payload_msg,
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_FIRMA,
        mensaje_parametro=json.dumps(payload_msg, ensure_ascii=False),
        texto_pendiente=id_peticion,
        formateador_respuesta=formatear_respuesta_firma,
    )


def iniciar_consulta_persona(usuario: Usuario, chat_id: int, tipo_doc: str, num_doc: str):
    config = get_consulta_config(TIPO_CONSULTA_PERSONA)
    if not config or config.estado_consulta != "ACTIVA":
        enviar_mensaje(chat_id, "⚠️ La consulta de persona está deshabilitada.")
        return

    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
        disponibles = usuario_creditos_disponibles(usuario_db)
        if disponibles < config.valor_consulta:
            enviar_mensaje(chat_id, textos.MENSAJE_SIN_CREDITOS)
            return
    finally:
        db.close()

    payload_msg = {
        "tipoDocumento": tipo_doc,
        "numeroDocumento": num_doc,
        "tipo_doc": tipo_doc,
        "num_doc": num_doc,
    }

    payload = {
        "Tipo": TIPO_CONSULTA_PERSONA,
        "Mensaje": json.dumps(payload_msg, ensure_ascii=False),
    }

    try:
        id_peticion = llamar_iniciar_consulta(payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_persona -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_PERSONA,
        nombre_servicio="persona",
        parametros=payload_msg,
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_PERSONA,
        mensaje_parametro=json.dumps(payload_msg, ensure_ascii=False),
        texto_pendiente=id_peticion,
        formateador_respuesta=formatear_respuesta_persona,
    )


def iniciar_consulta_vehiculo(usuario: Usuario, chat_id: int, placa: str):
    config = get_consulta_config(TIPO_CONSULTA_VEHICULO_SOLO)
    if not config or config.estado_consulta != "ACTIVA":
        enviar_mensaje(chat_id, "⚠️ La consulta de vehículo está deshabilitada.")
        return

    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
        disponibles = usuario_creditos_disponibles(usuario_db)
        if disponibles < config.valor_consulta:
            enviar_mensaje(chat_id, textos.MENSAJE_SIN_CREDITOS)
            return
    finally:
        db.close()

    placa_limpia = placa.replace(" ", "").upper()
    payload_msg = {"placa": placa_limpia, "solo_vehiculo": True}

    payload = {
        "Tipo": TIPO_CONSULTA_VEHICULO_SOLO,
        "Mensaje": json.dumps(payload_msg, ensure_ascii=False),
    }

    try:
        id_peticion = llamar_iniciar_consulta(payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_vehiculo -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_SOLO,
        nombre_servicio="vehiculo_placa",
        parametros=payload_msg,
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_SOLO,
        mensaje_parametro=placa_limpia,
        texto_pendiente=id_peticion,
        formateador_respuesta=formatear_respuesta_vehiculo,
    )


def iniciar_consulta_propietario(usuario: Usuario, chat_id: int, placa: str):
    config = get_consulta_config(TIPO_CONSULTA_PROPIETARIO_POR_PLACA)
    if not config or config.estado_consulta != "ACTIVA":
        enviar_mensaje(chat_id, "⚠️ La consulta de propietario por placa está deshabilitada.")
        return

    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
        disponibles = usuario_creditos_disponibles(usuario_db)
        if disponibles < config.valor_consulta:
            enviar_mensaje(chat_id, textos.MENSAJE_SIN_CREDITOS)
            return
    finally:
        db.close()

    placa_limpia = placa.replace(" ", "").upper()
    payload_msg = {"placa": placa_limpia}

    payload = {
        "Tipo": TIPO_CONSULTA_PROPIETARIO_POR_PLACA,
        "Mensaje": json.dumps(payload_msg, ensure_ascii=False),
    }

    try:
        id_peticion = llamar_iniciar_consulta(payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_propietario -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_PROPIETARIO_POR_PLACA,
        nombre_servicio="propietario_placa",
        parametros=payload_msg,
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_PROPIETARIO_POR_PLACA,
        mensaje_parametro=placa_limpia,
        texto_pendiente=id_peticion,
        formateador_respuesta=formatear_respuesta_propietario,
    )

# =====================================================================
# FLASK + WEBHOOK TELEGRAM
# =====================================================================

app = Flask(__name__)


@app.route(f"/webhook/{WEBHOOK_SECRET_PATH}", methods=["POST"])
def telegram_webhook():
    update = request.get_json(force=True, silent=True) or {}
    print(f"[DEBUG] Update recibido: {json.dumps(update, ensure_ascii=False)}")

    try:
        usuario = get_or_create_usuario_from_update(update)
    except Exception as e:
        print(f"[ERROR] obteniendo/creando usuario: {e}")
        return jsonify({"ok": True}), 200

    message = update.get("message") or update.get("edited_message") or {}
    chat = message.get("chat", {})
    chat_id = chat.get("id")

    if not chat_id:
        return jsonify({"ok": True}), 200

    text = (message.get("text") or "").strip()

    # ----------------- COMANDOS -----------------
    if text.startswith("/start"):
        enviar_mensaje(
            chat_id,
            textos.MENSAJE_BIENVENIDA,
            reply_markup=teclado_menu_principal(),
        )
        return jsonify({"ok": True}), 200

    if text.startswith("/saldo"):
        db = get_db()
        try:
            usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
            total = usuario_db.creditos_total
            usados = usuario_db.creditos_usados
            disponibles = usuario_creditos_disponibles(usuario_db)
        finally:
            db.close()

        msg = textos.MENSAJE_SALDO.format(
            total=total,
            usados=usados,
            disponibles=disponibles,
        )
        enviar_mensaje(chat_id, msg, reply_markup=teclado_menu_principal())
        return jsonify({"ok": True}), 200

    # Atajo rápido: "CC 123456789"
    if text.upper().startswith(("CC ", "TI ", "CE ", "NIT ")):
        partes = text.split()
        if len(partes) >= 2:
            tipo_doc = partes[0].upper()
            num_doc = partes[1]
            iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
            return jsonify({"ok": True}), 200

    # ----------------- MENÚ PRINCIPAL -----------------
    if text == "📝 Consulta de firma":
        enviar_mensaje(
            chat_id,
            textos.FIRMA_ELEGIDA_TEXTO,
            reply_markup=teclado_tipos_documento_firma(),
        )
        set_user_state(chat_id, "firma_esperando_tipo")
        return jsonify({"ok": True}), 200

    if text == "🧍 Consulta de persona":
        enviar_mensaje(
            chat_id,
            textos.PERSONA_INSTRUCCIONES,
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_persona")
        return jsonify({"ok": True}), 200

    if text == "🚗 Consulta de vehículo":
        enviar_mensaje(
            chat_id,
            textos.VEHICULO_INSTRUCCIONES,
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_vehiculo")
        return jsonify({"ok": True}), 200

    if text == "👤 Propietario por placa":
        enviar_mensaje(
            chat_id,
            textos.PROPIETARIO_INSTRUCCIONES,
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_propietario")
        return jsonify({"ok": True}), 200

    # ----------------- MANEJO DE ESTADOS -----------------
    estado_info = get_user_state(chat_id)
    estado = estado_info.get("estado")
    datos_estado = estado_info.get("datos", {})

    # Firma: usuario eligió tipo de documento
    if estado == "firma_esperando_tipo" and text in MAPA_BOTON_TIPO_DOC:
        tipo_doc = MAPA_BOTON_TIPO_DOC[text]
        set_user_state(chat_id, "firma_esperando_numero", {"tipo_doc": tipo_doc})
        enviar_mensaje(
            chat_id,
            textos.FIRMA_PEDIR_NUMERO.format(tipo_doc=tipo_doc),
            reply_markup=teclado_menu_principal(),
        )
        return jsonify({"ok": True}), 200

    # Firma: usuario envía número de documento
    if estado == "firma_esperando_numero":
        tipo_doc = datos_estado.get("tipo_doc")
        num_doc = text.replace(" ", "")
        if not tipo_doc:
            enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
            set_user_state(chat_id, None)
            return jsonify({"ok": True}), 200

        iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # Persona: esperamos "TIPO NUMERO"
    if estado == "esperando_persona":
        partes = text.split()
        if len(partes) < 2:
            enviar_mensaje(
                chat_id,
                "Por favor envía el documento en el formato: `CC 123456789`",
                reply_markup=teclado_menu_principal(),
            )
            return jsonify({"ok": True}), 200

        tipo_doc = partes[0].upper()
        num_doc = partes[1]
        iniciar_consulta_persona(usuario, chat_id, tipo_doc, num_doc)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # Vehículo por placa
    if estado == "esperando_placa_vehiculo":
        placa = text.strip().upper().replace(" ", "")
        iniciar_consulta_vehiculo(usuario, chat_id, placa)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # Propietario por placa
    if estado == "esperando_placa_propietario":
        placa = text.strip().upper().replace(" ", "")
        iniciar_consulta_propietario(usuario, chat_id, placa)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # ----------------- FALLBACK -----------------
    enviar_mensaje(
        chat_id,
        textos.MENSAJE_NO_ENTENDI,
        reply_markup=teclado_menu_principal(),
    )
    return jsonify({"ok": True}), 200


@app.route("/", methods=["GET"])
def index():
    return "Bot de consultas de firmas funcionando ✅", 200


if __name__ == "__main__":
    print("Iniciando bot Flask en http://0.0.0.0:5000/ ...")
    print("Ruta de webhook esperada:", f"/webhook/{WEBHOOK_SECRET_PATH}")
    app.run(host="0.0.0.0", port=5000, debug=True)
