import os
import json
import base64
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
    Boolean,
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

# Ruta "secreta" del webhook. Por defecto usamos el propio token.
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", TELEGRAM_TOKEN)

# URL base de la API de Telegram
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# ---------------------------------------------------------------------
# 2. CONFIGURACIÓN API HÉRCULES (TOKEN + URL BASE)
# ---------------------------------------------------------------------
API_TOKEN = os.getenv("HERCULES_TOKEN")
if not API_TOKEN:
    raise RuntimeError(
        "Falta la variable de entorno HERCULES_TOKEN. "
        "Configúrala con el token de la API Hércules (por ejemplo 'mvk')."
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

local_db_name = os.getenv("LOCAL_DB_NAME", "bot_hercules.db")
local_sqlite_url = f"sqlite:///{local_db_name}"

DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("MYSQL_URL")
    or local_sqlite_url
)

# Ajustamos driver de MySQL
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

if DATABASE_URL.startswith("mysql+pymysql://") and "charset=" not in DATABASE_URL:
    separador = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{separador}charset=utf8mb4"

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# =====================================================================
# MODELOS DE BASE DE DATOS
# =====================================================================

class Usuario(Base):
    """
    Usuario de Telegram.
    """
    __tablename__ = "usuarios"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String(64), unique=True, nullable=False, index=True)
    username = Column(String(64), nullable=True)
    first_name = Column(String(64), nullable=True)
    last_name = Column(String(64), nullable=True)

    rol = Column(String(32), default="user", nullable=False)

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

    tipo_consulta = Column(Integer, nullable=False)          # 1..8
    nombre_servicio = Column(String(50), nullable=False)     # "firma", "persona", etc.
    parametros = Column(Text, nullable=True)                 # JSON string
    creditos_costo = Column(Integer, default=0, nullable=False)

    estado = Column(String(20), default="pendiente", nullable=False)
    respuesta_bruta = Column(Text, nullable=True)
    mensaje_error = Column(Text, nullable=True)

    fecha_creacion = Column(DateTime, default=datetime.utcnow, nullable=False)

    usuario = relationship("Usuario", back_populates="mensajes")


class ConsultaConfig(Base):
    """
    Configuración de precio y estado por tipo de consulta.
    """
    __tablename__ = "consultas_config"

    id = Column(Integer, primary_key=True, index=True)
    tipo_consulta = Column(Integer, unique=True, nullable=False)
    nombre_servicio = Column(String(50), nullable=False)
    valor_consulta = Column(Integer, default=5000, nullable=False)
    estado_consulta = Column(String(20), default="ACTIVA", nullable=False)

# =====================================================================
# CONSTANTES TIPO CONSULTA
# =====================================================================

TIPO_CONSULTA_VEHICULO_PERSONA = 1
TIPO_CONSULTA_VEHICULO_CHASIS = 2
TIPO_CONSULTA_VEHICULO_SOLO = 3
TIPO_CONSULTA_PROPIETARIO_POR_PLACA = 4
TIPO_CONSULTA_PERSONA = 5
TIPO_CONSULTA_FIRMA = 8

# =====================================================================
# INICIALIZACIÓN BD
# =====================================================================

def init_db() -> None:
    """
    Crea tablas y precarga consultas_config si está vacía.
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
# AUXILIARES BD
# =====================================================================

def get_db():
    return SessionLocal()

def get_or_create_usuario_from_update(update: dict) -> Usuario:
    message = update.get("message") or update.get("edited_message")
    if not message:
        raise ValueError("Update de Telegram sin 'message' ni 'edited_message'.")

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

def marcar_mensaje_exito_y_cobrar(
    mensaje_id: int,
    respuesta_bruta: dict,
) -> None:
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
# TELEGRAM: TEXTOS Y ENVÍO DE MENSAJES
# =====================================================================

try:
    import textos
except ImportError:
    class textos:
        MENSAJE_BIENVENIDA = (
            "👋 *Bienvenido al bot de consultas*\n\n"
            "Estoy listo para tus consultas ✅\n"
        )
        MENSAJE_SIN_CREDITOS = (
            "⚠️ No tienes créditos suficientes para realizar esta consulta."
        )
        MENSAJE_ERROR_GENERICO = (
            "❌ Ocurrió un error realizando la consulta. Inténtalo más tarde."
        )
        MENSAJE_SIN_DATOS = "ℹ️ No se encontraron datos para los parámetros enviados."
        MENSAJE_SALDO = (
            "💰 *Tu saldo de créditos*\n\n"
            "Totales: {total}\n"
            "Usados: {usados}\n"
            "Disponibles: {disponibles}\n"
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

def teclado_tipos_documento():
    """
    Teclado para elegir tipo de documento (sin asumir nada).
    """
    return {
        "keyboard": [
            ["CC - Cédula", "TI - Tarjeta de Identidad"],
            ["CE - Cédula de Extranjería", "NIT"],
            ["🔙 Volver al menú"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }

# =====================================================================
# ESTADO EN MEMORIA POR USUARIO
# =====================================================================

user_states: Dict[int, Dict[str, Any]] = {}

def set_user_state(chat_id: int, estado: Optional[str], datos: Optional[Dict[str, Any]] = None):
    user_states[chat_id] = {"estado": estado, "datos": datos or {}}

def get_user_state(chat_id: int) -> Dict[str, Any]:
    return user_states.get(chat_id, {"estado": None, "datos": {}})

# =====================================================================
# API HÉRCULES
# =====================================================================

def llamar_iniciar_consulta(payload: dict) -> str:
    url = f"{API_BASE}/api/Hercules/Consulta/Inicio/IniciarConsulta"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }

    print(f"[DEBUG] IniciarConsulta payload: {payload}")
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
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
    def _run():
        try:
            deadline = time.time() + RESULTADOS_TIMEOUT
            ultimo_data = None

            while time.time() < deadline:
                data = llamar_resultados(tipo_consulta, mensaje_parametro, texto_pendiente)
                ultimo_data = data

                print(
                    f"[DEBUG] Resultado parcial (tipo={tipo_consulta}, mensaje='{mensaje_parametro}'): {data}"
                )

                if data.get("Tipo") == 2:
                    time.sleep(RESULTADOS_INTERVALO)
                    continue

                break

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
# FORMATEADORES RESPUESTA
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

    payload_msg = {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc}
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

    payload_msg = {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc}
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
        "Mensaje": json.dumps(payload_msg),
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
        "Mensaje": json.dumps(payload_msg),
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

    # ---------------- Comandos globales ----------------
    if text.startswith("/start"):
        enviar_mensaje(
            chat_id,
            textos.MENSAJE_BIENVENIDA,
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, None)
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

    # ---------------- Botones de menú principal ----------------

    if text == "📝 Consulta de firma":
        enviar_mensaje(
            chat_id,
            "✍️ Has elegido *Consulta de firma*.\n\n"
            "Primero selecciona el *tipo de documento*: 👇",
            reply_markup=teclado_tipos_documento(),
        )
        set_user_state(chat_id, "esperando_tipo_doc_firma")
        return jsonify({"ok": True}), 200

    if text == "🧍 Consulta de persona":
        enviar_mensaje(
            chat_id,
            "🧍 Has elegido *Consulta de persona*.\n\n"
            "Primero selecciona el *tipo de documento*: 👇",
            reply_markup=teclado_tipos_documento(),
        )
        set_user_state(chat_id, "esperando_tipo_doc_persona")
        return jsonify({"ok": True}), 200

    if text == "🚗 Consulta de vehículo":
        enviar_mensaje(
            chat_id,
            "🚗 Has elegido *Consulta de vehículo*.\n\n"
            "Escribe ahora la placa del vehículo (sin espacios), por ejemplo:\n"
            "`ABC123`",
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_vehiculo")
        return jsonify({"ok": True}), 200

    if text == "👤 Propietario por placa":
        enviar_mensaje(
            chat_id,
            "👤 Has elegido *Propietario por placa*.\n\n"
            "Escribe ahora la placa del vehículo (sin espacios).",
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_propietario")
        return jsonify({"ok": True}), 200

    # ---------------- Lógica según estado previo ----------------
    estado_info = get_user_state(chat_id)
    estado = estado_info.get("estado")
    datos_estado = estado_info.get("datos", {})

    # --- Selección de tipo de documento (firma / persona) ---
    if estado == "esperando_tipo_doc_firma" or estado == "esperando_tipo_doc_persona":
        # Botón "volver"
        if text == "🔙 Volver al menú":
            set_user_state(chat_id, None)
            enviar_mensaje(
                chat_id,
                "Has vuelto al menú principal.",
                reply_markup=teclado_menu_principal(),
            )
            return jsonify({"ok": True}), 200

        # Mapeamos el texto del botón a un código de tipo de documento
        text_upper = text.upper()
        tipo_doc = None
        if text_upper.startswith("CC"):
            tipo_doc = "CC"
        elif text_upper.startswith("TI"):
            tipo_doc = "TI"
        elif text_upper.startswith("CE"):
            tipo_doc = "CE"
        elif text_upper.startswith("NIT"):
            tipo_doc = "NIT"

        if not tipo_doc:
            enviar_mensaje(
                chat_id,
                "❗ Por favor, selecciona un tipo de documento usando los botones.",
                reply_markup=teclado_tipos_documento(),
            )
            return jsonify({"ok": True}), 200

        # Guardamos el tipo_doc en el estado y pasamos a pedir el número
        if estado == "esperando_tipo_doc_firma":
            set_user_state(chat_id, "esperando_num_doc_firma", {"tipo_doc": tipo_doc})
            enviar_mensaje(
                chat_id,
                f"✍️ Escribe ahora el número de documento para *firma* "
                f"({tipo_doc}), sin puntos ni comas:",
                reply_markup=teclado_menu_principal(),
            )
        else:
            set_user_state(chat_id, "esperando_num_doc_persona", {"tipo_doc": tipo_doc})
            enviar_mensaje(
                chat_id,
                f"🧍 Escribe ahora el número de documento de la *persona* "
                f"({tipo_doc}), sin puntos ni comas:",
                reply_markup=teclado_menu_principal(),
            )

        return jsonify({"ok": True}), 200

    # --- Recibir número de documento para firma ---
    if estado == "esperando_num_doc_firma":
        tipo_doc = datos_estado.get("tipo_doc")
        num_doc = text.replace(" ", "")
        if not tipo_doc:
            enviar_mensaje(
                chat_id,
                "❗ No tengo registrado el tipo de documento. "
                "Vuelve a empezar con el menú.",
                reply_markup=teclado_menu_principal(),
            )
            set_user_state(chat_id, None)
            return jsonify({"ok": True}), 200

        iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # --- Recibir número de documento para persona ---
    if estado == "esperando_num_doc_persona":
        tipo_doc = datos_estado.get("tipo_doc")
        num_doc = text.replace(" ", "")
        if not tipo_doc:
            enviar_mensaje(
                chat_id,
                "❗ No tengo registrado el tipo de documento. "
                "Vuelve a empezar con el menú.",
                reply_markup=teclado_menu_principal(),
            )
            set_user_state(chat_id, None)
            return jsonify({"ok": True}), 200

        iniciar_consulta_persona(usuario, chat_id, tipo_doc, num_doc)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # --- Placa de vehículo ---
    if estado == "esperando_placa_vehiculo":
        placa = text.strip().upper().replace(" ", "")
        iniciar_consulta_vehiculo(usuario, chat_id, placa)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # --- Placa para propietario ---
    if estado == "esperando_placa_propietario":
        placa = text.strip().upper().replace(" ", "")
        iniciar_consulta_propietario(usuario, chat_id, placa)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # ---------------- Atajo global para firma ----------------
    # Solo se usa si NO hay un estado pendiente.
    if text.upper().startswith(("CC ", "TI ", "CE ", "NIT ")):
        partes = text.split()
        if len(partes) >= 2:
            tipo_doc = partes[0].upper()
            num_doc = partes[1]
            iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
            return jsonify({"ok": True}), 200

    # ---------------- Mensaje por defecto ----------------
    enviar_mensaje(
        chat_id,
        "No entendí tu mensaje. Usa el menú o el formato rápido (`CC 123456789`).",
        reply_markup=teclado_menu_principal(),
    )
    return jsonify({"ok": True}), 200

@app.route("/", methods=["GET"])
def index():
    return "Bot de consultas de firmas funcionando ✅", 200

# =====================================================================
# MAIN LOCAL
# =====================================================================

if __name__ == "__main__":
    print("Iniciando bot Flask en http://0.0.0.0:5000/ ...")
    print("Ruta de webhook esperada:", f"/webhook/{WEBHOOK_SECRET_PATH}")
    app.run(host="0.0.0.0", port=5000, debug=True)
