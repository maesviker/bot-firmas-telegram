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
# Nunca dejes el token fijo en el código.
# Debe venir SIEMPRE de una variable de entorno: TELEGRAM_TOKEN
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    # Si llegas aquí en local, abre una terminal y exporta la variable, ej.:
    #   Windows (CMD):    set TELEGRAM_TOKEN=TU_TOKEN
    #   Windows (Powershell): $env:TELEGRAM_TOKEN="TU_TOKEN"
    #   Linux/Mac:        export TELEGRAM_TOKEN=TU_TOKEN
    raise RuntimeError(
        "Falta la variable de entorno TELEGRAM_TOKEN. "
        "Configúrala en tu entorno local y en Railway."
    )

# Ruta "secreta" del webhook. Por defecto usamos el propio token.
# Puedes definir WEBHOOK_SECRET_PATH en el entorno si quieres otra cadena.
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", TELEGRAM_TOKEN)

# URL base de la API de Telegram (no cambia)
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# ---------------------------------------------------------------------
# 2. CONFIGURACIÓN API HÉRCULES (TOKEN + URL BASE)
# ---------------------------------------------------------------------

# Token de autenticación contra Hércules.
# Defínelo en el entorno como HERCULES_TOKEN. Ejemplo:
#   HERCULES_TOKEN = mvk
API_TOKEN = os.getenv("HERCULES_TOKEN")
if not API_TOKEN:
    raise RuntimeError(
        "Falta la variable de entorno HERCULES_TOKEN. "
        "Configúrala con el token de la API Hércules (por ejemplo 'mvk')."
    )

# URL base de la API de Hércules.
# Puedes sobrescribirla con HERCULES_BASE_URL si algún día cambia.
API_BASE = os.getenv(
    "HERCULES_BASE_URL",
    "https://solutechherculesazf.azurewebsites.net",
)

# ---------------------------------------------------------------------
# 3. PARÁMETROS DE POLLING A /resultados DE HÉRCULES
# ---------------------------------------------------------------------

# Intervalo en segundos entre cada consulta a /resultados
# Lo pediste explícitamente en 4 segundos.
RESULTADOS_INTERVALO = int(os.getenv("RESULTADOS_INTERVALO", "4"))

# Tiempo máximo total de espera (segundos) antes de abortar la consulta
RESULTADOS_TIMEOUT = int(os.getenv("RESULTADOS_TIMEOUT", "60"))

# ---------------------------------------------------------------------
# 4. CONFIGURACIÓN DE BASE DE DATOS (SQLAlchemy)
# ---------------------------------------------------------------------

Base = declarative_base()

# Prioridad de resolución de la URL de base de datos:
#  1) DATABASE_URL  -> normalmente apuntará al MySQL de Railway
#  2) MYSQL_URL     -> respaldo por si Railway la expone así
#  3) LOCAL_DB_NAME -> nombre de fichero sqlite local (ej: bot_hercules.db)
#  4) fallback      -> sqlite:///bot_hercules.db
local_db_name = os.getenv("LOCAL_DB_NAME", "bot_hercules.db")
local_sqlite_url = f"sqlite:///{local_db_name}"

DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("MYSQL_URL")
    or local_sqlite_url
)

# Si es un MySQL sin dialecto, lo convertimos a mysql+pymysql
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

# Para MySQL, añadimos charset=utf8mb4 si no está presente
if DATABASE_URL.startswith("mysql+pymysql://") and "charset=" not in DATABASE_URL:
    separador = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{separador}charset=utf8mb4"

# Creamos el engine y la factoría de sesiones
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# =====================================================================
# MODELOS DE BASE DE DATOS
# =====================================================================

class Usuario(Base):
    """
    Representa a un usuario de Telegram que usa el bot.
    Se relaciona con la tabla 'mensajes' (consultas realizadas).
    """

    __tablename__ = "usuarios"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String(64), unique=True, nullable=False, index=True)
    username = Column(String(64), nullable=True)
    first_name = Column(String(64), nullable=True)
    last_name = Column(String(64), nullable=True)

    # Rol por si en el futuro quieres admin / user / etc.
    rol = Column(String(32), default="user", nullable=False)

    # Créditos totales asignados al usuario (inicialmente 10)
    creditos_total = Column(Integer, default=10, nullable=False)

    # Créditos ya usados en consultas exitosas
    creditos_usados = Column(Integer, default=0, nullable=False)

    # Fecha y hora de la última consulta (puede ser null)
    ultima_consulta = Column(DateTime, nullable=True)

    mensajes = relationship("Mensaje", back_populates="usuario")


class Mensaje(Base):
    """
    Representa cada consulta realizada por el usuario.
    Aquí se relaciona con Usuario y con la configuración de consultas.
    """

    __tablename__ = "mensajes"

    id = Column(Integer, primary_key=True, index=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)

    # tipo_consulta: 1,2,3,4,5,8 ... (según catálogo de Hércules)
    tipo_consulta = Column(Integer, nullable=False)

    # nombre_servicio: "firma", "persona", "vehiculo_placa", etc.
    nombre_servicio = Column(String(50), nullable=False)

    # Parámetros enviados en la consulta (JSON serializado)
    parametros = Column(Text, nullable=True)

    # Créditos que costaría esta consulta (valor tomada de ConsultaConfig)
    creditos_costo = Column(Integer, default=0, nullable=False)

    # Estado de la consulta:
    #  "pendiente" -> se inició la consulta
    #  "exito"     -> hubo respuesta útil y se cobró
    #  "error"     -> error técnico (timeout, HTTP, etc.) -> no se cobra
    #  "sin_datos" -> consulta válida pero sin información -> no se cobra
    estado = Column(String(20), default="pendiente", nullable=False)

    # Respuesta cruda de Hércules (JSON como texto)
    respuesta_bruta = Column(Text, nullable=True)

    # Mensaje de error (si aplica)
    mensaje_error = Column(Text, nullable=True)

    fecha_creacion = Column(DateTime, default=datetime.utcnow, nullable=False)

    usuario = relationship("Usuario", back_populates="mensajes")


class ConsultaConfig(Base):
    """
    Configuración por tipo de consulta:
      - cuánto cuesta
      - si está habilitada o no
    Así puedes cambiar precios y activar/desactivar servicios sin tocar código.
    """

    __tablename__ = "consultas_config"

    id = Column(Integer, primary_key=True, index=True)

    # Tipo de consulta (1..8) según catálogo de Hércules
    tipo_consulta = Column(Integer, unique=True, nullable=False)

    # Nombre interno del servicio ("firma", "persona", "vehiculo_placa", etc.)
    nombre_servicio = Column(String(50), nullable=False)

    # Valor de la consulta (en "créditos")
    valor_consulta = Column(Integer, default=5000, nullable=False)

    # Estado de la consulta: ACTIVA / INACTIVA
    estado_consulta = Column(String(20), default="ACTIVA", nullable=False)


# =====================================================================
# CONSTANTES PARA TIPOS DE CONSULTA
# =====================================================================

# Estas constantes facilitan mantener el mapeo entre tus funciones
# y los "Tipo" de la API Hércules.
TIPO_CONSULTA_VEHICULO_PERSONA = 1          # Ej. placa + propietario (si aplicara)
TIPO_CONSULTA_VEHICULO_CHASIS = 2           # Vehículo por chasis
TIPO_CONSULTA_VEHICULO_SOLO = 3             # Vehículo por placa (solo vehículo)
TIPO_CONSULTA_PROPIETARIO_POR_PLACA = 4     # Propietario por placa
TIPO_CONSULTA_PERSONA = 5                   # Persona por documento
TIPO_CONSULTA_FIRMA = 8                     # Firma

# =====================================================================
# FUNCIONES DE INICIALIZACIÓN DE BD
# =====================================================================

def init_db() -> None:
    """
    Crea las tablas si no existen y precarga la tabla consultas_config
    con valores por defecto si está vacía.
    """
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        # ¿Ya hay alguna config de consulta?
        existing = db.query(ConsultaConfig).first()
        if existing:
            return

        # Si no hay nada, creamos configuración por defecto
        configs = [
            # Puedes ajustar 'valor_consulta' luego desde DBeaver.
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


# Ejecutamos la inicialización al importar el módulo (arranque del bot)
init_db()


# =====================================================================
# FUNCIONES AUXILIARES DE BASE DE DATOS
# =====================================================================

def get_db():
    """
    Devuelve una sesión nueva de base de datos.
    Recuerda SIEMPRE hacer db.close() cuando termines.
    """
    return SessionLocal()


def get_or_create_usuario_from_update(update: dict) -> Usuario:
    """
    A partir del update de Telegram, identifica al usuario y lo crea si no existe.
    """
    # Dependiendo del tipo de update, el usuario está en distintas partes;
    # aquí asumimos mensajes estándar (update["message"])
    message = update.get("message") or update.get("edited_message")
    if not message:
        # Si por alguna razón no hay mensaje, no tiene mucho sentido continuar.
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
            # Actualizamos datos básicos por si han cambiado
            usuario.username = username
            usuario.first_name = first_name
            usuario.last_name = last_name
            db.commit()
            db.refresh(usuario)
            return usuario

        # Usuario nuevo -> le damos 10 créditos (por defecto en el modelo)
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
    """
    Devuelve la configuración de un tipo de consulta (o None si no existe).
    """
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
    """
    Calcula los créditos disponibles de un usuario.
    """
    return max(usuario.creditos_total - usuario.creditos_usados, 0)


def registrar_mensaje_pendiente(
    usuario: Usuario,
    tipo_consulta: int,
    nombre_servicio: str,
    parametros: Dict[str, Any],
    valor_consulta: int,
) -> int:
    """
    Registra un mensaje en estado 'pendiente' PERO NO DESCUNTA CRÉDITOS TODAVÍA.
    El cobro real se hace SOLO si la consulta resulta exitosa.
    """
    db = get_db()
    try:
        # Refrescamos el usuario desde BD
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
    """
    Marca un mensaje como 'exito', almacena la respuesta bruta
    y DESCUENTA los créditos al usuario asociado.
    """
    db = get_db()
    try:
        msg = db.query(Mensaje).filter_by(id=mensaje_id).one_or_none()
        if not msg:
            return

        usuario = db.query(Usuario).filter_by(id=msg.usuario_id).one()

        # Guardamos respuesta y marcamos éxito
        msg.estado = "exito"
        msg.respuesta_bruta = json.dumps(respuesta_bruta, ensure_ascii=False)

        # Aquí se realiza el COBRO REAL de créditos
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
    Marca un mensaje como 'error' o 'sin_datos' y almacena el mensaje de error.
    NO cobra créditos.
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
# TELEGRAM: ENVÍO DE MENSAJES Y TECLADOS
# =====================================================================

# Para mantener el archivo organizado, los textos largos los ponemos en otro
# módulo llamado 'textos.py'. Aquí asumimos que está en el mismo directorio.
try:
    import textos
except ImportError:
    # Si no existe textos.py, definimos unos mínimos para que el bot no explote.
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
    """
    Envía un mensaje de texto a un chat de Telegram.
    """
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
    """
    Teclado con las opciones principales del bot.
    """
    return {
        "keyboard": [
            ["📝 Consulta de firma", "🧍 Consulta de persona"],
            ["🚗 Consulta de vehículo", "👤 Propietario por placa"],
            ["/saldo"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


# =====================================================================
# ESTADO EN MEMORIA POR USUARIO (CONVERSACIÓN)
# =====================================================================

# Diccionario simple en memoria para manejar el "modo" de cada usuario.
# En producción con muchos usuarios se recomendaría algo más robusto (Redis, etc.),
# pero para tu caso es suficiente.
user_states: Dict[int, Dict[str, Any]] = {}


def set_user_state(chat_id: int, estado: str, datos: Optional[Dict[str, Any]] = None):
    """
    Guarda el estado actual de conversación para un chat.
    """
    user_states[chat_id] = {"estado": estado, "datos": datos or {}}


def get_user_state(chat_id: int) -> Dict[str, Any]:
    """
    Devuelve el estado actual del chat (o uno por defecto).
    """
    return user_states.get(chat_id, {"estado": None, "datos": {}})


# =====================================================================
# FUNCIONES PARA CONSUMIR LA API DE HÉRCULES
# =====================================================================

def llamar_iniciar_consulta(payload: dict) -> str:
    """
    Llama al endpoint IniciarConsulta de Hércules y devuelve el IdPeticion.
    """
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

    # La API suele devolver {"IdPeticion": "..."}
    id_peticion = data.get("IdPeticion") or data.get("idPeticion")
    if not id_peticion:
        raise RuntimeError("La respuesta de IniciarConsulta no trae IdPeticion.")
    return id_peticion


def llamar_resultados(tipo_consulta: int, mensaje: str, id_peticion: str) -> dict:
    """
    Llama al endpoint /Resultados de Hércules para obtener el estado de la consulta.
    """
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
    Determina si la respuesta de Hércules es considerada "exitosa"
    para efectos de COBRO de créditos.
    Criterio básico:
      - data["Tipo"] == 0
      - Y si existe 'Error' en el mensaje interno, que sea False.
    """
    try:
        if data.get("Tipo") != 0:
            return False

        # El campo "Mensaje" viene como JSON serializado (string).
        mensaje_str = data.get("Mensaje", "")
        if not mensaje_str:
            return False

        mensaje_json = json.loads(mensaje_str)

        # Muchos endpoints devuelven algo como {"Error": false, ...}
        # Si existe y es True, no cobramos.
        if isinstance(mensaje_json, dict) and mensaje_json.get("Error") is True:
            return False

        # Si hay codigoResultado y es EXITOSO, también es buena señal
        codigo = None
        if isinstance(mensaje_json, dict):
            codigo = mensaje_json.get("codigoResultado") or mensaje_json.get(
                "codigo"
            )
        if codigo and str(codigo).upper() != "EXITOSO":
            # Si hay código y no es EXITOSO, no cobramos
            return False

        # Si llegamos aquí, consideramos la respuesta como "exitosa".
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
    Ejecuta en un hilo separado el ciclo:
      1) Polling a /Resultados hasta que haya respuesta final o timeout.
      2) Determina si es éxito o error/sin_datos.
      3) Marca en BD y cobra (solo si éxito).
      4) Envía el mensaje al usuario.
    """
    def _run():
        try:
            deadline = time.time() + RESULTADOS_TIMEOUT
            ultimo_data = None

            # Mientras no superemos el timeout, consultamos cada RESULTADOS_INTERVALO
            while time.time() < deadline:
                data = llamar_resultados(tipo_consulta, mensaje_parametro, texto_pendiente)

                # Guardamos el último data por si lo necesitamos al final
                ultimo_data = data

                tipo = data.get("Tipo")
                mensaje = data.get("Mensaje")

                print(f"[DEBUG] Resultado parcial (tipo={tipo_consulta}, mensaje='{mensaje_parametro}'): {data}")

                # Tipo 2 -> "Procesando..." (seguimos esperando)
                if tipo == 2:
                    time.sleep(RESULTADOS_INTERVALO)
                    continue

                # Tipo 0 -> respuesta final
                break

            if not ultimo_data:
                # Nunca obtuvimos respuesta
                marcar_mensaje_error_o_sin_datos(
                    mensaje_id,
                    estado="error",
                    mensaje_error="Sin respuesta de resultados (timeout).",
                )
                enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
                return

            # ¿Es éxito?
            if es_respuesta_exitosa_hercules(ultimo_data):
                # Marcamos éxito y COBRAMOS créditos
                marcar_mensaje_exito_y_cobrar(mensaje_id, ultimo_data)

                # Formateamos la respuesta en un mensaje bonito para Telegram
                texto_respuesta = formateador_respuesta(ultimo_data)
                enviar_mensaje(chat_id, texto_respuesta)
            else:
                # No se considera éxito -> NO se cobra
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

    # Lanzamos el hilo
    th = threading.Thread(target=_run, daemon=True)
    th.start()


# =====================================================================
# FORMATEADORES DE RESPUESTA (HÉRCULES -> TEXTO PARA TELEGRAM)
# =====================================================================

def formatear_respuesta_firma(data: dict) -> str:
    """
    Da formato a la respuesta de una consulta de firma.
    Aquí puedes adaptar al detalle según el JSON real que recibes.
    """
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
    """
    Formato básico para respuesta de persona.
    """
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
    """
    Formato básico para respuesta de vehículo por placa.
    """
    try:
        mensaje_str = data.get("Mensaje", "")
        info = json.loads(mensaje_str)

        # Según ejemplos que mostraste, viene algo como {"vehiculo": {"datos": {...}}}
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
    """
    Formato básico para respuesta de propietario por placa.
    Maneja tanto persona natural (CC) como NIT (empresa).
    """
    try:
        mensaje_str = data.get("Mensaje", "")
        info = json.loads(mensaje_str)

        persona = info.get("persona") or {}
        datos_empresa = persona.get("datosEmpresa") or info.get("datosEmpresa") or {}

        # Intentamos persona natural
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

        # Si no hay persona natural, probamos datosEmpresa (NIT)
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
    Orquesta la consulta de firma (tipo 8).
    """
    # 1) Revisamos configuración de la consulta
    config = get_consulta_config(TIPO_CONSULTA_FIRMA)
    if not config or config.estado_consulta != "ACTIVA":
        enviar_mensaje(chat_id, "⚠️ La consulta de firma está deshabilitada.")
        return

    # 2) Verificamos créditos disponibles
    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
        disponibles = usuario_creditos_disponibles(usuario_db)
        if disponibles < config.valor_consulta:
            enviar_mensaje(chat_id, textos.MENSAJE_SIN_CREDITOS)
            return
    finally:
        db.close()

    # 3) Llamamos a IniciarConsulta
    payload = {
        "Tipo": TIPO_CONSULTA_FIRMA,
        "Mensaje": json.dumps(
            {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc},
            ensure_ascii=False,
        ),
    }
    try:
        id_peticion = llamar_iniciar_consulta(payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_firma -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    # 4) Registramos el mensaje en estado 'pendiente' (NO cobramos todavía)
    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_FIRMA,
        nombre_servicio="firma",
        parametros={"tipoDocumento": tipo_doc, "numeroDocumento": num_doc},
        valor_consulta=config.valor_consulta,
    )

    # 5) Lanzamos hilo para hacer polling y cobrar solo si hay éxito
    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_FIRMA,
        mensaje_parametro=json.dumps(
            {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc},
            ensure_ascii=False,
        ),
        texto_pendiente=id_peticion,
        formateador_respuesta=formatear_respuesta_firma,
    )


def iniciar_consulta_persona(usuario: Usuario, chat_id: int, tipo_doc: str, num_doc: str):
    """
    Orquesta la consulta de persona (tipo 5).
    """
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

    payload = {
        "Tipo": TIPO_CONSULTA_PERSONA,
        "Mensaje": json.dumps(
            {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc},
            ensure_ascii=False,
        ),
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
        parametros={"tipoDocumento": tipo_doc, "numeroDocumento": num_doc},
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_PERSONA,
        mensaje_parametro=json.dumps(
            {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc},
            ensure_ascii=False,
        ),
        texto_pendiente=id_peticion,
        formateador_respuesta=formatear_respuesta_persona,
    )


def iniciar_consulta_vehiculo(usuario: Usuario, chat_id: int, placa: str):
    """
    Orquesta la consulta de vehículo por placa (tipo 3, solo vehículo).
    """
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
    payload = {
        "Tipo": TIPO_CONSULTA_VEHICULO_SOLO,
        "Mensaje": json.dumps({"placa": placa_limpia, "solo_vehiculo": True}),
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
        parametros={"placa": placa_limpia, "solo_vehiculo": True},
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
    """
    Orquesta la consulta de propietario por placa (tipo 4).
    """
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
    payload = {
        "Tipo": TIPO_CONSULTA_PROPIETARIO_POR_PLACA,
        "Mensaje": json.dumps({"placa": placa_limpia}),
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
        parametros={"placa": placa_limpia},
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
# FLASK + MANEJO DE WEBHOOK TELEGRAM
# =====================================================================

app = Flask(__name__)


@app.route(f"/webhook/{WEBHOOK_SECRET_PATH}", methods=["POST"])
def telegram_webhook():
    """
    Endpoint que recibe los updates de Telegram vía webhook.
    """
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

    # Comandos básicos
    if text.startswith("/start"):
        enviar_mensaje(chat_id, textos.MENSAJE_BIENVENIDA, reply_markup=teclado_menu_principal())
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

    # Atajos de texto tipo "CC 123456789" para firma
    # Formato: <TIPO_DOC> <NUM_DOC>
    if text.upper().startswith(("CC ", "TI ", "CE ", "NIT ")):
        partes = text.split()
        if len(partes) >= 2:
            tipo_doc = partes[0].upper()
            num_doc = partes[1]
            iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
            return jsonify({"ok": True}), 200

    # Menú principal por texto
    if text == "📝 Consulta de firma":
        enviar_mensaje(
            chat_id,
            "✍️ Envía el documento así:\n`CC 123456789`",
            reply_markup=teclado_menu_principal(),
        )
        return jsonify({"ok": True}), 200

    if text == "🧍 Consulta de persona":
        enviar_mensaje(
            chat_id,
            "🧍 Envía el documento de la persona, por ejemplo:\n`CC 123456789`",
            reply_markup=teclado_menu_principal(),
        )
        # Podrías poner el estado para esperar documento específico si quieres
        set_user_state(chat_id, "esperando_persona")
        return jsonify({"ok": True}), 200

    if text == "🚗 Consulta de vehículo":
        enviar_mensaje(
            chat_id,
            "🚗 Envía la placa del vehículo (sin espacios), por ejemplo:\n`ABC123`",
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_vehiculo")
        return jsonify({"ok": True}), 200

    if text == "👤 Propietario por placa":
        enviar_mensaje(
            chat_id,
            "👤 Envía la placa del vehículo (sin espacios) para consultar el propietario:",
        )
        set_user_state(chat_id, "esperando_placa_propietario")
        return jsonify({"ok": True}), 200

    # Lógica según estado previo del usuario
    estado_info = get_user_state(chat_id)
    estado = estado_info.get("estado")

    if estado == "esperando_persona":
        # Interpretamos todo el texto como "CC 123" o solo el número
        partes = text.split()
        if len(partes) == 1:
            # Solo número -> asumimos CC
            tipo_doc = "CC"
            num_doc = partes[0]
        else:
            tipo_doc = partes[0].upper()
            num_doc = partes[1]

        iniciar_consulta_persona(usuario, chat_id, tipo_doc, num_doc)
        # Reseteamos estado
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    if estado == "esperando_placa_vehiculo":
        placa = text.strip().upper().replace(" ", "")
        iniciar_consulta_vehiculo(usuario, chat_id, placa)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    if estado == "esperando_placa_propietario":
        placa = text.strip().upper().replace(" ", "")
        iniciar_consulta_propietario(usuario, chat_id, placa)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # Si no coincidió con nada, respondemos algo genérico y mostramos menú
    enviar_mensaje(
        chat_id,
        "No entendí tu mensaje. Usa el menú o el formato rápido (`CC 123456789`).",
        reply_markup=teclado_menu_principal(),
    )
    return jsonify({"ok": True}), 200


@app.route("/", methods=["GET"])
def index():
    """
    Endpoint sencillo para comprobar que el bot está vivo.
    """
    return "Bot de consultas de firmas funcionando ✅", 200


# =====================================================================
# MAIN LOCAL
# =====================================================================

if __name__ == "__main__":
    print("Iniciando bot Flask en http://0.0.0.0:5000/ ...")
    print("Ruta de webhook esperada:", f"/webhook/{WEBHOOK_SECRET_PATH}")
    app.run(host="0.0.0.0", port=5000, debug=True)
