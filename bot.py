import os
import json
import time
import threading
import base64
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
# 1. CONFIGURACIÓN GENERAL
# =====================================================================

# ---------------------------------------------------------------
# 1.1 TOKEN DEL BOT DE TELEGRAM
# ---------------------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError(
        "Falta TELEGRAM_TOKEN en variables de entorno.\n"
        "Configúralo en tu entorno local y en Railway."
    )

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Ruta de webhook: por defecto usamos el token (puedes cambiarla)
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", TELEGRAM_TOKEN)

# ---------------------------------------------------------------
# 1.2 CONFIGURACIÓN API HÉRCULES
# ---------------------------------------------------------------
# Token de usuario asignado por Hércules (ej. 'mvk')
HERCULES_TOKEN = os.getenv("HERCULES_TOKEN")
if not HERCULES_TOKEN:
    raise RuntimeError(
        "Falta HERCULES_TOKEN en variables de entorno.\n"
        "Configúralo con el token de la API Hércules (ej. 'mvk')."
    )

# URL base de la API Hércules
API_BASE = os.getenv(
    "HERCULES_BASE_URL",
    "https://solutechherculesazf.azurewebsites.net",
)

# Intervalo entre consultas a /resultados (segundos)
RESULTADOS_INTERVALO = int(os.getenv("RESULTADOS_INTERVALO", "4"))
# Tiempo máximo de espera total para resultados (segundos)
RESULTADOS_TIMEOUT = int(os.getenv("RESULTADOS_TIMEOUT", "180"))

# ---------------------------------------------------------------
# 1.3 CONFIGURACIÓN BASE DE DATOS
# ---------------------------------------------------------------
Base = declarative_base()

local_db_name = os.getenv("LOCAL_DB_NAME", "bot_hercules.db")
local_sqlite_url = f"sqlite:///{local_db_name}"

DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("MYSQL_URL")
    or local_sqlite_url
)

# Ajuste de dialecto para MySQL -> mysql+pymysql
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

# Charset para MySQL
if DATABASE_URL.startswith("mysql+pymysql://") and "charset=" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}charset=utf8mb4"

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# =====================================================================
# 2. MODELOS DE BASE DE DATOS
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

    # Créditos totales que tiene el usuario (inicialmente 10)
    creditos_total = Column(Integer, default=10, nullable=False)
    # Créditos que ya ha consumido en consultas exitosas
    creditos_usados = Column(Integer, default=0, nullable=False)

    ultima_consulta = Column(DateTime, nullable=True)

    mensajes = relationship("Mensaje", back_populates="usuario")


class Mensaje(Base):
    """
    Registro de cada consulta hecha por un usuario.
    """
    __tablename__ = "mensajes"

    id = Column(Integer, primary_key=True, index=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), nullable=False)

    # Tipo de consulta Hércules (1..8)
    tipo_consulta = Column(Integer, nullable=False)

    # Nombre del servicio lógico (firma, persona, vehiculo_placa, etc.)
    nombre_servicio = Column(String(50), nullable=False)

    # Parámetros enviados (JSON serializado)
    parametros = Column(Text, nullable=True)

    # Cuánto costaría esta consulta (en créditos)
    creditos_costo = Column(Integer, default=0, nullable=False)

    # pendiente | exito | error | sin_datos
    estado = Column(String(20), default="pendiente", nullable=False)

    respuesta_bruta = Column(Text, nullable=True)
    mensaje_error = Column(Text, nullable=True)

    fecha_creacion = Column(DateTime, default=datetime.utcnow, nullable=False)

    usuario = relationship("Usuario", back_populates="mensajes")


class ConsultaConfig(Base):
    """
    Configuración por tipo de consulta:
      - valor_consulta
      - estado_consulta (ACTIVA / INACTIVA)
    """
    __tablename__ = "consultas_config"

    id = Column(Integer, primary_key=True, index=True)
    tipo_consulta = Column(Integer, unique=True, nullable=False)
    nombre_servicio = Column(String(50), nullable=False)
    valor_consulta = Column(Integer, default=5000, nullable=False)
    estado_consulta = Column(String(20), default="ACTIVA", nullable=False)

# =====================================================================
# 3. CONSTANTES DE TIPO DE CONSULTA (CATÁLOGO HÉRCULES)
# =====================================================================

TIPO_CONSULTA_VEHICULO_PERSONA = 1
TIPO_CONSULTA_VEHICULO_CHASIS = 2
TIPO_CONSULTA_VEHICULO_SOLO = 3
TIPO_CONSULTA_PROPIETARIO_POR_PLACA = 4
TIPO_CONSULTA_PERSONA = 5
TIPO_CONSULTA_FIRMA = 8

# =====================================================================
# 4. INICIALIZACIÓN DE BD
# =====================================================================

def init_db() -> None:
    """
    Crea tablas y precarga consultas_config si está vacía.
    """
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        if db.query(ConsultaConfig).first():
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
# 5. FUNCIONES AUXILIARES DE BD
# =====================================================================

def get_db():
    return SessionLocal()


def get_or_create_usuario_from_update(update: dict) -> Usuario:
    """
    Localiza o crea el usuario de Telegram que envía el mensaje.
    """
    message = update.get("message") or update.get("edited_message")
    if not message:
        raise ValueError("Update sin message ni edited_message")

    from_user = message["from"]
    telegram_id = str(from_user["id"])

    db = get_db()
    try:
        usuario = db.query(Usuario).filter_by(telegram_id=telegram_id).one_or_none()
        if usuario:
            usuario.username = from_user.get("username")
            usuario.first_name = from_user.get("first_name")
            usuario.last_name = from_user.get("last_name")
            db.commit()
            db.refresh(usuario)
            return usuario

        usuario = Usuario(
            telegram_id=telegram_id,
            username=from_user.get("username"),
            first_name=from_user.get("first_name"),
            last_name=from_user.get("last_name"),
            rol="user",
            creditos_total=10,     # usuario nuevo arranca con 10 créditos
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
    """
    total - usados (no puede ser negativo)
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
    Registra la consulta en estado 'pendiente'.
    No descuenta créditos todavía.
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
    Marca el mensaje como 'exito' y descuenta créditos al usuario asociado.
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
    Marca el mensaje como 'error' o 'sin_datos'.
    No descuenta créditos.
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
# 6. TEXTOS Y TECLADOS TELEGRAM
# =====================================================================

# Intentamos importar textos.py; si no existe, definimos valores por defecto
try:
    import textos
except ImportError:
    class textos:
        MENSAJE_BIENVENIDA = (
            "👋 *Bienvenido a Bot_Telegram_V1.1*\n\n"
            "Elige el tipo de consulta con los botones de abajo.\n\n"
            "Modo rápido (firma): `CC 123456789`.\n"
            "Escribe `/saldo` para ver tus créditos."
        )
        MENSAJE_SIN_CREDITOS = (
            "⚠️ No tienes créditos suficientes para realizar esta consulta."
        )
        MENSAJE_ERROR_GENERICO = (
            "❌ Ocurrió un error realizando la consulta.\n"
            "Por favor inténtalo de nuevo más tarde."
        )
        MENSAJE_SIN_DATOS = "ℹ️ La consulta fue procesada pero no se encontraron datos para los parámetros enviados."
        MENSAJE_SALDO = (
            "💰 *Tu saldo de créditos*\n\n"
            "Totales: {total}\n"
            "Usados: {usados}\n"
            "Disponibles: {disponibles}\n"
        )


def enviar_mensaje(chat_id: int, texto: str, reply_markup: Optional[dict] = None):
    """
    Wrapper para enviar mensajes a Telegram.
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


def enviar_documento_firma_desde_b64(chat_id: int, firma_b64: str):
    """
    Decodifica la firma en base64 y la envía a Telegram como documento (GIF/imagen).
    """
    try:
        if not firma_b64:
            return

        image_bytes = base64.b64decode(firma_b64)

        files = {
            "document": ("firma.gif", image_bytes)  # la firma es un GIF (R0lGOD...)
        }
        data = {
            "chat_id": chat_id,
            "caption": "🖊 Firma registrada",
        }

        resp = requests.post(
            f"{TELEGRAM_API_URL}/sendDocument",
            data=data,
            files=files,
            timeout=30,
        )
        resp.raise_for_status()
        print("[DEBUG] Firma enviada como documento a Telegram")
    except Exception as e:
        print(f"[ERROR] Enviando imagen de firma a Telegram: {e}")


def teclado_menu_principal():
    """
    Teclado principal.
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


def teclado_tipos_documento():
    """
    Teclado para elegir tipo de documento (CC, TI, NIT).
    Útil tanto para firma como para persona.
    """
    return {
        "keyboard": [
            ["CC - Cédula", "TI - Tarjeta de identidad"],
            ["NIT - NIT"],
            ["⬅ Volver al menú"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }

# =====================================================================
# 7. ESTADO EN MEMORIA POR USUARIO
# =====================================================================

user_states: Dict[int, Dict[str, Any]] = {}


def set_user_state(chat_id: int, estado: Optional[str], datos: Optional[Dict[str, Any]] = None):
    user_states[chat_id] = {"estado": estado, "datos": datos or {}}


def get_user_state(chat_id: int) -> Dict[str, Any]:
    return user_states.get(chat_id, {"estado": None, "datos": {}})

# =====================================================================
# 8. LLAMADAS A LA API HÉRCULES
# =====================================================================

def llamar_iniciar_consulta(tipo_consulta: int, mensaje_payload: Any) -> str:
    """
    Llama a POST /api/IniciarConsulta y devuelve el IdPeticion.

    Formatos de respuesta soportados:

    1) Formato NUEVO:
       { "IdPeticion": "guid..." }

    2) Formato ANTIGUO:
       { "Tipo": 0, "Mensaje": "guid..." }

    Además:
      - Si mensaje_payload es dict/list -> se serializa a JSON.
      - Si mensaje_payload es str -> se manda tal cual (sin json.dumps).
    """
    url = f"{API_BASE}/api/IniciarConsulta"

    if isinstance(mensaje_payload, (dict, list)):
        mensaje_str = json.dumps(mensaje_payload, ensure_ascii=False)
    else:
        mensaje_str = str(mensaje_payload)

    body = {
        "token": HERCULES_TOKEN,
        "tipo": tipo_consulta,
        "mensaje": mensaje_str,
    }

    print(f"[DEBUG] IniciarConsulta payload: {body}")

    resp = requests.post(url, json=body, timeout=30)

    try:
        resp.raise_for_status()
    except Exception:
        print(f"[ERROR] HTTP IniciarConsulta status={resp.status_code}, body={resp.text}")
        raise

    data = resp.json()
    print(f"[DEBUG] Respuesta IniciarConsulta: {data}")

    # Formato NUEVO
    id_peticion = data.get("IdPeticion") or data.get("idPeticion")
    if id_peticion:
        return str(id_peticion)

    # Formato ANTIGUO
    tipo = data.get("Tipo")
    if tipo is None:
        tipo = data.get("tipo")

    mensaje = data.get("Mensaje") or data.get("mensaje")

    if tipo == 0 and mensaje:
        return str(mensaje)

    raise RuntimeError(f"Respuesta no esperada de IniciarConsulta: {data}")


def llamar_resultados(id_peticion: str) -> dict:
    """
    Llama a GET /api/resultados/{token}/{idPeticion}
    Respuesta esperada:
      { "Tipo": 0|1|2, "Mensaje": "..." }
    """
    url = f"{API_BASE}/api/resultados/{HERCULES_TOKEN}/{id_peticion}"

    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        print(f"[ERROR] HTTP resultados status={resp.status_code}, body={resp.text}")
        resp.raise_for_status()

    data = resp.json()
    print(f"[DEBUG] Respuesta Resultados: {data}")
    return data


def es_respuesta_exitosa_hercules(data: dict) -> bool:
    """
    Determina si la respuesta de Hércules es considerada "exitosa"
    para efectos de COBRO de créditos.

    Criterio:
      - Tipo == 0 (aceptando 0 o "0")
      - Mensaje no vacío
      - Si existe 'Error' == True o 'error' con mensaje de no encontrado -> fallo
      - Si existe 'codigoResultado' y es distinto de 'EXITOSO' -> fallo
      - En cualquier otro caso con Tipo == 0 -> éxito.
    """
    try:
        if isinstance(data, str):
            data = json.loads(data)

        if not isinstance(data, dict):
            print(f"[DEBUG] es_respuesta_exitosa_hercules: data no es dict: {type(data)}")
            return False

        # 1) Validar Tipo == 0
        tipo = data.get("Tipo")
        if tipo is None:
            tipo = data.get("tipo")

        if str(tipo) != "0":
            print(f"[DEBUG] es_respuesta_exitosa_hercules: Tipo != 0 -> {tipo}")
            return False

        # 2) Extraer Mensaje
        mensaje_raw = data.get("Mensaje") or data.get("mensaje")
        if not mensaje_raw:
            print("[DEBUG] es_respuesta_exitosa_hercules: Mensaje vacío")
            return False

        if isinstance(mensaje_raw, str):
            try:
                mensaje_json = json.loads(mensaje_raw)
            except Exception:
                # No se pudo parsear, pero hay contenido y Tipo == 0 -> éxito
                print("[DEBUG] es_respuesta_exitosa_hercules: no se pudo parsear Mensaje, pero hay contenido.")
                return True
        elif isinstance(mensaje_raw, dict):
            mensaje_json = mensaje_raw
        else:
            print(f"[DEBUG] es_respuesta_exitosa_hercules: Mensaje tipo {type(mensaje_raw)}, lo aceptamos.")
            return True

        # 3) Revisar banderas de error
        if isinstance(mensaje_json, dict):
            # Error explícito en mayúscula
            if mensaje_json.get("Error") is True:
                print("[DEBUG] es_respuesta_exitosa_hercules: Error == True en mensaje_json")
                return False

            # codigoResultado distinto de EXITOSO
            codigo = mensaje_json.get("codigoResultado") or mensaje_json.get("codigo")
            if codigo and str(codigo).upper() != "EXITOSO":
                print(f"[DEBUG] es_respuesta_exitosa_hercules: codigoResultado != EXITOSO -> {codigo}")
                return False

            # error en minúscula con texto tipo "Vehiculo no encontrado"
            err_text = mensaje_json.get("error")
            if isinstance(err_text, str) and "no encontrado" in err_text.lower():
                print(f"[DEBUG] es_respuesta_exitosa_hercules: error de 'no encontrado' -> {err_text}")
                return False

        # 4) Si llegamos aquí, consideramos éxito
        return True

    except Exception as e:
        print(f"[ERROR] Analizando respuesta de Hércules: {e}")
        return False


def ejecutar_consulta_en_hilo(
    chat_id: int,
    usuario: Usuario,
    mensaje_id: int,
    tipo_consulta: int,
    mensaje_parametro_str: str,
    id_peticion: str,
    formateador_respuesta,
):
    """
    Hilo que hace polling a /resultados y decide si se cobra o no.
    Ahora también permite que el formateador devuelva (texto, firma_b64)
    para enviar la imagen de la firma en consultas tipo 8.
    """

    def _run():
        try:
            deadline = time.time() + RESULTADOS_TIMEOUT
            ultimo_data = None

            while time.time() < deadline:
                data = llamar_resultados(id_peticion)
                ultimo_data = data

                tipo = data.get("Tipo")
                if tipo is None:
                    tipo = data.get("tipo")

                mensaje = data.get("Mensaje") or data.get("mensaje")

                print(
                    f"[DEBUG] Resultado parcial "
                    f"(tipo={tipo_consulta}, mensaje='{mensaje_parametro_str}') -> "
                    f"Tipo={tipo}, Mensaje={mensaje}"
                )

                # Tipo 2 -> procesando
                if tipo == 2:
                    time.sleep(RESULTADOS_INTERVALO)
                    continue

                # Tipo 0 / 1 -> respuesta final
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

                # formateador puede devolver:
                #  - solo texto (str)
                #  - (texto, firma_b64)
                resultado_formateo = formateador_respuesta(ultimo_data)

                texto_respuesta = textos.MENSAJE_ERROR_GENERICO
                firma_b64 = None

                if isinstance(resultado_formateo, tuple):
                    if len(resultado_formateo) >= 1:
                        texto_respuesta = resultado_formateo[0]
                    if len(resultado_formateo) >= 2:
                        firma_b64 = resultado_formateo[1]
                else:
                    texto_respuesta = resultado_formateo

                enviar_mensaje(chat_id, texto_respuesta)

                # Si hay firma en base64, la enviamos como documento
                if firma_b64:
                    enviar_documento_firma_desde_b64(chat_id, firma_b64)

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

    threading.Thread(target=_run, daemon=True).start()

# =====================================================================
# 9. FORMATEADORES DE RESPUESTA
# =====================================================================

def formatear_respuesta_firma(data: dict):
    """
    Formatea la respuesta de consulta de firma.

    Soporta:
      1) Formato "viejo": {"person": {...}} o {"persona": {...}}
         con nombre1, nombre2, apellido1, apellido2, etc.
      2) Formato "nuevo": {"nombres": "...", "apellidos": "...",
                           "grupoSanguineo": "...", "sexo": "...",
                           "fechaNacimiento": "...", "lugarNacimiento": "...",
                           "firma": "base64...", ...}
         directamente en la raíz del JSON.

    Devuelve:
      - Solo texto (str)   -> para compatibilidad.
      - (texto, firma_b64) -> si encuentra la firma en base64.
    """
    try:
        mensaje_raw = data.get("Mensaje") or data.get("mensaje") or ""
        print(f"[DEBUG] formatear_respuesta_firma.mensaje_raw (tipo={type(mensaje_raw)}): {mensaje_raw}")

        # 1) Normalizar a dict
        info = {}
        if isinstance(mensaje_raw, str):
            try:
                info = json.loads(mensaje_raw)
            except Exception as e:
                print(f"[ERROR] formatear_respuesta_firma: no se pudo json.loads(mensaje_raw): {e}")
                # Devolvemos texto crudo
                texto = (
                    "📝 *Resultado de consulta de firma (sin formato JSON)*\n\n"
                    f"`{mensaje_raw}`"
                )
                return texto
        elif isinstance(mensaje_raw, dict):
            info = mensaje_raw
        else:
            print(f"[DEBUG] formatear_respuesta_firma: mensaje_raw tipo inesperado: {type(mensaje_raw)}")
            texto = (
                "📝 *Resultado de consulta de firma (formato no esperado)*\n\n"
                f"`{str(mensaje_raw)}`"
            )
            return texto

        print(f"[DEBUG] formatear_respuesta_firma.info (tipo={type(info)}): {info}")

        # 2) Intentar formato "viejo": person/persona
        person = info.get("person") or info.get("persona") or {}
        if person:
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
            firma_b64 = person.get("firma") or info.get("firma")
        else:
            # 3) Formato "nuevo": campos en la raíz
            nombres = info.get("nombres") or ""
            apellidos = info.get("apellidos") or ""
            nombre = f"{nombres} {apellidos}".strip()

            tipo_doc = (
                info.get("tipoDocumento")
                or info.get("idTipoDoc")
                or info.get("tipoDoc")
                or ""
            )
            nro_doc = (
                info.get("numeroDocumento")
                or info.get("nroDocumento")
                or info.get("nroDoc")
                or ""
            )
            firma_b64 = info.get("firma")

        grupo = info.get("grupoSanguineo") or "-"
        sexo = info.get("sexo") or "-"
        lugar_nac = info.get("lugarNacimiento") or "-"

        fecha_nac_raw = info.get("fechaNacimiento")
        fecha_nac_fmt = "-"
        if isinstance(fecha_nac_raw, str) and len(fecha_nac_raw) >= 10:
            fecha_nac_fmt = fecha_nac_raw[:10]

        texto = (
            "📝 *Resultado de consulta de firma*\n\n"
            f"*Nombre:* {nombre or '-'}\n"
            f"*Documento:* {tipo_doc} {nro_doc}\n"
            f"*Sexo:* {sexo}\n"
            f"*Grupo sanguíneo:* {grupo}\n"
            f"*Fecha de nacimiento:* {fecha_nac_fmt}\n"
            f"*Lugar de nacimiento:* {lugar_nac}\n"
        )

        print(f"[DEBUG] formatear_respuesta_firma.texto: {texto!r}")

        # Si tenemos firma, la devolvemos también
        if firma_b64:
            return texto, firma_b64

        return texto

    except Exception as e:
        print(f"[ERROR] formateando firma: {e}")
        return textos.MENSAJE_ERROR_GENERICO


def formatear_respuesta_persona(data: dict) -> str:
    try:
        mensaje_str = data.get("Mensaje") or data.get("mensaje") or ""
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
            "🧍 *Consulta de persona*\n\n"
            f"*Nombre:* {nombre or '-'}\n"
            f"*Documento:* {tipo_doc} {nro_doc}\n"
        )
    except Exception as e:
        print(f"[ERROR] formateando persona: {e}")
        return textos.MENSAJE_ERROR_GENERICO


def formatear_respuesta_vehiculo(data: dict) -> str:
    """
    Formatea la respuesta de vehículo para mostrarla en Telegram.

    Soporta estas estructuras de Mensaje:
      1) {"vehiculo": {"datos": {...}, "adicional": {...}}}
      2) {"datos": {...}, "adicional": {...}}
      3) {"placaNumeroUnicoIdentificacion": "...", ...} (campos en la raíz)
    """
    try:
        mensaje_raw = data.get("Mensaje") or data.get("mensaje") or ""

        print(f"[DEBUG] formatear_respuesta_vehiculo.mensaje_raw (tipo={type(mensaje_raw)}): {mensaje_raw}")

        info = {}
        if isinstance(mensaje_raw, str):
            try:
                info = json.loads(mensaje_raw)
            except Exception as e:
                print(f"[ERROR] formatear_respuesta_vehiculo: no se pudo json.loads(mensaje_raw): {e}")
                return (
                    "🚗 *Respuesta de vehículo (sin formato JSON)*\n\n"
                    f"`{mensaje_raw}`"
                )
        elif isinstance(mensaje_raw, dict):
            info = mensaje_raw
        else:
            print(f"[DEBUG] formatear_respuesta_vehiculo: mensaje_raw tipo inesperado: {type(mensaje_raw)}")
            return (
                "🚗 *Respuesta de vehículo (formato no esperado)*\n\n"
                f"`{str(mensaje_raw)}`"
            )

        print(f"[DEBUG] formatear_respuesta_vehiculo.info (tipo={type(info)}): {info}")

        datos = {}

        if isinstance(info, dict) and "datos" in info and isinstance(info["datos"], dict):
            datos = info["datos"]
            print("[DEBUG] formatear_respuesta_vehiculo: usando info['datos']")
        else:
            veh = info.get("vehiculo")
            if isinstance(veh, dict):
                if "datos" in veh and isinstance(veh["datos"], dict):
                    datos = veh["datos"]
                    print("[DEBUG] formatear_respuesta_vehiculo: usando info['vehiculo']['datos']")
                else:
                    datos = veh
                    print("[DEBUG] formatear_respuesta_vehiculo: usando info['vehiculo'] directo")
            else:
                if any(
                    k in info
                    for k in (
                        "placaNumeroUnicoIdentificacion",
                        "placa",
                        "marcaVehiculo",
                        "lineaVehiculo",
                    )
                ):
                    datos = info
                    print("[DEBUG] formatear_respuesta_vehiculo: usando info directo (campos en raíz)")
                else:
                    print("[DEBUG] formatear_respuesta_vehiculo: no se encontró 'datos' ni 'vehiculo' adecuados")

        datos = datos or {}
        print(f"[DEBUG] formatear_respuesta_vehiculo.datos: {datos}")

        placa = (
            datos.get("placaNumeroUnicoIdentificacion")
            or datos.get("placa")
            or "-"
        )
        marca = datos.get("marcaVehiculo") or "-"
        linea = datos.get("lineaVehiculo") or "-"
        modelo = datos.get("modelo") or "-"
        color = datos.get("color") or "-"
        servicio = datos.get("servicio") or "-"
        clase = datos.get("claseVehiculo") or "-"

        return (
            "🚗 *Información del vehículo*\n\n"
            f"*Placa:* {placa}\n"
            f"*Marca:* {marca}\n"
            f"*Línea:* {linea}\n"
            f"*Modelo:* {modelo}\n"
            f"*Color:* {color}\n"
            f"*Clase:* {clase}\n"
            f"*Servicio:* {servicio}\n"
        )

    except Exception as e:
        print(f"[ERROR] formateando vehículo: {e}")
        return textos.MENSAJE_ERROR_GENERICO


def formatear_respuesta_propietario(data: dict) -> str:
    try:
        mensaje_str = data.get("Mensaje") or data.get("mensaje") or ""
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
            "👤 *Propietario del vehículo*\n\n"
            f"*Nombre / Razón social:* {nombre_persona}\n"
            f"*Documento:* {tipo_doc} {nro_doc}\n"
        )
    except Exception as e:
        print(f"[ERROR] formateando propietario: {e}")
        return textos.MENSAJE_ERROR_GENERICO

# =====================================================================
# 10. LÓGICA DE NEGOCIO: INICIAR CONSULTAS
# =====================================================================

def _verificar_creditos_o_mensaje(chat_id: int, usuario: Usuario, config: ConsultaConfig) -> bool:
    """
    Devuelve True si el usuario tiene créditos y la consulta está ACTIVA.
    En caso contrario envía el mensaje correspondiente y devuelve False.
    """
    if not config or config.estado_consulta != "ACTIVA":
        enviar_mensaje(chat_id, "⚠️ Esta consulta está deshabilitada.")
        return False

    db = get_db()
    try:
        usuario_db = db.query(Usuario).filter_by(id=usuario.id).one()
        disponibles = usuario_creditos_disponibles(usuario_db)
        if disponibles < config.valor_consulta:
            enviar_mensaje(chat_id, textos.MENSAJE_SIN_CREDITOS)
            return False
    finally:
        db.close()

    return True


def iniciar_consulta_firma(usuario: Usuario, chat_id: int, tipo_doc: str, num_doc: str):
    """
    Para tipo 8, la API espera:
      "mensaje": "CC,15645123"
    """
    config = get_consulta_config(TIPO_CONSULTA_FIRMA)
    if not _verificar_creditos_o_mensaje(chat_id, usuario, config):
        return

    # Formato requerido por la API: "CC,15645123"
    mensaje_payload = f"{tipo_doc},{num_doc}"

    try:
        id_peticion = llamar_iniciar_consulta(TIPO_CONSULTA_FIRMA, mensaje_payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_firma -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    # Guardamos parámetros de forma "humana" en la BD
    parametros_guardar = {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc}

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_FIRMA,
        nombre_servicio="firma",
        parametros=parametros_guardar,
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_FIRMA,
        mensaje_parametro_str=mensaje_payload,
        id_peticion=id_peticion,
        formateador_respuesta=formatear_respuesta_firma,
    )


def iniciar_consulta_persona(usuario: Usuario, chat_id: int, tipo_doc: str, num_doc: str):
    config = get_consulta_config(TIPO_CONSULTA_PERSONA)
    if not _verificar_creditos_o_mensaje(chat_id, usuario, config):
        return

    mensaje_payload = {"tipoDocumento": tipo_doc, "numeroDocumento": num_doc}

    try:
        id_peticion = llamar_iniciar_consulta(TIPO_CONSULTA_PERSONA, mensaje_payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_persona -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_PERSONA,
        nombre_servicio="persona",
        parametros=mensaje_payload,
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_PERSONA,
        mensaje_parametro_str=json.dumps(mensaje_payload, ensure_ascii=False),
        id_peticion=id_peticion,
        formateador_respuesta=formatear_respuesta_persona,
    )


def iniciar_consulta_vehiculo(usuario: Usuario, chat_id: int, placa: str):
    """
    Consulta de vehículo por placa (tipo 3).
    En IniciarConsulta la API espera:
      "mensaje": "PDK400"
    (solo la placa, no JSON).
    """
    config = get_consulta_config(TIPO_CONSULTA_VEHICULO_SOLO)
    if not _verificar_creditos_o_mensaje(chat_id, usuario, config):
        return

    placa_limpia = placa.replace(" ", "").upper()
    mensaje_payload = placa_limpia

    try:
        id_peticion = llamar_iniciar_consulta(TIPO_CONSULTA_VEHICULO_SOLO, mensaje_payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_vehiculo -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_SOLO,
        nombre_servicio="vehiculo_placa",
        parametros={"placa": placa_limpia},
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_SOLO,
        mensaje_parametro_str=placa_limpia,
        id_peticion=id_peticion,
        formateador_respuesta=formatear_respuesta_vehiculo,
    )


def iniciar_consulta_propietario(usuario: Usuario, chat_id: int, placa: str):
    """
    Consulta de propietario por placa (tipo 4).
    La API espera también solo la placa como string.
    """
    config = get_consulta_config(TIPO_CONSULTA_PROPIETARIO_POR_PLACA)
    if not _verificar_creditos_o_mensaje(chat_id, usuario, config):
        return

    placa_limpia = placa.replace(" ", "").upper()
    mensaje_payload = placa_limpia

    try:
        id_peticion = llamar_iniciar_consulta(TIPO_CONSULTA_PROPIETARIO_POR_PLACA, mensaje_payload)
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
        mensaje_parametro_str=placa_limpia,
        id_peticion=id_peticion,
        formateador_respuesta=formatear_respuesta_propietario,
    )

# =====================================================================
# 11. FLASK + WEBHOOK TELEGRAM
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

    # leemos estado actual del usuario
    estado_info = get_user_state(chat_id)
    estado = estado_info.get("estado")
    datos_estado = estado_info.get("datos", {})

    # ----------------- COMANDOS -------------------
    if text.startswith("/start"):
        enviar_mensaje(chat_id, textos.MENSAJE_BIENVENIDA, reply_markup=teclado_menu_principal())
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

    # ----------------- MENÚ PRINCIPAL -------------------
    if text == "📝 Consulta de firma":
        enviar_mensaje(
            chat_id,
            "✍️ Has elegido *Consulta de firma*.\n\n"
            "Primero selecciona el *tipo de documento*: 👇",
            reply_markup=teclado_tipos_documento(),
        )
        set_user_state(chat_id, "firma_esperando_tipo_doc")
        return jsonify({"ok": True}), 200

    if text == "🧍 Consulta de persona":
        enviar_mensaje(
            chat_id,
            "🧍 Has elegido *Consulta de persona*.\n\n"
            "Primero selecciona el *tipo de documento*: 👇",
            reply_markup=teclado_tipos_documento(),
        )
        set_user_state(chat_id, "persona_esperando_tipo_doc")
        return jsonify({"ok": True}), 200

    if text == "🚗 Consulta de vehículo":
        enviar_mensaje(
            chat_id,
            "🚗 Has elegido *Consulta de vehículo por placa*.\n\n"
            "👉 Escribe ahora la placa del vehículo (ejemplo: `ABC123`).",
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_vehiculo")
        return jsonify({"ok": True}), 200

    if text == "👤 Propietario por placa":
        enviar_mensaje(
            chat_id,
            "👤 Has elegido *Propietario por placa*.\n\n"
            "👉 Escribe ahora la placa del vehículo.",
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, "esperando_placa_propietario")
        return jsonify({"ok": True}), 200

    if text == "⬅ Volver al menú":
        enviar_mensaje(
            chat_id,
            "Volviendo al menú principal…",
            reply_markup=teclado_menu_principal(),
        )
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    # ----------------- BOTONES DE TIPO DE DOCUMENTO -------------------
    if text in ("CC - Cédula", "TI - Tarjeta de identidad", "NIT - NIT"):
        tipo_doc = text.split()[0].upper()

        if estado == "firma_esperando_tipo_doc":
            set_user_state(chat_id, "firma_esperando_num_doc", {"tipo_doc": tipo_doc})
            enviar_mensaje(
                chat_id,
                f"✍️ Has elegido *firma* con documento tipo *{tipo_doc}*.\n\n"
                "👉 Escribe ahora el *número de documento* (sin puntos ni comas).",
            )
            return jsonify({"ok": True}), 200

        if estado == "persona_esperando_tipo_doc":
            set_user_state(chat_id, "persona_esperando_num_doc", {"tipo_doc": tipo_doc})
            enviar_mensaje(
                chat_id,
                f"🧍 Has elegido *persona* con documento tipo *{tipo_doc}*.\n\n"
                "👉 Escribe ahora el *número de documento* (sin puntos ni comas).",
            )
            return jsonify({"ok": True}), 200

        enviar_mensaje(
            chat_id,
            "Primero elige el tipo de consulta (firma o persona) en el menú principal.",
            reply_markup=teclado_menu_principal(),
        )
        return jsonify({"ok": True}), 200

    # ----------------- LÓGICA SEGÚN ESTADO -------------------
    if estado == "firma_esperando_num_doc":
        tipo_doc = datos_estado.get("tipo_doc", "CC")
        num_doc = text.replace(" ", "")
        iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
        set_user_state(chat_id, None)
        return jsonify({"ok": True}), 200

    if estado == "persona_esperando_num_doc":
        tipo_doc = datos_estado.get("tipo_doc", "CC")
        num_doc = text.replace(" ", "")
        iniciar_consulta_persona(usuario, chat_id, tipo_doc, num_doc)
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

    # ----------------- MODO RÁPIDO (firma: CC 123456) -------------------
    if estado is None and text.upper().startswith(("CC ", "TI ", "CE ", "NIT ")):
        partes = text.split()
        if len(partes) >= 2:
            tipo_doc = partes[0].upper()
            num_doc = partes[1]
            iniciar_consulta_firma(usuario, chat_id, tipo_doc, num_doc)
            return jsonify({"ok": True}), 200

    # ----------------- MENSAJE POR DEFECTO -------------------
    enviar_mensaje(
        chat_id,
        "No entendí tu mensaje.\n\n"
        "Usa el menú de abajo o el modo rápido para firma: `CC 123456789`.",
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
