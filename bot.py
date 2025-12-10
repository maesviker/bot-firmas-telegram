import os
import json
import time
import threading
import base64
import io
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

# ReportLab y QR
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import mm
import qrcode

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


def enviar_documento_pdf(chat_id: int, nombre_archivo: str, pdf_bytes: bytes):
    """
    Envía un PDF a Telegram como documento, usando bytes en memoria.
    """
    try:
        files = {
            "document": (nombre_archivo, pdf_bytes, "application/pdf")
        }
        data = {
            "chat_id": chat_id,
            "caption": "📄 Informe vehicular generado",
        }

        resp = requests.post(
            f"{TELEGRAM_API_URL}/sendDocument",
            data=data,
            files=files,
            timeout=30,
        )
        resp.raise_for_status()
        print("[DEBUG] PDF vehicular enviado como documento a Telegram")
    except Exception as e:
        print(f"[ERROR] Enviando PDF vehicular a Telegram: {e}")


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

# =====================================================================
# 9. GENERACIÓN DE INFORME VEHICULAR (PDF B7 v2)
# =====================================================================

def generar_informe_vehicular_B7_v2(data: dict, qr_url: str = "https://t.me/QuantumFBot") -> bytes:
    """
    Genera el informe vehicular en PDF (plantilla B7 v2) en memoria
    y devuelve los bytes del archivo.

    Soporta que 'Mensaje' venga como string JSON o como dict ya deserializado.
    """
    mensaje_raw = data.get("Mensaje") or data.get("mensaje") or ""

    # --- NUEVO: soportar string o dict en Mensaje ---
    info = {}
    if isinstance(mensaje_raw, str):
        try:
            info = json.loads(mensaje_raw)
        except Exception as e:
            print(f"[ERROR] generar_informe_vehicular_B7_v2: no se pudo json.loads(Mensaje): {e}")
            info = {}
    elif isinstance(mensaje_raw, dict):
        info = mensaje_raw
    else:
        print(f"[WARN] generar_informe_vehicular_B7_v2: Mensaje es tipo inesperado: {type(mensaje_raw)}")
        info = {}

    # Soportar dos estructuras:
    # 1) {"vehiculo": {"datos":..., "adicional":...}, "persona": {...}}
    # 2) {"datos":..., "adicional":...} (solo vehículo)
    vehiculo_info = info.get("vehiculo") or {}
    if not vehiculo_info and isinstance(info, dict) and "datos" in info:
        vehiculo_info = {
            "datos": info.get("datos") or {},
            "adicional": info.get("adicional") or {},
        }

    datos_vehiculo = vehiculo_info.get("datos", {}) or {}
    adicional = vehiculo_info.get("adicional", {}) or {}

    persona_info = info.get("persona", {}) or {}
    person = persona_info.get("person", {}) or {}
    ubic_list = persona_info.get("ubicabilidad") or []
    ubic = ubic_list[0] if ubic_list else {}

    # ==========================
    # 1. Datos vehículo
    # ==========================
    placa = datos_vehiculo.get("placaNumeroUnicoIdentificacion", "-")
    clase = datos_vehiculo.get("claseVehiculo", "-")
    servicio = datos_vehiculo.get("servicio", "-")
    estado_registro = datos_vehiculo.get("estadoRegistroVehiculo", "-")

    marca = datos_vehiculo.get("marcaVehiculo", "-")
    linea = datos_vehiculo.get("lineaVehiculo", "-")
    modelo = datos_vehiculo.get("modelo", "-")
    color = datos_vehiculo.get("color", "-")
    carroceria = datos_vehiculo.get("carroceria", "-")
    cilindraje = datos_vehiculo.get("cilindraje", "-")

    nro_serie = "-"
    vin = datos_vehiculo.get("vin", "-")
    numero_motor = datos_vehiculo.get("numeroMotor", "-")
    numero_chasis = datos_vehiculo.get("numeroChasis", "-")

    importado = datos_vehiculo.get("origenRegistro", "-")
    radio_accion = "-"
    nivel_servicio = "-"
    tipo_combustible = (
        datos_vehiculo.get("tipoCombustible")
        or datos_vehiculo.get("combustible")
        or datos_vehiculo.get("tipoCombustibleVehiculo")
        or "-"
    )
    estado_vehiculo = estado_registro
    modalidad_servicio = "-"

    regrab_motor = "-"
    regrab_chasis = "-"
    regrab_serie = "-"
    regrab_vin = "-"

    tiene_gravamen = datos_vehiculo.get("poseeGravamenes", "-")
    vehiculo_rematado = "-"
    medidas_cautelares = "-"

    transmision = datos_vehiculo.get("tipoTransmision", "-")
    traccion = datos_vehiculo.get("tipoTraccion", "-")
    nivel_emisiones = datos_vehiculo.get("nivelEmisiones", "-")

    aspiracion = datos_vehiculo.get("tipoAspiracion", "-")
    freno = datos_vehiculo.get("tipoFreno", "-")

    info_veh_dto = adicional.get("informacionVehiculoDTO", {}) or {}
    blindado = info_veh_dto.get("blindado", "-")

    # ==========================
    # 2. Propietario
    # ==========================
    nombre_prop = " ".join(
        [
            person.get("nombre1", ""),
            person.get("nombre2", ""),
            person.get("apellido1", ""),
            person.get("apellido2", ""),
        ]
    ).strip() or "-"

    prop_tipo_doc = person.get("idTipoDoc") or person.get("tipoDocumento") or "-"
    prop_num_doc = person.get("nroDocumento") or person.get("numeroDocumento") or "-"

    direccion = ubic.get("direccion", "-")
    municipio_raw = ubic.get("municipio", "") or "-"
    if " - " in municipio_raw:
        ciudad, departamento = [x.strip() for x in municipio_raw.split(" - ", 1)]
    else:
        ciudad = municipio_raw
        departamento = "-"

    telefono = ubic.get("telefono") or person.get("celular") or "-"
    correo = ubic.get("correoElectronico") or person.get("email") or "-"

    # ==========================
    # 3. Licencias
    # ==========================
    licencias_list = []
    lista_comparendos = adicional.get("listaComparendos") or []
    if lista_comparendos:
        comp = lista_comparendos[0]
        for lic in comp.get("listaLicencias") or []:
            licencias_list.append(
                {
                    "numero": lic.get("numeroLicencia", "") or "",
                    "categoria": lic.get("categoria", "") or "",
                    "fechaExpedicion": lic.get("fechaExpedicion", "") or "",
                    "fechaVencimiento": lic.get("fechaVencimiento", "") or "",
                    "estado": lic.get("estado", "") or "",
                }
            )

    # ==========================
    # 4. SOAT y RTM
    # ==========================
    lista_polizas = adicional.get("listaPolizas") or []
    soat_list = [
        p for p in lista_polizas
        if (p.get("tipoPoliza", "") or "").upper() == "SOAT"
    ]

    def calcular_vigente(fecha_str, formato="%d/%m/%Y"):
        try:
            fecha = datetime.strptime(fecha_str, formato).date()
            hoy = datetime.now().date()
            return "SI" if fecha >= hoy else "NO"
        except Exception:
            return "-"

    soat_vigente = "SI" if any(
        calcular_vigente(p.get("fechaVencimiento", "")) == "SI" for p in soat_list
    ) else "NO"

    rtm_list = adicional.get("listaRtm") or []
    rtm_vigente = "SI" if any(
        calcular_vigente(r.get("fechaVigencia", "")) == "SI" for r in rtm_list
    ) else "NO"

    inscrito_runt = datos_vehiculo.get("vehiculoInscritoRUNT", "-")
    gravamenes = datos_vehiculo.get("poseeGravamenes", "-")
    tarjeta_servicios = datos_vehiculo.get("numeroTarjetaServicios", "-")
    tarjeta_vence = datos_vehiculo.get("fechaVencimientoTarjetaServicios", "-") or "-"

    # ==========================
    # 5. Estilos ReportLab
    # ==========================
    styles = getSampleStyleSheet()

    section_title_style = ParagraphStyle(
        name="SectionTitle",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        textColor=colors.HexColor("#003366"),
        spaceBefore=6,
        spaceAfter=4,
    )
    normal_style = styles["Normal"]
    normal_style.fontSize = 9

    small_style = ParagraphStyle(
        name="Small",
        parent=styles["Normal"],
        fontSize=8,
    )

    def cell(t):
        return Paragraph(str(t if t is not None else "-"), normal_style)

    def cell_small(t):
        return Paragraph(str(t if t is not None else "-"), small_style)

    # ==========================
    # 6. QR en imagen temporal
    # ==========================
    qr_path = "qr_temp_informe_vehicular.png"
    qr_obj = qrcode.QRCode(box_size=8, border=1)
    qr_obj.add_data(qr_url)
    qr_obj.make(fit=True)
    img = qr_obj.make_image(fill_color="black", back_color="white")
    img.save(qr_path)

    # ==========================
    # 7. Construir PDF en memoria
    # ==========================
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=45 * mm,
        bottomMargin=15 * mm,
    )
    total_width = doc.width
    story = []

    # ---------- 1. Datos principales ----------
    story.append(Paragraph("1. Datos principales del vehículo", section_title_style))
    tabla_datos_principales = Table(
        [
            [cell("Placa"), cell(placa), cell("Clase"), cell(clase)],
            [cell("Servicio"), cell(servicio), cell("Estado del registro"), cell(estado_registro)],
        ],
        colWidths=[total_width / 4] * 4,
    )
    tabla_datos_principales.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEEEEE")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ]
        )
    )
    story.append(tabla_datos_principales)
    story.append(Spacer(1, 6))

    # ---------- 2. Información de propietario ----------
    story.append(Paragraph("2. Información de propietario", section_title_style))

    tabla_propietario = Table(
        [
            [cell("Nombre / Razón social"), cell(nombre_prop)],
            [cell("Tipo y número de documento"), cell(f"{prop_tipo_doc} {prop_num_doc}")],
        ],
        colWidths=[total_width / 2, total_width / 2],
    )
    tabla_propietario.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F5F5F5")),
                ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#003366")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ]
        )
    )
    story.append(tabla_propietario)
    story.append(Spacer(1, 4))

    tabla_ubicacion = Table(
        [
            [cell("Departamento"), cell(departamento), cell("Ciudad"), cell(ciudad)],
            [cell("Dirección"), cell(direccion), cell("Teléfono"), cell(telefono)],
            [cell("Correo"), cell(correo), cell(""), cell("")],
        ],
        colWidths=[total_width / 4] * 4,
    )
    tabla_ubicacion.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEEEEE")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ]
        )
    )
    story.append(tabla_ubicacion)
    story.append(Spacer(1, 4))

    # Licencias solo si existen
    if licencias_list:
        tabla_lic_title = Table(
            [[Paragraph("<b>LICENCIAS DE CONDUCCIÓN</b>", normal_style)]],
            colWidths=[total_width],
        )
        tabla_lic_title.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#DDDDDD")),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        story.append(tabla_lic_title)

        lic_header = [
            cell_small("No. licencia"),
            cell_small("Categoría"),
            cell_small("Fecha expedición"),
            cell_small("Fecha vencimiento"),
            cell_small("Estado"),
        ]
        lic_rows = [lic_header]
        for lic in licencias_list:
            lic_rows.append(
                [
                    cell_small(lic["numero"]),
                    cell_small(lic["categoria"]),
                    cell_small(lic["fechaExpedicion"]),
                    cell_small(lic["fechaVencimiento"]),
                    cell_small(lic["estado"]),
                ]
            )
        tabla_lic = Table(
            lic_rows,
            colWidths[
                :
            ] if False else [
                total_width * 0.18,
                total_width * 0.12,
                total_width * 0.20,
                total_width * 0.20,
                total_width * 0.30,
            ],
        )
        tabla_lic.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F5F5F5")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(tabla_lic)
        story.append(Spacer(1, 6))

    # ---------- 3. Características del vehículo ----------
    story.append(Paragraph("3. Características del vehículo", section_title_style))
    tabla_caracteristicas = Table(
        [
            [cell("Marca"), cell(marca), cell("Modelo"), cell(modelo)],
            [cell("Línea"), cell(linea), cell("Clase"), cell(clase)],
            [cell("Color"), cell(color), cell("Carrocería"), cell(carroceria)],
            [cell("Cilindraje"), cell(cilindraje), cell("Tipo de combustible"), cell(tipo_combustible)],
            [cell("Nro. Serie"), cell(nro_serie), cell("Nro. VIN"), cell(vin)],
            [cell("Nro. Motor"), cell(numero_motor), cell("Nro. Chasis"), cell(numero_chasis)],
            [cell("Importado"), cell(importado), cell("Radio acción"), cell(radio_accion)],
            [cell("Nivel servicio"), cell(nivel_servicio), cell("Transmisión"), cell(transmision)],
            [cell("Tracción"), cell(traccion), cell("Nivel de emisiones"), cell(nivel_emisiones)],
            [cell("Estado del vehículo"), cell(estado_vehiculo), cell("Modalidad servicio"), cell(modalidad_servicio)],
            [cell("Regrabación motor"), cell(regrab_motor), cell("Regrabación chasis"), cell(regrab_chasis)],
            [cell("Regrabación serie"), cell(regrab_serie), cell("Regrabación VIN"), cell(regrab_vin)],
            [cell("Tiene gravamen"), cell(tiene_gravamen), cell("Vehículo rematado"), cell(vehiculo_rematado)],
            [cell("Tiene medidas cautelares"), cell(medidas_cautelares), cell(""), cell("")],
        ],
        colWidths=[total_width / 4] * 4,
    )
    tabla_caracteristicas.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEEEEE")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ]
        )
    )
    story.append(tabla_caracteristicas)
    story.append(Spacer(1, 6))

    # ---------- Forzar sección 4 a nueva página ----------
    story.append(PageBreak())

    # ---------- 4. Estado de documentos y seguridad ----------
    story.append(Paragraph("4. Estado de documentos y seguridad", section_title_style))
    tabla_docs_resumen = Table(
        [
            [cell("Inscrito en RUNT"), cell(inscrito_runt), cell("Gravámenes"), cell(gravamenes)],
            [cell("Tarjeta de servicios"), cell(tarjeta_servicios), cell("Vigencia tarjeta"), cell(tarjeta_vence)],
            [cell("SOAT vigente"), cell(soat_vigente), cell("RTM vigente"), cell(rtm_vigente)],
        ],
        colWidths=[total_width / 4] * 4,
    )
    tabla_docs_resumen.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEEEEE")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ]
        )
    )
    story.append(tabla_docs_resumen)
    story.append(Spacer(1, 4))

    if soat_list:
        tabla_soat_title = Table(
            [[Paragraph("<b>SOAT</b>", normal_style)]],
            colWidths=[total_width],
        )
        tabla_soat_title.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#DDDDDD")),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        story.append(tabla_soat_title)

        soat_header = [
            cell_small("No. Póliza"),
            cell_small("Fecha inicio vigencia"),
            cell_small("Fecha fin vigencia"),
            cell_small("Entidad que expide SOAT"),
            cell_small("Vigente"),
        ]
        soat_rows = [soat_header]
        for pol in soat_list:
            soat_rows.append(
                [
                    cell_small(pol.get("numeroPoliza", "-")),
                    cell_small(pol.get("fechaInicio", "-")),
                    cell_small(pol.get("fechaVencimiento", "-")),
                    cell_small(pol.get("aseguradora", "-")),
                    cell_small(calcular_vigente(pol.get("fechaVencimiento", "-"))),
                ]
            )
        tabla_soat = Table(
            soat_rows,
            colWidths=[
                total_width * 0.20,
                total_width * 0.16,
                total_width * 0.16,
                total_width * 0.33,
                total_width * 0.15,
            ],
        )
        tabla_soat.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F5F5F5")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(tabla_soat)
        story.append(Spacer(1, 4))

    if rtm_list:
        tabla_rtm_title = Table(
            [[Paragraph("<b>REVISIÓN TÉCNICO MECÁNICA</b>", normal_style)]],
            colWidths=[total_width],
        )
        tabla_rtm_title.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#DDDDDD")),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        story.append(tabla_rtm_title)

        rtm_header = [
            cell_small("Tipo de revisión"),
            cell_small("Fecha expedición"),
            cell_small("Fecha vigencia"),
            cell_small("CDA expide RTM"),
            cell_small("Vigente"),
        ]
        rtm_rows = [rtm_header]
        for r in rtm_list:
            rtm_rows.append(
                [
                    cell_small(r.get("tipoRevision", "-")),
                    cell_small(r.get("fechaExpedicion", "-")),
                    cell_small(r.get("fechaVigencia", "-")),
                    cell_small(r.get("nombreCda", "-")),
                    cell_small(calcular_vigente(r.get("fechaVigencia", "-"))),
                ]
            )
        tabla_rtm = Table(
            rtm_rows,
            colWidths=[
                total_width * 0.25,
                total_width * 0.18,
                total_width * 0.18,
                total_width * 0.24,
                total_width * 0.15,
            ],
        )
        tabla_rtm.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F5F5F5")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#003366")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(tabla_rtm)
        story.append(Spacer(1, 6))

    # ---------- 5. Información adicional ----------
    story.append(Paragraph("5. Información adicional del vehículo", section_title_style))
    tabla_extra = Table(
        [
            [cell("Aspiración"), cell(aspiracion)],
            [cell("Tipo de freno"), cell(freno)],
            [cell("Blindado"), cell(blindado)],
        ],
        colWidths=[total_width / 3, total_width * 2 / 3],
    )
    tabla_extra.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F5F5F5")),
                ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#003366")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ]
        )
    )
    story.append(tabla_extra)
    story.append(Spacer(1, 10))

    disclaimer = Paragraph(
        "<font size='7' color='#555555'>Este informe es generado por un sistema interno de consultas "
        "y no sustituye documentos oficiales de tránsito ni certificados expedidos por autoridades competentes.</font>",
        styles["Normal"],
    )
    story.append(disclaimer)

    # ==========================
    # 8. Encabezado y pie
    # ==========================
    def draw_header_and_footer(canvas, doc_obj):
        canvas.saveState()
        page_width, page_height = doc_obj.pagesize
        left = doc_obj.leftMargin
        top_margin = doc_obj.topMargin
        frame_top = page_height - top_margin

        title_y = frame_top + 10 * mm
        subtitle_y = frame_top + 4 * mm
        date_y = subtitle_y - 3 * mm

        canvas.setFont("Helvetica-Bold", 18)
        canvas.setFillColor(colors.HexColor("#003366"))
        canvas.drawString(left, title_y, "INFORME VEHICULAR")

        canvas.setFont("Helvetica", 9)
        canvas.setFillColor(colors.HexColor("#555555"))
        canvas.drawString(left, subtitle_y, "Reporte generado por sistema de consultas Hércules")

        fecha_text = f"Fecha de emisión: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        canvas.setFont("Helvetica", 8)
        text_width = canvas.stringWidth(fecha_text, "Helvetica", 8)
        canvas.drawString(page_width - doc_obj.rightMargin - text_width, date_y, fecha_text)

        qr_size = 30 * mm
        qr_x = page_width - doc_obj.rightMargin - qr_size
        qr_y = frame_top + 5 * mm
        canvas.drawImage(qr_path, qr_x, qr_y, qr_size, qr_size, preserveAspectRatio=True, mask="auto")

        canvas.setStrokeColor(colors.HexColor("#003366"))
        canvas.setLineWidth(1)
        canvas.line(doc_obj.leftMargin, frame_top, page_width - doc_obj.rightMargin, frame_top)

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.black)
        page_num = canvas.getPageNumber()
        canvas.drawCentredString(page_width / 2.0, 10 * mm, f"Página {page_num}")

        canvas.restoreState()

    doc.build(story, onFirstPage=draw_header_and_footer, onLaterPages=draw_header_and_footer)

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

# =====================================================================
# 10. FORMATEADORES DE RESPUESTA (TEXTO TELEGRAM)
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
    IMPORTANTE: no mezclar varios campos en la misma línea.
    Cada campo va en su propia línea.
    """
    try:
        mensaje_raw = data.get("Mensaje") or data.get("mensaje") or ""
        print(f"[DEBUG] formatear_respuesta_vehiculo.mensaje_raw (tipo={type(mensaje_raw)}): {mensaje_raw}")

        # --- Parsear mensaje: puede ser string JSON o dict ---
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

        # Detectar estructura de datos del vehículo
        datos = {}
        adicional = {}

        if isinstance(info, dict) and "datos" in info and isinstance(info["datos"], dict):
            datos = info["datos"]
            adicional = info.get("adicional") or {}
            print("[DEBUG] formatear_respuesta_vehiculo: usando info['datos']")
        else:
            veh = info.get("vehiculo")
            if isinstance(veh, dict):
                if "datos" in veh and isinstance(veh["datos"], dict):
                    datos = veh["datos"]
                    adicional = veh.get("adicional") or {}
                    print("[DEBUG] formatear_respuesta_vehiculo: usando info['vehiculo']['datos']")
                else:
                    datos = veh
                    adicional = info.get("adicional") or {}
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
                    adicional = info.get("adicional") or {}
                    print("[DEBUG] formatear_respuesta_vehiculo: usando info directo (campos en raíz)")
                else:
                    print("[DEBUG] formatear_respuesta_vehiculo: no se encontró 'datos' ni 'vehiculo' adecuados")

        datos = datos or {}
        print(f"[DEBUG] formatear_respuesta_vehiculo.datos: {datos}")
        print(f"[DEBUG] formatear_respuesta_vehiculo.adicional: {adicional}")

        # Campos del vehículo
        placa = (
            datos.get("placaNumeroUnicoIdentificacion")
            or datos.get("placa")
            or "-"
        )
        clase = datos.get("claseVehiculo") or "-"
        marca = datos.get("marcaVehiculo") or "-"
        linea = datos.get("lineaVehiculo") or "-"
        modelo = datos.get("modelo") or "-"
        color = datos.get("color") or "-"
        carroceria = datos.get("carroceria") or "-"
        cilindraje = datos.get("cilindraje") or "-"
        servicio = datos.get("servicio") or "-"
        estado_registro = datos.get("estadoRegistroVehiculo") or "-"
        numero_motor = datos.get("numeroMotor") or "-"
        numero_chasis = datos.get("numeroChasis") or "-"
        vin = datos.get("vin") or "-"
        inscrito_runt = datos.get("vehiculoInscritoRUNT", "-")
        gravamenes = datos.get("poseeGravamenes", "-")

        tipo_combustible = (
            datos.get("tipoCombustible")
            or datos.get("combustible")
            or datos.get("tipoCombustibleVehiculo")
            or "-"
        )

        # SOAT / RTM
        lista_polizas = adicional.get("listaPolizas") or []
        soat_list = [
            p for p in lista_polizas
            if (p.get("tipoPoliza", "") or "").upper() == "SOAT"
        ]

        lista_rtm = adicional.get("listaRtm") or []

        def calcular_vigente(fecha_str, formato="%d/%m/%Y"):
            try:
                fecha = datetime.strptime(fecha_str, formato).date()
                hoy = datetime.now().date()
                return "SI" if fecha >= hoy else "NO"
            except Exception:
                return "-"

        soat_vigente = "SI" if any(
            calcular_vigente(p.get("fechaVencimiento", "")) == "SI"
            for p in soat_list
        ) else "NO"
        rtm_vigente = "SI" if any(
            calcular_vigente(r.get("fechaVigencia", "")) == "SI"
            for r in lista_rtm
        ) else "NO"

        # Última póliza y RTM (si existen)
        ultima_poliza = soat_list[0] if soat_list else None
        ultima_rtm = lista_rtm[0] if lista_rtm else None

        # Propietario (si viene en el JSON con persona)
        nombre_prop = "-"
        tipo_doc_prop = "-"
        nro_doc_prop = "-"

        persona_info = info.get("persona") or {}
        person = persona_info.get("person") or {}
        if person:
            nombre_prop = " ".join(
                [
                    person.get("nombre1", ""),
                    person.get("nombre2", ""),
                    person.get("apellido1", ""),
                    person.get("apellido2", ""),
                ]
            ).strip() or "-"
            tipo_doc_prop = person.get("idTipoDoc") or person.get("tipoDocumento") or "-"
            nro_doc_prop = person.get("nroDocumento") or person.get("numeroDocumento") or "-"

        # Accidentes y licencias
        lista_accidentes = adicional.get("listaAccidentes") or []
        accidentes_count = len(lista_accidentes)

        licencias_list = []
        lista_comparendos = adicional.get("listaComparendos") or []
        if lista_comparendos:
            comp = lista_comparendos[0]
            for lic in comp.get("listaLicencias") or []:
                licencias_list.append(
                    {
                        "numero": lic.get("numeroLicencia", "") or "",
                        "categoria": lic.get("categoria", "") or "",
                        "estado": lic.get("estado", "") or "",
                    }
                )

        # Blindaje
        info_veh_dto = adicional.get("informacionVehiculoDTO", {}) or {}
        blindado = info_veh_dto.get("blindado", "-")

        # --- Construir mensaje, 1 campo por línea ---
        partes = []

        partes.append(f"🚗 *Informe vehicular – {placa}*")
        partes.append("")

        # 1. Datos principales
        partes.append("*1. Datos principales del vehículo*")
        partes.append(f"• Placa: `{placa}`")
        partes.append(f"• Clase: {clase}")
        partes.append(f"• Servicio: {servicio}")
        partes.append(f"• Estado del registro: {estado_registro}")
        partes.append("")

        # 2. Características del vehículo (una sola etiqueta por línea)
        partes.append("*2. Características del vehículo*")
        partes.append(f"• Marca: {marca}")
        partes.append(f"• Línea: {linea}")
        partes.append(f"• Modelo: {modelo}")
        partes.append(f"• Color: {color}")
        partes.append(f"• Carrocería: {carroceria}")
        partes.append(f"• Cilindraje: {cilindraje}")
        partes.append(f"• Tipo de combustible: {tipo_combustible}")
        partes.append(f"• Nro. Motor: {numero_motor}")
        partes.append(f"• Nro. Chasis: {numero_chasis}")
        partes.append(f"• Nro. VIN: {vin}")
        partes.append("")

        # 3. Documentos y seguridad
        partes.append("*3. Estado de documentos y seguridad*")
        partes.append(f"• Inscrito en RUNT: {inscrito_runt}")
        partes.append(f"• Posee gravámenes: {gravamenes}")
        partes.append(f"• SOAT vigente: {soat_vigente}")
        if ultima_poliza:
            partes.append("• Detalle de la última póliza SOAT:")
            partes.append(f"  ─ Número de póliza: {ultima_poliza.get('numeroPoliza','-')}")
            partes.append(f"  ─ Entidad aseguradora: {ultima_poliza.get('aseguradora','-')}")
            partes.append(f"  ─ Fecha inicio vigencia: {ultima_poliza.get('fechaInicio','-')}")
            partes.append(f"  ─ Fecha fin vigencia: {ultima_poliza.get('fechaVencimiento','-')}")
        partes.append(f"• RTM vigente: {rtm_vigente}")
        if ultima_rtm:
            partes.append("• Detalle de la última revisión técnico-mecánica:")
            partes.append(f"  ─ Tipo de revisión: {ultima_rtm.get('tipoRevision','-')}")
            partes.append(f"  ─ CDA: {ultima_rtm.get('nombreCda','-')}")
            partes.append(f"  ─ Fecha expedición: {ultima_rtm.get('fechaExpedicion','-')}")
            partes.append(f"  ─ Fecha vigencia: {ultima_rtm.get('fechaVigencia','-')}")
        partes.append("")

        # 4. Propietario (si hay información)
        if nombre_prop != "-" or nro_doc_prop != "-":
            partes.append("*4. Propietario*")
            partes.append(f"• Nombre / Razón social: {nombre_prop}")
            partes.append(f"• Tipo de documento: {tipo_doc_prop}")
            partes.append(f"• Número de documento: {nro_doc_prop}")
            partes.append("")

        # 5. Información adicional
        partes.append("*5. Información adicional*")
        partes.append(f"• Blindado: {blindado}")
        partes.append(f"• Accidentes reportados: {accidentes_count}")
        if licencias_list:
            partes.append("• Licencia(s) de conducción asociada(s):")
            for idx, lic in enumerate(licencias_list, start=1):
                partes.append(f"  ─ Licencia #{idx}:")
                partes.append(f"    • Número de licencia: {lic['numero']}")
                partes.append(f"    • Categoría: {lic['categoria']}")
                partes.append(f"    • Estado: {lic['estado']}")

        return "\n".join(partes)

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
# 11. LÓGICA DE NEGOCIO: INICIAR CONSULTAS
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
        id_peticion = llamar_iniciar_consulta(TIPO_CONSULTA_VEHICULO_PERSONA, mensaje_payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_vehiculo -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_PERSONA,
        nombre_servicio="vehiculo_placa",
        parametros={"placa": placa_limpia},
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
TIPO_CONSULTA_VEHICULO_PERSONA,       chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_PERSONA,
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
# 12. EJECUCIÓN EN HILO Y ENVÍO DE PDF
# =====================================================================

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
    Ahora también permite que, en caso de consulta de vehículo,
    se genere y envíe un PDF con el informe vehicular.
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
                #  - (texto, firma_b64) en el caso de firma
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

                # === NUEVO: si es consulta de vehículo, generamos y enviamos PDF ===
                if tipo_consulta == TIPO_CONSULTA_VEHICULO_SOLO:
                    try:
                        # Intentamos extraer la placa para usarla en el nombre del archivo
                        placa_para_nombre = "VEHICULO"
                        try:
                            mensaje_str_local = ultimo_data.get("Mensaje") or ultimo_data.get("mensaje") or ""
                            info_local = {}
                            if isinstance(mensaje_str_local, str):
                                info_local = json.loads(mensaje_str_local)
                            elif isinstance(mensaje_str_local, dict):
                                info_local = mensaje_str_local

                            # Soportar estructuras con 'vehiculo' o con 'datos' en la raíz
                            veh_local = info_local.get("vehiculo") or {}
                            if not veh_local and "datos" in info_local:
                                datos_local = info_local.get("datos") or {}
                            else:
                                datos_local = (veh_local.get("datos") or veh_local) if veh_local else {}
                            placa_para_nombre = datos_local.get("placaNumeroUnicoIdentificacion", "VEHICULO")
                        except Exception as e:
                            print(f"[WARN] No se pudo extraer placa para nombre de PDF: {e}")

                        pdf_bytes = generar_informe_vehicular_B7_v2(ultimo_data)
                        nombre_pdf = f"Informe_vehicular_{placa_para_nombre}.pdf"
                        enviar_documento_pdf(chat_id, nombre_pdf, pdf_bytes)
                    except Exception as e:
                        print(f"[ERROR] generando/enviando PDF vehicular: {e}")

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
# 13. FLASK + WEBHOOK TELEGRAM
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
