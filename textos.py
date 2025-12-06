# textos.py

MENSAJE_BIENVENIDA = (
    "👋 *Bienvenido a Bot_Telegram_Version_1.1*\n\n"
    "Elige el tipo de consulta que deseas realizar usando los botones de abajo.\n\n"
    "También puedes usar el modo rápido escribiendo, por ejemplo:\n"
    "`CC 123456789`\n"
    "para realizar una consulta de *firma*.\n\n"
    "Escribe `/saldo` para ver tus créditos."
)

MENSAJE_SIN_CREDITOS = (
    "⚠️ No tienes créditos suficientes para realizar esta consulta.\n"
    "Si crees que es un error, contacta con el administrador."
)

MENSAJE_ERROR_GENERICO = (
    "❌ Ocurrió un error realizando la consulta.\n\n"
    "Por favor inténtalo de nuevo más tarde. "
    "Si el problema persiste, contacta con el administrador."
)

MENSAJE_SIN_DATOS = (
    "ℹ️ La consulta fue procesada pero no se encontraron datos para los parámetros enviados."
)

MENSAJE_SALDO = (
    "💰 *Tu saldo de créditos*\n\n"
    "🔢 Totales: {total}\n"
    "📥 Usados: {usados}\n"
    "📤 Disponibles: {disponibles}\n"
)

FIRMA_ELEGIDA_TEXTO = (
    "✍️ Has elegido *Consulta de firma*.\n\n"
    "Primero selecciona el *tipo de documento* usando los botones de abajo 👇"
)

FIRMA_PEDIR_NUMERO = (
    "✍️ Has elegido firma con documento tipo *{tipo_doc}*.\n\n"
    "👉 Escribe ahora el número de documento, sin puntos ni comas."
)

PERSONA_INSTRUCCIONES = (
    "🧍 Para consulta de persona, envía el documento en el formato:\n\n"
    "`CC 123456789`\n\n"
    "Puedes cambiar *CC* por *TI*, *CE*, *NIT*, etc."
)

VEHICULO_INSTRUCCIONES = (
    "🚗 Has elegido *Consulta de vehículo por placa*.\n\n"
    "👉 Escribe ahora la placa del vehículo (sin espacios), por ejemplo: `ABC123`."
)

PROPIETARIO_INSTRUCCIONES = (
    "👤 Has elegido *Propietario por placa*.\n\n"
    "👉 Escribe ahora la placa del vehículo (sin espacios)."
)

MENSAJE_NO_ENTENDI = (
    "No entendí tu mensaje 🤔.\n\n"
    "Usa el menú principal o escribe `CC 123456789` para una consulta rápida de firma."
)
