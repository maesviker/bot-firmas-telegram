"""
Módulo de textos del bot.

Aquí centralizamos todos los mensajes largos para que:
- Sea más fácil editarlos.
- El archivo bot.py no se llene de cadenas de texto.
"""

MENSAJE_BIENVENIDA = (
    "👋 *Bienvenido a Bot_Telegram_V1.1*\n\n"
    "Estoy listo para tus consultas ✅\n\n"
    "👉 Elige una opción con los botones de abajo.\n"
    "👉 O usa el modo rápido, por ejemplo:\n"
    "`CC 123456789` (consulta de *firma*)\n\n"
    "Escribe `/saldo` para ver tus créditos.\n"
    "Si tienes dudas, usa el menú."
)

MENSAJE_SIN_CREDITOS = (
    "⚠️ No tienes créditos suficientes para realizar esta consulta.\n\n"
    "Si crees que esto es un error, contacta con el administrador."
)

MENSAJE_ERROR_GENERICO = (
    "❌ Ocurrió un error realizando la consulta.\n\n"
    "Por favor inténtalo de nuevo más tarde. "
    "Si el problema persiste, contacta con el administrador."
)

MENSAJE_SIN_DATOS = (
    "ℹ️ La consulta se realizó correctamente pero no se encontraron "
    "datos para los parámetros enviados."
)

MENSAJE_SALDO = (
    "💰 *Tu saldo de créditos*\n\n"
    "Créditos totales: `{total}`\n"
    "Créditos usados: `{usados}`\n"
    "Créditos disponibles: `{disponibles}`\n"
)
