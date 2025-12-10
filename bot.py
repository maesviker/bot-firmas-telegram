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

        # 2. Características del vehículo
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
    Consulta de vehículo por placa utilizando tipo 1 (vehículo + persona).
    En IniciarConsulta la API espera:
      "mensaje": "PDK400"
    (solo la placa, no JSON).
    """
    # Usamos el tipo 1 en la configuración
    config = get_consulta_config(TIPO_CONSULTA_VEHICULO_PERSONA)
    if not _verificar_creditos_o_mensaje(chat_id, usuario, config):
        return

    placa_limpia = placa.replace(" ", "").upper()
    mensaje_payload = placa_limpia

    try:
        # Aquí ya usamos tipo 1
        id_peticion = llamar_iniciar_consulta(TIPO_CONSULTA_VEHICULO_PERSONA, mensaje_payload)
    except Exception as e:
        print(f"[ERROR] iniciar_consulta_vehiculo -> IniciarConsulta: {e}")
        enviar_mensaje(chat_id, textos.MENSAJE_ERROR_GENERICO)
        return

    msg_id = registrar_mensaje_pendiente(
        usuario=usuario,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_PERSONA,   # guardamos tipo 1
        nombre_servicio="vehiculo_placa",
        parametros={"placa": placa_limpia},
        valor_consulta=config.valor_consulta,
    )

    ejecutar_consulta_en_hilo(
        chat_id=chat_id,
        usuario=usuario,
        mensaje_id=msg_id,
        tipo_consulta=TIPO_CONSULTA_VEHICULO_PERSONA,   # el hilo sabe que es tipo 1
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

                # === Si es consulta de vehículo, generamos y enviamos PDF ===
                # Aceptamos tipo 1 (vehículo + persona) y 3 por compatibilidad
                if tipo_consulta in (TIPO_CONSULTA_VEHICULO_PERSONA, TIPO_CONSULTA_VEHICULO_SOLO):
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
