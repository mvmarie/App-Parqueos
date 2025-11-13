import os, csv, uuid, tempfile, time
from datetime import datetime, date, time as dtime, timedelta, timezone
from typing import List, Dict, Optional
from tempfile import NamedTemporaryFile

import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

#Al estar en la nube es necesario para que los CSV funcionen
MODO_DEMO = False

# ---------- Parámetros ----------
PARQUEOS_CSV = "Parqueos.csv"
EVENTOS_CSV  = "Eventos.csv"
USUARIOS_CSV = "Usuarios.csv"
LOCK_FILE    = ".parqueos.lock"     
ADMIN_CODE   = "UVG-2025"          
AUTO_CLOSE_ON_START = True          

# ---------- Config UI ----------
st.set_page_config(page_title="Parqueos UVG — App", layout="wide")
THEME_CSS = """
<style>
:root{
  --c-bg:#0e1117; --c-card:#111827; --c-primary:#0a2a66; --c-accent:#1f3b82;
  --c-text:#d1d5db; --c-muted:#9ca3af;
}
html, body, [data-testid="stAppViewContainer"] { background: var(--c-bg); color: var(--c-text); }
div[data-testid="stSidebar"]{
  background: linear-gradient(180deg, #0c1324 0%, #0e1117 60%);
  border-right:1px solid #1f2937;
}
.stButton>button{
  background:var(--c-primary)!important;
  color:#e5e7eb!important;
  border-radius:12px;
  border:1px solid #1f3b82;
}
.stButton>button:hover{ background:var(--c-accent)!important; }
div[data-baseweb="select"]>div, .stTextInput input, .stDateInput input, .stTimeInput input{
  background:#0b1220!important;
  color:var(--c-text)!important;
  border-radius:10px;
  border:1px solid #1f2937!important;
}
</style>
"""
st.markdown(THEME_CSS, unsafe_allow_html=True)

# ---------- Lockfile  ----------
def acquire_lock(path: str, timeout_sec: int = 4) -> bool:
    inicio = time.time()
    while time.time() - inicio <= timeout_sec:
        try:
            with open(path, "x"):
                return True
        except FileExistsError:
            time.sleep(0.08)
    return False

def release_lock(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass

# ---------- Cabeceras de eventos ----------
EVENT_HEADERS = [
    "event_id","timestamp","user_email","accion","motivo","lot_id","spot_id",
    "booking_id","success","free_spots_after","capacity","source","app_version",
    "error_code","slot_start","slot_end"
]

def asegurar_csv_eventos(ruta: str) -> None:
    if os.path.exists(ruta):
        try:
            df = pd.read_csv(ruta, dtype=str)
            falt = [h for h in EVENT_HEADERS if h not in df.columns]
            if falt:
                for h in falt:
                    df[h] = ""
                df.to_csv(ruta, index=False)
        except Exception:
            pass
        return
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(EVENT_HEADERS)

def asegurar_csv_usuarios(ruta: str) -> None:
    if os.path.exists(ruta):
        return
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["email","name","role","created_at"])

def cargar_parqueos(ruta: str) -> List[List]:
    if not os.path.exists(ruta):
        return []
    lotes: List[List] = []
    with open(ruta, "r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            partes = linea.split(",")
            if len(partes) != 3:
                continue
            try:
                lotes.append([partes[0], int(partes[1]), int(partes[2])])
            except ValueError:
                continue
    return lotes

def guardar_parqueos(ruta: str, lotes_estado) -> None:
    if MODO_DEMO:
        return

    tmp = NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8")
    fieldnames = [
        "lot_id",
        "nombre",
        "capacidad",
        "ocupados",
        "libres",
        "activo",
        "apertura",
        "cierre",
        "permite_espera",
    ]

    with tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()

      for lot in lotes_estado:
          lot_id = str(lot.get("lot_id", ""))

          writer.writerow({
              "lot_id": lot_id,
              "nombre": lot.get("nombre", ""),
              "capacidad": lot.get("capacidad", 0),
              "ocupados": lot.get("ocupados", 0),
              "libres": lot.get("libres", 0),
              "activo": int(lot.get("activo", True)),
              "apertura": lot.get("apertura", ""),
              "cierre": lot.get("cierre", ""),
              "permite_espera": int(lot.get("permite_espera", True)),
    })

os.replace(tmp.name, ruta)


def leer_eventos(ruta: str) -> pd.DataFrame:
    if not os.path.exists(ruta):
        return pd.DataFrame(columns=EVENT_HEADERS)

    df = pd.read_csv(ruta, dtype=str)

    # Columns to convert to numeric
    for c in ["success", "free_spots_after", "capacity"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Convert timestamps
    for c in ["timestamp", "slot_start", "slot_end"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    # Auto-add fecha y hora
    if "timestamp" in df.columns:
        df["fecha"] = df["timestamp"].dt.date
        df["hora"] = df["timestamp"].dt.hour

    # Clean text fields
    for c in ["accion", "motivo", "lot_id", "user_email", "booking_id", "error_code"]:
        if c in df.columns:
            df[c] = df[c].fillna("").str.strip()

    return df
def registrar_evento(
    ruta_eventos: str,
    user_email: str, accion: str, motivo: str,
    lot_id: str, booking_id: str,
    exito: bool, libres_despues: int, capacidad: int,
    slot_start: Optional[datetime] = None,
    slot_end:   Optional[datetime] = None,
    origen: str = "ui",
    version: str = "v2",
    codigo_error: str = ""
) -> None:
    # En modo demo (nube) no escribir CSV
    if MODO_DEMO:
        return

    asegurar_csv_eventos(ruta_eventos)
    fila = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_email": user_email,
        "accion": accion,
        "motivo": motivo,
        "lot_id": lot_id,
        "spot_id": "",
        "booking_id": booking_id,
        "success": "1" if exito else "0",
        "free_spots_after": str(libres_despues),
        "capacity": str(capacidad),
        "source": origen,
        "app_version": version,
        "error_code": codigo_error,
        "slot_start": slot_start.isoformat() if slot_start else "",
        "slot_end":   slot_end.isoformat()   if slot_end   else ""
    }

    if acquire_lock(LOCK_FILE):
        try:
            write_header = (
                not os.path.exists(ruta_eventos)
                or os.stat(ruta_eventos).st_size == 0
            )
            with open(ruta_eventos, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=EVENT_HEADERS)
                if write_header:
                    w.writeheader()
                w.writerow(fila)
        finally:
            release_lock(LOCK_FILE)

# ---------- Reglas de negocio con horarios ----------
def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return (a_start < b_end) and (b_start < a_end)

def reservas_activas(df: pd.DataFrame, ahora_utc: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    ok_res = (df["accion"] == "reserva") & (df["success"] == 1)
    ok_can = (df["accion"] == "cancelacion") & (df["success"] == 1)
    vivos = df[ok_res & (~df["booking_id"].isin(df[ok_can]["booking_id"]))].copy()
    if "slot_end" in vivos.columns:
        vivos = vivos[vivos["slot_end"] >= ahora_utc]
    return vivos

def hay_traslape(df: pd.DataFrame, lot_id: str, start: datetime, end: datetime) -> bool:
    if df.empty:
        return False
    df_lote = df[(df["lot_id"] == lot_id) & (df["accion"] == "reserva") & (df["success"] == 1)].copy()
    cancel_ok = df[(df["accion"] == "cancelacion") & (df["success"] == 1)][["booking_id"]]
    df_lote = df_lote[~df_lote["booking_id"].isin(cancel_ok["booking_id"])]
    if df_lote.empty:
        return False
    df_lote = df_lote.dropna(subset=["slot_start", "slot_end"])
    for _, r in df_lote.iterrows():
        if overlap(start, end, r["slot_start"], r["slot_end"]):
            return True
    return False

def recalcular_ocupacion_desde_eventos(lotes: List[List], df: pd.DataFrame, instante: datetime) -> List[List]:
    activos = reservas_activas(df, instante)
    mapa = {}
    for _, r in activos.iterrows():
        lid = str(r["lot_id"])
        mapa[lid] = mapa.get(lid, 0) + 1
    actualizados: List[List] = []
    for nombre, cap, _ in lotes:
        ocup = mapa.get(nombre, 0)
        actualizados.append([nombre, cap, min(ocup, cap)])
    return actualizados

def tiene_checkin(df: pd.DataFrame, booking_id: str) -> bool:
    if df.empty or not booking_id:
        return False
    sel = (df["accion"] == "checkin") & (df["success"] == 1) & (df["booking_id"] == booking_id)
    return bool(sel.any())

def expirar_vencidas(df: pd.DataFrame, ahora: datetime, ruta_eventos: str) -> None:
    if df.empty:
        return
    ok_res = (df["accion"] == "reserva") & (df["success"] == 1)
    ok_can = (df["accion"] == "cancelacion") & (df["success"] == 1)
    activos = df[ok_res & (~df["booking_id"].isin(df[ok_can]["booking_id"]))].copy()
    vencidas = activos.dropna(subset=["slot_end"])
    vencidas = vencidas[vencidas["slot_end"] < ahora]
    ya_fin = set(df[df["accion"].isin(["expiracion", "cierrejornada", "no_show"])]["booking_id"])
    for _, r in vencidas.iterrows():
        bid = r["booking_id"]
        if not bid or bid in ya_fin:
            continue
        accion = "expiracion" if tiene_checkin(df, bid) else "no_show"
        registrar_evento(
            ruta_eventos,
            r["user_email"],
            accion,
            r.get("motivo", ""),
            r["lot_id"],
            bid,
            True,
            0,
            int(r.get("capacity") or 0),
            r["slot_start"],
            r["slot_end"],
            origen="system",
            version="v2"
        )

def cerrar_jornada(df: pd.DataFrame, ahora: datetime, ruta_eventos: str) -> int:
    activos = reservas_activas(df, ahora)
    if activos.empty:
        return 0
    ya = set(df[df["accion"].isin(["expiracion", "cierrejornada", "no_show"])]["booking_id"])
    n = 0
    for _, r in activos.iterrows():
        bid = r["booking_id"]
        if not bid or bid in ya:
            continue
        accion = "expiracion" if tiene_checkin(df, bid) else "no_show"
        registrar_evento(
            ruta_eventos,
            r["user_email"],
            accion,
            "cierrejornada",
            r["lot_id"],
            bid,
            True,
            0,
            int(r.get("capacity") or 0),
            r["slot_start"],
            ahora,
            origen="admin",
            version="v2"
        )
        n += 1
    return n

# ---------- Auth ----------
def registrar_usuario(email: str, name: str, role: str) -> None:
    asegurar_csv_usuarios(USUARIOS_CSV)
    ya = False
    if os.path.exists(USUARIOS_CSV):
        with open(USUARIOS_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("email", "").lower() == email.lower():
                    ya = True
                    break
    if not ya:
        with open(USUARIOS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["email", "name", "role", "created_at"])
            if os.stat(USUARIOS_CSV).st_size == 0:
                w.writeheader()
            w.writerow({
                "email": email,
                "name": name,
                "role": role,
                "created_at": datetime.now(timezone.utc).isoformat()
            })

def auth_ui() -> Optional[Dict]:
    st.sidebar.subheader("👤 Acceso")
    email = st.sidebar.text_input("Correo institucional", placeholder="alguien@uvg.edu.gt")
    nombre = st.sidebar.text_input("Nombre (opcional)")
    admin_try = st.sidebar.text_input("Admin Code (opcional)", type="password")
    c1, c2 = st.sidebar.columns(2)
    ses = st.session_state
    if "user" not in ses:
        ses["user"] = None
    if c1.button("Ingresar / Registrar"):
        if email:
            role = "admin" if admin_try and admin_try == ADMIN_CODE else "user"
            registrar_usuario(email, nombre or "", role)
            ses["user"] = {"email": email, "name": nombre or "", "role": role}
    if c2.button("Salir"):
        ses["user"] = None
    return ses.get("user")

# ---------- Generador de reporte HTML ----------
def generar_reporte_html(
    dff: pd.DataFrame,
    reservas: pd.DataFrame,
    total_res: int,
    exito: float,
    ocup: float,
    f_ini: date,
    f_fin: date
) -> str:
    motivo_top = ""
    if "motivo" in reservas.columns and not reservas.empty:
        serie_mot = reservas["motivo"].value_counts()
        if len(serie_mot) > 0:
            motivo_top = str(serie_mot.index[0])
    lote_top = ""
    if "lot_id" in reservas.columns and not reservas.empty:
        serie_lote = reservas["lot_id"].value_counts()
        if len(serie_lote) > 0:
            lote_top = str(serie_lote.index[0])
    acciones = dff["accion"].value_counts() if "accion" in dff.columns else pd.Series(dtype=int)
    html = "<html><head><meta charset='utf-8'><title>Reporte Parqueos</title></head><body>"
    html += "<h1>Reporte de uso de parqueos</h1>"
    html += f"<p>Rango de fechas analizado: <b>{f_ini}</b> a <b>{f_fin}</b>.</p>"
    html += "<h2>Resumen general</h2>"
    html += f"<ul>"
    html += f"<li>Total de reservas: <b>{total_res}</b></li>"
    html += f"<li>Tasa de éxito: <b>{exito:.2f}%</b></li>"
    html += f"<li>Ocupación promedio (estimada): <b>{ocup:.2f}%</b></li>"
    if motivo_top:
        html += f"<li>Motivo más frecuente: <b>{motivo_top}</b></li>"
    if lote_top:
        html += f"<li>Lote más utilizado: <b>{lote_top}</b></li>"
    html += "</ul>"
    html += "<h2>Acciones registradas</h2><ul>"
    for acc, cnt in acciones.items():
        html += f"<li>{acc}: {cnt}</li>"
    html += "</ul>"
    html += "<p>Este reporte fue generado automáticamente por el prototipo de gestión de parqueos UVG.</p>"
    html += "</body></html>"
    return html

# ---------- App ----------
asegurar_csv_eventos(EVENTOS_CSV)
asegurar_csv_usuarios(USUARIOS_CSV)
lotes = cargar_parqueos(PARQUEOS_CSV)

usuario = auth_ui()
st.title("🚗 Parqueos UVG — Horarios y Reservas")
if not lotes:
    st.error("No se encontró **Parqueos.csv**.")
    st.stop()
if not usuario:
    st.info("Ingresa tu **correo** en la barra lateral para usar la app.")
    st.stop()

st.caption(f"Sesión: **{usuario['email']}** — Rol: **{usuario['role']}**")

df_all = leer_eventos(EVENTOS_CSV)
ahora = datetime.now(timezone.utc)

# 1) Expirar vencidas por slot_end < ahora 
expirar_vencidas(df_all, ahora, EVENTOS_CSV)
df_all = leer_eventos(EVENTOS_CSV)

# 2) Opcional: cerrar jornada al iniciar 
if AUTO_CLOSE_ON_START:
    cerradas = cerrar_jornada(df_all, ahora, EVENTOS_CSV)
    if cerradas:
        df_all = leer_eventos(EVENTOS_CSV)

# ---------- Tabs  ----------
tabs = ["Estado", "Reservar", "Check-in", "Cancelar", "Análisis"]
if usuario["role"] == "admin":
    tabs.append("Admin")

estado_tab, reservar_tab, checkin_tab, cancelar_tab, analisis_tab, *rest = st.tabs(tabs)
admin_tab = rest[0] if rest else None

# ----- Estado -----
with estado_tab:
    st.subheader("Disponibilidad por horario")
    colt1, colt2 = st.columns(2)
    with colt1:
        fecha_ref = st.date_input("Fecha de referencia", value=date.today())
    with colt2:
        hora_ref = st.time_input("Hora de referencia", value=dtime(hour=ahora.hour, minute=0))
    ref_dt = datetime.combine(fecha_ref, hora_ref).replace(tzinfo=timezone.utc)
    lotes_estado = recalcular_ocupacion_desde_eventos(lotes, df_all, ref_dt)
    df_estado = pd.DataFrame(
        [{"Parqueo": l[0], "Capacidad": l[1], "Ocupados": l[2], "Libres": max(l[1] - l[2], 0)} for l in lotes_estado]
    )
    st.dataframe(df_estado, use_container_width=True)
    guardar_parqueos(PARQUEOS_CSV, lotes_estado)

# ----- Reservar -----
with reservar_tab:
    st.subheader("Crear reserva por horario")
    col1, col2, col3 = st.columns(3)
    with col1:
        lote_sel = st.selectbox("Parqueo", [l[0] for l in lotes])
    with col2:
        fecha = st.date_input("Fecha", value=date.today(), key="res_fecha")
        hora  = st.time_input(
            "Hora de inicio",
            value=dtime(hour=max(ahora.hour, 7), minute=0),
            key="res_hora"
        )
    with col3:
        dur_min = st.selectbox("Duración (min)", [30, 60, 90], index=1)
    motivo = st.selectbox("Motivo", ["clase", "examen", "visita", "reunión", "actividad", "charla DELVA", "otro"])

    if st.button("Reservar"):
        start_dt = datetime.combine(fecha, hora).replace(tzinfo=timezone.utc)
        end_dt   = start_dt + timedelta(minutes=int(dur_min))
        # Chequeo de traslape
        if hay_traslape(df_all, lote_sel, start_dt, end_dt):
            capacidad_lote = 0
            for l in lotes:
                if l[0] == lote_sel:
                    capacidad_lote = int(l[1])
                    break
            registrar_evento(
                EVENTOS_CSV,
                usuario["email"],
                "lista_espera",
                motivo,
                lote_sel,
                "",
                True,
                0,
                capacidad_lote,
                start_dt,
                end_dt,
                codigo_error="TRASLAPE"
            )
            st.error("Ese horario ya está ocupado en ese lote. Se registró tu solicitud en la lista de espera.")
        else:
            lotes_slot = recalcular_ocupacion_desde_eventos(lotes, df_all, start_dt)
            lotemap = {l[0]: l for l in lotes_slot}
            lote = lotemap.get(lote_sel)
            libres = max(int(lote[1]) - int(lote[2]), 0) if lote else 0
            if libres <= 0:
                capacidad_lote = int(lote[1]) if lote else 0
                registrar_evento(
                    EVENTOS_CSV,
                    usuario["email"],
                    "lista_espera",
                    motivo,
                    lote_sel,
                    "",
                    True,
                    0,
                    capacidad_lote,
                    start_dt,
                    end_dt,
                    codigo_error="SIN_CUPO"
                )
                st.error("No hay cupo en ese horario. Se registró tu solicitud en la lista de espera.")
            else:
                booking = str(uuid.uuid4())
                registrar_evento(
                    EVENTOS_CSV,
                    usuario["email"],
                    "reserva",
                    motivo,
                    lote_sel,
                    booking,
                    True,
                    libres - 1,
                    int(lote[1]),
                    start_dt,
                    end_dt
                )
                st.success(
                    f"Reserva confirmada en **{lote_sel}** — "
                    f"{start_dt.astimezone().strftime('%d/%m %H:%M')}–"
                    f"{end_dt.astimezone().strftime('%H:%M')}.\n"
                    f"Booking: `{booking}`"
                )
                df_all = leer_eventos(EVENTOS_CSV)

# ----- Check-in -----
with checkin_tab:
    st.subheader("Check-in de reservas activas")
    activos_usuario = reservas_activas(df_all, datetime.now(timezone.utc))
    activos_usuario = activos_usuario[activos_usuario["user_email"] == usuario["email"]].copy()
    if activos_usuario.empty:
        st.info("No tienes reservas activas para hacer check-in.")
    else:
        activos_usuario = activos_usuario.sort_values("slot_start")
        opciones = []
        for _, r in activos_usuario.iterrows():
            inicio_local = r["slot_start"].astimezone()
            fin_local    = r["slot_end"].astimezone()
            etiqueta = f"{r['lot_id']} — {inicio_local.strftime('%d/%m %H:%M')}–{fin_local.strftime('%H:%M')}"
            opciones.append((etiqueta, r["booking_id"]))
        etiquetas = [e[0] for e in opciones]
        mapa_labels = {e[0]: e[1] for e in opciones}
        elegido = st.selectbox("Selecciona tu reserva", etiquetas)
        if st.button("Hacer check-in"):
            booking_sel = mapa_labels.get(elegido, "")
            if booking_sel and tiene_checkin(df_all, booking_sel):
                st.warning("Esta reserva ya tiene check-in registrado.")
            else:
                fila_sel = activos_usuario[activos_usuario["booking_id"] == booking_sel].iloc[0]
                cap = int(fila_sel.get("capacity") or 0)
                libres_reg = int(fila_sel.get("free_spots_after") or 0)
                registrar_evento(
                    EVENTOS_CSV,
                    usuario["email"],
                    "checkin",
                    "",
                    fila_sel["lot_id"],
                    booking_sel,
                    True,
                    libres_reg,
                    cap,
                    fila_sel["slot_start"],
                    fila_sel["slot_end"],
                    origen="ui",
                    version="v2"
                )
                st.success("Check-in registrado correctamente.")
                df_all = leer_eventos(EVENTOS_CSV)

# ----- Cancelar -----
def ultima_reserva_activa(df: pd.DataFrame, email: str, lot_id: str) -> Optional[str]:
    if df.empty:
        return None
    ok_res = (
        (df["accion"] == "reserva") &
        (df["success"] == 1) &
        (df["user_email"] == email) &
        (df["lot_id"] == lot_id)
    )
    res = df[ok_res].copy()
    if res.empty:
        return None
    canc = df[(df["accion"] == "cancelacion") & (df["success"] == 1)][["booking_id"]]
    res = res[~res["booking_id"].isin(canc["booking_id"])]
    if res.empty:
        return None
    res = res.sort_values("slot_start")
    return str(res.iloc[-1]["booking_id"])

with cancelar_tab:
    st.subheader("Cancelar mi reserva")
    lote_cancel = st.selectbox("Parqueo", [l[0] for l in lotes], key="cancel_lote")
    if st.button("Cancelar"):
        b = ultima_reserva_activa(df_all, usuario["email"], lote_cancel)
        lotes_now = recalcular_ocupacion_desde_eventos(lotes, df_all, ahora)
        lotemap = {l[0]: l for l in lotes_now}
        lote = lotemap.get(lote_cancel)
        libres_actuales = max(int(lote[1]) - int(lote[2]), 0) if lote else 0
        capacidad = int(lote[1]) if lote else 0
        if not b:
            registrar_evento(
                EVENTOS_CSV,
                usuario["email"],
                "cancelacion",
                "",
                lote_cancel,
                "",
                False,
                libres_actuales,
                capacidad,
                origen="ui",
                codigo_error="SIN_RESERVA_ACTIVA"
            )
            st.warning("No se encontró una reserva activa tuya en ese parqueo.")
        else:
            registrar_evento(
                EVENTOS_CSV,
                usuario["email"],
                "cancelacion",
                "",
                lote_cancel,
                b,
                True,
                libres_actuales + 1,
                capacidad
            )
            st.success("Reserva cancelada.")
            df_all = leer_eventos(EVENTOS_CSV)

# ----- Análisis -----
with analisis_tab:
    st.subheader("Análisis (filtros + 5 gráficos)")
    df_eventos = leer_eventos(EVENTOS_CSV)
    if df_eventos.empty:
        st.info("Aún no hay eventos.")
        st.stop()

    colf1, colf2, colf3 = st.columns(3)
    fechas = sorted([d for d in df_eventos["fecha"].dropna().unique()])
    with colf1:
        f_ini = st.date_input("Fecha inicial", value=min(fechas) if fechas else date.today())
    with colf2:
        f_fin = st.date_input("Fecha final", value=max(fechas) if fechas else date.today())
    with colf3:
        motivos = st.multiselect(
            "Motivos",
            options=sorted(df_eventos["motivo"].dropna().unique()),
            default=[]
        )

    dff = df_eventos.copy()
    dff = dff[(dff["fecha"] >= f_ini) & (dff["fecha"] <= f_fin)]
    if motivos:
        dff = dff[dff["motivo"].isin(motivos)]

    reservas = dff[dff["accion"] == "reserva"]

    m1, m2, m3 = st.columns(3)
    total_res = int(len(reservas))
    exito = float(round((reservas["success"] == 1).mean() * 100, 2)) if len(reservas) else 0.0
    ocup = 0.0
    if {"free_spots_after", "capacity"}.issubset(dff.columns) and len(
        dff.dropna(subset=["free_spots_after", "capacity"])
    ) > 0:
        occ = 1 - (dff["free_spots_after"] / dff["capacity"])
        ocup = float(round(occ.mean() * 100, 2))
    m1.metric("Reservas (total)", total_res)
    m2.metric("Tasa de éxito (%)", exito)
    m3.metric("Ocupación promedio (%)", ocup)

    c1, c2 = st.columns(2)
    with c1:
        serie = dff["accion"].value_counts()
        fig, ax = plt.subplots()
        if len(serie) == 0:
            ax.set_title("Frecuencia de acciones (sin datos)")
        else:
            serie.plot(kind="bar", ax=ax)
            ax.set_title("Frecuencia de acciones")
            ax.set_xlabel("Acción")
            ax.set_ylabel("Cantidad")
        st.pyplot(fig)

    with c2:
        fig, ax = plt.subplots()
        r_ok = int((reservas["success"] == 1).sum())
        r_bad = int((reservas["success"] == 0).sum())
        if r_ok + r_bad == 0:
            ax.set_title("Éxito vs. fallo (sin datos)")
        else:
            ax.pie([r_ok, r_bad], labels=["Éxito", "Fallo"], autopct="%1.1f%%")
            ax.set_title("Éxito vs. fallo en reservas")
        st.pyplot(fig)

    c3, c4 = st.columns(2)
    with c3:
        serie = reservas.groupby("fecha").size() if len(reservas) else pd.Series(dtype=int)
        fig, ax = plt.subplots()
        if len(serie) == 0:
            ax.set_title("Reservas por día (sin datos)")
        else:
            serie.plot(kind="line", ax=ax)
            ax.set_title("Reservas por día")
            ax.set_xlabel("Fecha")
            ax.set_ylabel("Nº")
        st.pyplot(fig)

    with c4:
        vals = dff["hora"].dropna()
        fig, ax = plt.subplots()
        if len(vals) == 0:
            ax.set_title("Distribución por hora (sin datos)")
        else:
            ax.hist(vals, bins=24, range=(0, 24))
            ax.set_title("Distribución por hora")
            ax.set_xlabel("Hora")
            ax.set_ylabel("Frecuencia")
        st.pyplot(fig)

    serie_lotes = reservas["lot_id"].value_counts()
    fig, ax = plt.subplots()
    if len(serie_lotes) == 0:
        ax.set_title("Reservas por lote (sin datos)")
    else:
        serie_lotes.plot(kind="bar", ax=ax)
        ax.set_title("Reservas por lote")
        ax.set_xlabel("Lote")
        ax.set_ylabel("Nº")
    st.pyplot(fig)

    # ---- Lista de espera  ----
    st.subheader("Solicitudes en lista de espera")
    df_wait = dff[dff["accion"] == "lista_espera"].copy()
    if df_wait.empty:
        st.caption("No hay registros en lista de espera en el rango seleccionado.")
    else:
        cols = ["user_email", "lot_id", "motivo", "slot_start", "slot_end", "error_code"]
        for c in cols:
            if c not in df_wait.columns:
                df_wait[c] = ""
        df_wait = df_wait[cols].copy()
        if "slot_start" in df_wait.columns:
            df_wait["slot_start"] = df_wait["slot_start"].dt.tz_convert(None)
        if "slot_end" in df_wait.columns:
            df_wait["slot_end"] = df_wait["slot_end"].dt.tz_convert(None)
        st.dataframe(df_wait, use_container_width=True)

    # ---- Botón de descarga de reporte ----
    reporte_html = generar_reporte_html(dff, reservas, total_res, exito, ocup, f_ini, f_fin)
    st.download_button(
        "Descargar reporte (HTML)",
        data=reporte_html.encode("utf-8"),
        file_name="reporte_parqueos.html",
        mime="text/html"
    )

# ----- Admin  -----
# ----- Admin (solo rol admin) -----
if admin_tab is not None:
    with admin_tab:
        st.subheader("Panel de Administración")

        colA, colB = st.columns(2)
        with colA:
            fecha_ref = st.date_input("Fecha ref.", value=date.today(), key="adm_f")
        with colB:
            hora_ref = st.time_input("Hora ref.", value=dtime(hour=ahora.hour, minute=0), key="adm_h")
        ref_dt = datetime.combine(fecha_ref, hora_ref).replace(tzinfo=timezone.utc)

        df_now = reservas_activas(df_all, ref_dt).copy()
        if not df_now.empty:
            df_now = df_now.sort_values(["lot_id", "slot_start"])[
                ["user_email", "lot_id", "booking_id", "slot_start", "slot_end", "motivo"]
            ]
            df_now["slot_start"] = df_now["slot_start"].dt.tz_convert(None)
            df_now["slot_end"]   = df_now["slot_end"].dt.tz_convert(None)

        st.markdown("**Reservas activas en el instante seleccionado**")
        st.dataframe(
            df_now if not df_now.empty else pd.DataFrame(
                columns=["user_email", "lot_id", "booking_id", "slot_start", "slot_end", "motivo"]
            ),
            use_container_width=True
        )

        lotes_ref = recalcular_ocupacion_desde_eventos(lotes, df_all, ref_dt)
        df_occ = pd.DataFrame(
            [{"Lote": l[0], "Capacidad": l[1], "Ocupados": l[2], "Libres": max(l[1] - l[2], 0)} for l in lotes_ref]
        )
        st.markdown("**Ocupación por lote (instante ref.)**")
        st.dataframe(df_occ, use_container_width=True)

        st.divider()
        st.markdown("⚠️ **Acciones de fin de día**")
        confirmar = st.checkbox("Estoy seguro de que quiero cerrar la jornada", key="chk_cierre")

        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("Cerrar jornada (expirar activas)"):
                if confirmar:
                    n = cerrar_jornada(df_all, datetime.now(timezone.utc), EVENTOS_CSV)
                    df_all = leer_eventos(EVENTOS_CSV)
                    st.success(f"Se cerró la jornada. Reservas expiradas/no-show marcadas: {n}.")
                else:
                    st.warning("Marca la casilla de confirmación antes de cerrar la jornada.")
        with col_btn2:
            if st.button("Refrescar datos"):
                df_all = leer_eventos(EVENTOS_CSV)
                st.info("Datos recargados.")













