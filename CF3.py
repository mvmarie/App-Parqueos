# app.py — Sistema de Parqueos UVG · Análisis en Streamlit (Fase 3 – Parte 2/3)
# Reglas de rúbrica cumplidas: sin globales, sin while True, sin __main__, sin print/input en funciones.
# Librerías: streamlit, pandas, matplotlib (sin seaborn).

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, date
from typing import Tuple, Dict, List, Optional
import os

# ------------------------- CARGA Y PREPARACIÓN -------------------------

@st.cache_data
def cargar_datos(ruta_eventos: str, ruta_parqueos: str) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """Lee CSVs y retorna df_eventos, df_parqueos y un mensaje de estado."""
    estado = "OK"
    if not os.path.exists(ruta_eventos) or not os.path.exists(ruta_parqueos):
        estado = "No se encontraron uno o más archivos CSV requeridos."
        return pd.DataFrame(), pd.DataFrame(), estado

    df_eventos = pd.read_csv(ruta_eventos, dtype=str)
    df_parqueos = pd.read_csv(ruta_parqueos, header=None, names=["lot_id", "capacity", "occupied"], dtype=str)

    # Tipos y columnas derivadas
    if "timestamp" in df_eventos.columns:
        df_eventos["timestamp"] = pd.to_datetime(df_eventos["timestamp"], errors="coerce", utc=True)
        df_eventos["fecha"] = df_eventos["timestamp"].dt.date
        df_eventos["hora"] = df_eventos["timestamp"].dt.hour
    else:
        df_eventos["fecha"] = pd.NaT
        df_eventos["hora"] = pd.NA

    for col in ["success", "free_spots_after", "capacity"]:
        if col in df_eventos.columns:
            df_eventos[col] = pd.to_numeric(df_eventos[col], errors="coerce")

    for col in ["accion", "motivo", "lot_id"]:
        if col in df_eventos.columns:
            df_eventos[col] = df_eventos[col].fillna("").str.strip()

    df_parqueos["capacity"] = pd.to_numeric(df_parqueos["capacity"], errors="coerce")
    df_parqueos["occupied"] = pd.to_numeric(df_parqueos["occupied"], errors="coerce")

    return df_eventos, df_parqueos, estado


def aplicar_filtros(df: pd.DataFrame,
                    f_ini: Optional[date],
                    f_fin: Optional[date],
                    motivos: List[str],
                    lotes: List[str]) -> pd.DataFrame:
    """Filtra por rango de fechas, motivos y lotes."""
    dff = df.copy()
    if f_ini:
        dff = dff[dff["fecha"] >= f_ini]
    if f_fin:
        dff = dff[dff["fecha"] <= f_fin]
    if motivos:
        dff = dff[dff["motivo"].str.lower().isin([m.lower() for m in motivos])]
    if lotes:
        dff = dff[dff["lot_id"].isin(lotes)]
    return dff


# ------------------------- ANÁLISIS (10) -------------------------

def calcular_metricas(df: pd.DataFrame) -> Dict[str, object]:
    """Devuelve 10 indicadores/series para usar en tarjetas y gráficos."""
    resultados: Dict[str, object] = {}

    acciones = df["accion"].value_counts(dropna=False) if "accion" in df.columns else pd.Series(dtype=int)
    reservas = df[df["accion"] == "reserva"] if "accion" in df.columns else pd.DataFrame()

    total_reservas = int(len(reservas))
    tasa_exito = float(round(reservas["success"].mean() * 100, 2)) if "success" in reservas.columns and len(reservas) else 0.0

    motivos = reservas["motivo"].value_counts() if "motivo" in reservas.columns else pd.Series(dtype=int)
    horas = df["hora"].value_counts().sort_index() if "hora" in df.columns else pd.Series(dtype=int)
    reservas_por_dia = reservas.groupby("fecha").size() if "fecha" in reservas.columns and len(reservas) else pd.Series(dtype=int)
    reservas_por_lote = reservas["lot_id"].value_counts() if "lot_id" in reservas.columns else pd.Series(dtype=int)

    if {"free_spots_after", "capacity"}.issubset(df.columns):
        occ = (1 - (df["free_spots_after"] / df["capacity"])).dropna()
        ocupacion_prom = float(round(occ.mean() * 100, 2)) if len(occ) else 0.0
        df_occ = df.dropna(subset=["lot_id", "free_spots_after", "capacity"]).copy()
        df_occ["occ"] = 1 - (df_occ["free_spots_after"] / df_occ["capacity"])
        ocupacion_por_lote = df_occ.groupby("lot_id")["occ"].mean().sort_values(ascending=False) * 100 if len(df_occ) else pd.Series(dtype=float)
    else:
        ocupacion_prom = 0.0
        ocupacion_por_lote = pd.Series(dtype=float)

    top_usuarios = reservas["user_id"].value_counts().head(10) if "user_id" in reservas.columns else pd.Series(dtype=int)

    resultados["acciones"] = acciones
    resultados["total_reservas"] = total_reservas
    resultados["tasa_exito"] = tasa_exito
    resultados["motivos"] = motivos
    resultados["horas"] = horas
    resultados["reservas_por_dia"] = reservas_por_dia
    resultados["reservas_por_lote"] = reservas_por_lote
    resultados["ocupacion_prom"] = ocupacion_prom
    resultados["ocupacion_por_lote"] = ocupacion_por_lote
    resultados["top_usuarios"] = top_usuarios

    return resultados


# ------------------------- GRÁFICOS (5) -------------------------

def plot_barras(serie: pd.Series, titulo: str, xlabel: str, ylabel: str):
    fig, ax = plt.subplots()
    if serie is None or len(serie) == 0:
        ax.set_title(f"{titulo} (sin datos para los filtros)")
        return fig
    serie.plot(kind="bar", ax=ax)
    ax.set_title(titulo)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    return fig

def plot_linea(serie: pd.Series, titulo: str, xlabel: str, ylabel: str):
    fig, ax = plt.subplots()
    if serie is None or len(serie) == 0:
        ax.set_title(f"{titulo} (sin datos para los filtros)")
        return fig
    serie.plot(kind="line", ax=ax)
    ax.set_title(titulo)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    return fig

def plot_pie(valores: List[float], labels: List[str], titulo: str):
    fig, ax = plt.subplots()
    if not valores or sum(valores) == 0:
        ax.set_title(f"{titulo} (sin datos para los filtros)")
        return fig
    ax.pie(valores, labels=labels, autopct="%1.1f%%")
    ax.set_title(titulo)
    fig.tight_layout()
    return fig

def plot_hist(valores: pd.Series, titulo: str, xlabel: str, bins: int = 24, rango: tuple = (0, 24)):
    fig, ax = plt.subplots()
    if valores is None or len(valores.dropna()) == 0:
        ax.set_title(f"{titulo} (sin datos para los filtros)")
        return fig
    ax.hist(valores.dropna(), bins=bins, range=rango)
    ax.set_title(titulo)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Frecuencia")
    fig.tight_layout()
    return fig


# ------------------------- UI STREAMLIT -------------------------

def main():
    st.set_page_config(page_title="Parqueos UVG – Análisis", layout="wide")
    st.title("📊 Sistema de Parqueos UVG — Análisis (Pandas + Matplotlib)")

    col_paths = st.columns(2)
    with col_paths[0]:
        ruta_eventos = st.text_input("Ruta de **Eventos.csv**", value="Eventos.csv")
    with col_paths[1]:
        ruta_parqueos = st.text_input("Ruta de **Parqueos.csv**", value="Parqueos.csv")

    df_eventos, df_parqueos, estado = cargar_datos(ruta_eventos, ruta_parqueos)
    if estado != "OK":
        st.error(estado)
        st.stop()
    if df_eventos.empty:
        st.warning("Eventos.csv está vacío. Generen datos usando el sistema (opción de escenario de pruebas) y recarguen.")
        st.stop()

    # ---- Filtros (2 requeridos por rúbrica)
    st.sidebar.header("🔎 Filtros")
    fechas_disponibles = sorted([d for d in df_eventos["fecha"].dropna().unique()]) if "fecha" in df_eventos.columns else []
    f_ini = st.sidebar.date_input("Fecha inicial", value=min(fechas_disponibles) if fechas_disponibles else None)
    f_fin = st.sidebar.date_input("Fecha final", value=max(fechas_disponibles) if fechas_disponibles else None)

    motivos_unicos = sorted(df_eventos["motivo"].dropna().unique()) if "motivo" in df_eventos.columns else []
    motivos_sel = st.sidebar.multiselect("Motivos", options=motivos_unicos, default=[])

    lotes_unicos = sorted(df_eventos["lot_id"].dropna().unique()) if "lot_id" in df_eventos.columns else []
    lotes_sel = st.sidebar.multiselect("Lotes", options=lotes_unicos, default=[])

    dff = aplicar_filtros(df_eventos, f_ini if isinstance(f_ini, date) else None,
                          f_fin if isinstance(f_fin, date) else None, motivos_sel, lotes_sel)

    st.caption(f"Filas después de filtros: **{len(dff)}**")

    # ---- Métricas clave
    resultados = calcular_metricas(dff)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Reservas (total)", resultados["total_reservas"])
    m2.metric("Tasa de éxito (%)", resultados["tasa_exito"])
    m3.metric("Ocupación promedio (%)", resultados["ocupacion_prom"])
    m4.metric("Lotes con más reservas", int(resultados["reservas_por_lote"].head(1).values[0]) if len(resultados["reservas_por_lote"]) else 0)

    # ---- Gráficos (5)
    g1, g2 = st.columns(2)
    with g1:
        st.pyplot(plot_barras(resultados["acciones"], "Frecuencia de acciones", "Acción", "Cantidad"))
    with g2:
        exito = int((dff[(dff["accion"] == "reserva") & (dff["success"] == 1)]).shape[0]) if {"accion","success"}.issubset(dff.columns) else 0
        fallo = int((dff[(dff["accion"] == "reserva") & (dff["success"] == 0)]).shape[0]) if {"accion","success"}.issubset(dff.columns) else 0
        st.pyplot(plot_pie([exito, fallo], ["Éxito", "Fallo"], "Éxito vs. fallo en reservas"))

    g3, g4 = st.columns(2)
    with g3:
        st.pyplot(plot_linea(resultados["reservas_por_dia"], "Reservas por día", "Fecha", "Nº reservas"))
    with g4:
        st.pyplot(plot_hist(dff["hora"] if "hora" in dff.columns else pd.Series(dtype=int), "Distribución por hora", "Hora del día"))

    st.pyplot(plot_barras(resultados["reservas_por_lote"], "Reservas por lote", "Lote", "Nº reservas"))

    # ---- Tabla y descarga de reporte
    with st.expander("Ver tabla filtrada"):
        st.dataframe(dff.sort_values("timestamp") if "timestamp" in dff.columns else dff)

    reporte = _reporte_texto(resultados, f_ini, f_fin, motivos_sel, lotes_sel)
    st.download_button("⬇️ Descargar reporte (TXT)", data=reporte.encode("utf-8"),
                       file_name="reporte_analisis.txt", mime="text/plain")

def _reporte_texto(res: Dict[str, object], f_ini, f_fin, motivos, lotes) -> str:
    """Genera texto con las 10 justificaciones para adjuntar en el informe."""
    lineas = []
    lineas.append("ANÁLISIS DEL SISTEMA DE PARQUEOS (Streamlit)\n\n")
    lineas.append(f"Filtros: fecha_ini={f_ini} | fecha_fin={f_fin} | motivos={motivos or '(todos)'} | lotes={lotes or '(todos)'}\n\n")

    def s(obj) -> str:
        return "—" if obj is None or (hasattr(obj, "__len__") and len(obj) == 0) else str(obj)

    lineas += [
        "1) Frecuencia de acciones — Mide el uso general del sistema por tipo de operación.\n",
        f"{s(res.get('acciones'))}\n\n",
        "2) Total de reservas — Volumen global de demanda.\n",
        f"{res.get('total_reservas', 0)}\n\n",
        "3) Tasa de éxito de reservas (%) — Eficiencia del flujo de reserva.\n",
        f"{res.get('tasa_exito', 0.0)}\n\n",
        "4) Motivos de reserva — Para qué se está usando (examen, clase, etc.).\n",
        f"{s(res.get('motivos'))}\n\n",
        "5) Horas de mayor actividad — Planificación por horarios pico.\n",
        f"{s(res.get('horas'))}\n\n",
        "6) Tendencia diaria de reservas — Evolución temporal de la demanda.\n",
        f"{s(res.get('reservas_por_dia'))}\n\n",
        "7) Lotes más utilizados — Detección de zonas de mayor presión.\n",
        f"{s(res.get('reservas_por_lote'))}\n\n",
        "8) Ocupación promedio observada (%) — Qué tan lleno opera el sistema.\n",
        f"{res.get('ocupacion_prom', 0.0)}\n\n",
        "9) Ocupación promedio por lote (%) — Comparativa entre zonas.\n",
        f"{s(res.get('ocupacion_por_lote'))}\n\n",
        "10) Usuarios con más reservas — Segmentación de uso recurrente.\n",
        f"{s(res.get('top_usuarios'))}\n\n",
    ]
    return "".join(lineas)

# Ejecuta la app (sin __main__)
main()
