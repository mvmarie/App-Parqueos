# Análisis de uso del Sistema de Parqueos UVG (Fase 3 – Parte 2)
# Reglas de rúbrica: sin globales, sin print()/input() dentro de funciones, sin while True, sin __main__.
# Librerías: pandas, matplotlib (sin seaborn).

from typing import Tuple, List, Dict, Optional
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os

# ---------------------- CARGA Y PREPARACIÓN ----------------------

def cargar_datos(ruta_eventos: str, ruta_parqueos: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Lee CSVs y retorna df_eventos y df_parqueos con tipos preparados."""
    df_eventos = pd.read_csv(ruta_eventos, dtype=str)
    df_parqueos = pd.read_csv(
        ruta_parqueos, header=None, names=["lot_id", "capacity", "occupied"], dtype=str
    )

    # Parseo de tipos
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

    # Normalización sencilla
    for col in ["accion", "motivo", "lot_id"]:
        if col in df_eventos.columns:
            df_eventos[col] = df_eventos[col].fillna("").str.strip()

    # Parqueos con tipos correctos
    df_parqueos["capacity"] = pd.to_numeric(df_parqueos["capacity"], errors="coerce")
    df_parqueos["occupied"] = pd.to_numeric(df_parqueos["occupied"], errors="coerce")

    return df_eventos, df_parqueos


def aplicar_filtros(df: pd.DataFrame,
                    fecha_ini: Optional[str],
                    fecha_fin: Optional[str],
                    motivos: List[str]) -> pd.DataFrame:
    """Aplica filtros por fecha (inclusive) y lista de motivos; retorna DataFrame filtrado."""
    df_filtrado = df.copy()

    # Filtro de fechas si se proporcionan
    if fecha_ini:
        try:
            f_ini = datetime.fromisoformat(fecha_ini).date()
            df_filtrado = df_filtrado[df_filtrado["fecha"] >= f_ini]
        except Exception:
            pass

    if fecha_fin:
        try:
            f_fin = datetime.fromisoformat(fecha_fin).date()
            df_filtrado = df_filtrado[df_filtrado["fecha"] <= f_fin]
        except Exception:
            pass

    # Filtro de motivos (si se dan)
    motivos_norm = [m.strip().lower() for m in motivos if m.strip()]
    if len(motivos_norm) > 0 and "motivo" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["motivo"].str.lower().isin(motivos_norm)]

    return df_filtrado


# ---------------------- ANÁLISIS (10) ----------------------

def analisis_basicos(df: pd.DataFrame) -> Dict[str, object]:
    """
    Devuelve un diccionario con resultados clave.
    Cada análisis está justificado en el reporte que se escribe a disco.
    """
    resultados: Dict[str, object] = {}

    # 1) Conteo de eventos por acción (uso del sistema)
    acciones = df["accion"].value_counts(dropna=False)

    # 2) Total de reservas
    total_reservas = (df["accion"] == "reserva").sum()

    # 3) Tasa de éxito de reservas
    reservas = df[df["accion"] == "reserva"]
    tasa_exito = reservas["success"].mean() * 100 if len(reservas) > 0 else 0.0

    # 4) Distribución por motivos (para entender “para qué” se usa)
    motivos = df[df["accion"] == "reserva"]["motivo"].value_counts()

    # 5) Horas pico (actividad por hora)
    horas = df["hora"].value_counts().sort_index()

    # 6) Tendencia diaria de reservas
    reservas_por_dia = reservas.groupby("fecha").size() if "fecha" in reservas.columns else pd.Series(dtype=int)

    # 7) Lotes más utilizados (reservas por lot_id)
    reservas_por_lote = reservas["lot_id"].value_counts()

    # 8) Promedio de ocupación observado (1 - libres/capacidad)
    #    Nota: usamos columnas al momento del evento (free_spots_after/capacity)
    if "free_spots_after" in df.columns and "capacity" in df.columns:
        ocupacion = (1 - (df["free_spots_after"] / df["capacity"])).dropna()
        ocupacion_prom = ocupacion.mean() * 100 if len(ocupacion) > 0 else 0.0
    else:
        ocupacion_prom = 0.0

    # 9) Ocupación promedio por lote (promedio de ocupación observado por lot_id)
    if "lot_id" in df.columns and "free_spots_after" in df.columns and "capacity" in df.columns:
        df_occ = df.dropna(subset=["lot_id", "free_spots_after", "capacity"]).copy()
        df_occ["occ"] = 1 - (df_occ["free_spots_after"] / df_occ["capacity"])
        ocupacion_por_lote = df_occ.groupby("lot_id")["occ"].mean().sort_values(ascending=False) * 100
    else:
        ocupacion_por_lote = pd.Series(dtype=float)

    # 10) Usuarios más activos (número de reservas por user_id)
    top_usuarios = reservas["user_id"].value_counts().head(10)

    resultados["acciones"] = acciones
    resultados["total_reservas"] = int(total_reservas)
    resultados["tasa_exito"] = float(round(tasa_exito, 2))
    resultados["motivos_reserva"] = motivos
    resultados["horas_actividad"] = horas
    resultados["reservas_por_dia"] = reservas_por_dia
    resultados["reservas_por_lote"] = reservas_por_lote
    resultados["ocupacion_promedio"] = float(round(ocupacion_prom, 2))
    resultados["ocupacion_por_lote"] = ocupacion_por_lote
    resultados["top_usuarios_reservas"] = top_usuarios

    return resultados


# ---------------------- GRÁFICOS (5) ----------------------

def asegurar_directorio(ruta_dir: str) -> None:
    """Crea el directorio si no existe."""
    if not os.path.exists(ruta_dir):
        os.makedirs(ruta_dir, exist_ok=True)

def grafico_barras_acciones(df: pd.DataFrame, ruta_salida: str) -> None:
    asegurar_directorio(os.path.dirname(ruta_salida))
    plt.figure()
    df["accion"].value_counts().plot(kind="bar")
    plt.title("Frecuencia de acciones")
    plt.xlabel("Acción")
    plt.ylabel("Cantidad")
    plt.tight_layout()
    plt.savefig(ruta_salida)
    plt.close()

def grafico_linea_reservas_diarias(df: pd.DataFrame, ruta_salida: str) -> None:
    asegurar_directorio(os.path.dirname(ruta_salida))
    reservas = df[df["accion"] == "reserva"]
    serie = reservas.groupby("fecha").size()
    plt.figure()
    serie.plot(kind="line")
    plt.title("Reservas por día")
    plt.xlabel("Fecha")
    plt.ylabel("Nº reservas")
    plt.tight_layout()
    plt.savefig(ruta_salida)
    plt.close()

def grafico_pie_exito_reservas(df: pd.DataFrame, ruta_salida: str) -> None:
    asegurar_directorio(os.path.dirname(ruta_salida))
    reservas = df[df["accion"] == "reserva"]
    if len(reservas) == 0:
        # No hay datos; generamos una figura vacía para no fallar
        plt.figure()
        plt.title("Éxito de reservas (sin datos)")
        plt.savefig(ruta_salida)
        plt.close()
        return
    exito = (reservas["success"] == 1).sum()
    fallo = (reservas["success"] == 0).sum()
    plt.figure()
    plt.pie([exito, fallo], labels=["Éxito", "Fallo"], autopct="%1.1f%%")
    plt.title("Éxito vs. fallo en reservas")
    plt.tight_layout()
    plt.savefig(ruta_salida)
    plt.close()

def grafico_histograma_horas(df: pd.DataFrame, ruta_salida: str) -> None:
    asegurar_directorio(os.path.dirname(ruta_salida))
    horas = df["hora"].dropna()
    plt.figure()
    plt.hist(horas, bins=24, range=(0, 24))
    plt.title("Distribución por hora")
    plt.xlabel("Hora del día")
    plt.ylabel("Frecuencia")
    plt.tight_layout()
    plt.savefig(ruta_salida)
    plt.close()

def grafico_barras_reservas_por_lote(df: pd.DataFrame, ruta_salida: str) -> None:
    asegurar_directorio(os.path.dirname(ruta_salida))
    reservas = df[df["accion"] == "reserva"]
    serie = reservas["lot_id"].value_counts()
    plt.figure()
    serie.plot(kind="bar")
    plt.title("Reservas por lote")
    plt.xlabel("Lote")
    plt.ylabel("Nº reservas")
    plt.tight_layout()
    plt.savefig(ruta_salida)
    plt.close()


# ---------------------- REPORTE TEXTO ----------------------

def escribir_reporte(resultados: Dict[str, object], ruta_reporte: str,
                     filtros_aplicados: Dict[str, object]) -> None:
    """
    Escribe un .txt con los 10 análisis y la justificación de cada uno,
    citando cómo responde a preguntas del negocio (demanda, uso, eficiencia).
    """
    lineas: List[str] = []

    lineas.append("ANÁLISIS DEL SISTEMA DE PARQUEOS (Fase 3 – Parte 2)\n")
    lineas.append("Filtros aplicados:\n")
    for k, v in filtros_aplicados.items():
        lineas.append(f"  - {k}: {v}\n")
    lineas.append("\n")

    # 1 Acciones
    lineas.append("1) Frecuencia de acciones (consulta, reserva, cancelación, reinicio)\n")
    lineas.append("   Justificación: mide el uso real del sistema y dónde se concentra la interacción.\n")
    lineas.append(f"{resultados['acciones']}\n\n")

    # 2 Total de reservas
    lineas.append("2) Total de reservas\n")
    lineas.append("   Justificación: volumen global de demanda registrada en el período analizado.\n")
    lineas.append(f"   total_reservas = {resultados['total_reservas']}\n\n")

    # 3 Tasa de éxito
    lineas.append("3) Tasa de éxito de reservas (%)\n")
    lineas.append("   Justificación: eficiencia del flujo de reserva; detecta fricciones o falta de cupo.\n")
    lineas.append(f"   tasa_exito = {resultados['tasa_exito']}%\n\n")

    # 4 Motivos
    lineas.append("4) Motivos de reserva (examen, visita, reunión, clase, actividad, charla DELVA, otro)\n")
    lineas.append("   Justificación: identifica para qué se usa el parqueo y cómo varía la demanda.\n")
    lineas.append(f"{resultados['motivos_reserva']}\n\n")

    # 5 Horas
    lineas.append("5) Horas de mayor actividad\n")
    lineas.append("   Justificación: permite planificar señalización y disponibilidad según horarios pico.\n")
    lineas.append(f"{resultados['horas_actividad']}\n\n")

    # 6 Trend diario
    lineas.append("6) Tendencia diaria de reservas\n")
    lineas.append("   Justificación: ver evolución temporal de la demanda (picos por fechas específicas).\n")
    lineas.append(f"{resultados['reservas_por_dia']}\n\n")

    # 7 Lotes
    lineas.append("7) Lotes más utilizados (reservas por lot_id)\n")
    lineas.append("   Justificación: detectar zonas “imán” de demanda para tomar decisiones operativas.\n")
    lineas.append(f"{resultados['reservas_por_lote']}\n\n")

    # 8 Ocupación promedio global
    lineas.append("8) Ocupación promedio observada (%)\n")
    lineas.append("   Justificación: mide cuán lleno opera el sistema en promedio.\n")
    lineas.append(f"   ocupacion_promedio = {resultados['ocupacion_promedio']}%\n\n")

    # 9 Ocupación por lote
    lineas.append("9) Ocupación promedio por lote (%)\n")
    lineas.append("   Justificación: comparación entre zonas para reasignar cupos y priorizar mejoras.\n")
    lineas.append(f"{resultados['ocupacion_por_lote']}\n\n")

    # 10 Usuarios más activos
    lineas.append("10) Usuarios con más reservas\n")
    lineas.append("   Justificación: segmenta el uso por usuarios para estudiar reglas (ej. 1 reserva activa).\n")
    lineas.append(f"{resultados['top_usuarios_reservas']}\n\n")

    with open(ruta_reporte, "w", encoding="utf-8") as f:
        f.writelines(lineas)


# ---------------------- FLUJO SUPERIOR (AQUÍ SÍ HAY input/print) ----------------------

# 1) Lectura de filtros “interactivos” (por consola; no dentro de funciones)
print("\n=== FILTROS (ENTER para omitir) ===")
f_ini = input("Fecha inicial (YYYY-MM-DD) : ").strip()
f_fin = input("Fecha final   (YYYY-MM-DD) : ").strip()
motivos_raw = input("Motivos separados por coma (p.ej. clase,examen) : ").strip()

motivos_list = [m.strip() for m in motivos_raw.split(",")] if motivos_raw else []

# 2) Carga y preparación
RUTA_EVENTOS = "Eventos.csv"
RUTA_PARQUEOS = "Parqueos.csv"

df_eventos, df_parqueos = cargar_datos(RUTA_EVENTOS, RUTA_PARQUEOS)

# 3) Aplicar filtros
df_filtrado = aplicar_filtros(df_eventos, f_ini if f_ini else None, f_fin if f_fin else None, motivos_list)

# 4) Análisis (10)
resultados = analisis_basicos(df_filtrado)

# 5) Gráficos (5) → carpeta /graficos
grafico_barras_acciones(df_filtrado, "graficos/acciones.png")
grafico_linea_reservas_diarias(df_filtrado, "graficos/reservas_diarias.png")
grafico_pie_exito_reservas(df_filtrado, "graficos/exito_reservas.png")
grafico_histograma_horas(df_filtrado, "graficos/horas.png")
grafico_barras_reservas_por_lote(df_filtrado, "graficos/reservas_por_lote.png")

# 6) Reporte con justificación de cada análisis
filtros_info = {
    "fecha_inicial": f_ini if f_ini else "(sin filtro)",
    "fecha_final": f_fin if f_fin else "(sin filtro)",
    "motivos": motivos_list if motivos_list else "(sin filtro)"
}
escribir_reporte(resultados, "graficos/reporte_analisis.txt", filtros_info)

print("\nListo ✅")
print("Se generaron gráficos en la carpeta: graficos/")
print("Y el reporte con justificaciones: graficos/reporte_analisis.txt\n")
