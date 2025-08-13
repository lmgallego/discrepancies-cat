# ================================================
# APP STREAMLIT: eRVC (MASTER) + DeclaracionVerificador (EXTRANET)
# Limpieza, transformaciones, comprobación de integridad y selector múltiple nipd
# ================================================
import sys, subprocess, pkgutil, os
import pandas as pd
import numpy as np
from io import BytesIO
import unicodedata, re
import streamlit as st

# Instalación silenciosa si falta
def _install_if_missing(pkgs):
    import importlib.util
    for p in pkgs:
        if importlib.util.find_spec(p) is None:
            with open(os.devnull, "w") as devnull:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", p],
                                      stdout=devnull, stderr=devnull)
_install_if_missing(["xlsxwriter", "openpyxl", "plotly", "streamlit-shadcn-ui"])

# Importar componentes modernos de UI
try:
    import streamlit_shadcn_ui as ui
except ImportError:
    ui = None

# ------------------------------
# Utilidades
# ------------------------------
def normalize_text(s):
    if pd.isna(s) or s == "":
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s

def build_normalized_map(columns):
    return {normalize_text(c): c for c in columns}

def safe_get_col(map_norm, candidates):
    for cand in candidates:
        norm_cand = normalize_text(cand)
        if norm_cand in map_norm:
            return map_norm[norm_cand]
    return None

def detect_header_row(xl_bytes, sheet_index=0, max_probe_rows=20, min_hits=3):
    try:
        for row_idx in range(max_probe_rows):
            df_test = pd.read_excel(BytesIO(xl_bytes), sheet_name=sheet_index, header=row_idx, nrows=1)
            hits = sum(1 for col in df_test.columns if any(kw in str(col).lower() for kw in ["nipd", "tiquet", "verificador", "pesnet", "nif"]))
            if hits >= min_hits:
                return row_idx
    except Exception:
        pass
    return None

def limpiar_eRVC(eRVC_df):
    """Limpia y procesa el DataFrame de eRVC según especificaciones"""
    df = eRVC_df.copy()
    
    # Filtrar columna 'dos' para mantener solo 'CV'
    if 'dos' in df.columns:
        df = df[df['dos'] == 'CV']
    
    # Formatear dataPesada a DD/MM/AAAA (sin hora)
    if 'dataPesada' in df.columns:
        df['dataPesada'] = pd.to_datetime(df['dataPesada'], errors='coerce').dt.strftime('%d/%m/%Y')
    
    # Formatear dataGravacio a DD/MM/AAAA (sin hora)
    if 'dataGravacio' in df.columns:
        df['dataGravacio'] = pd.to_datetime(df['dataGravacio'], errors='coerce').dt.strftime('%d/%m/%Y')
    
    # Convertir columnas problemáticas a string para evitar errores de tipo
    string_columns = ['tiquetBascula', 'nipd', 'nifLliurador']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Eliminar columnas especificadas
    columns_to_drop = [
        'qualificacioPesada', 
        'motiuPesadaIncidental', 
        'modificada', 
        'destiRaim', 
        'varietatDesc'
    ]
    
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df.drop(columns=existing_columns_to_drop, inplace=True)
    
    return df

def limpiar_extranet(extranet_bytes, eRVC_df):
    extranet_xl = pd.ExcelFile(BytesIO(extranet_bytes))
    extranet_sheet = extranet_xl.sheet_names[0]

    hdr = detect_header_row(extranet_bytes, sheet_index=0)
    try_headers = []
    if hdr is not None: try_headers.append(hdr)
    try_headers += [6, 5]  # fallbacks comunes

    df = None
    tried = set()
    for h in try_headers:
        if h in tried: continue
        tried.add(h)
        try:
            tmp = pd.read_excel(BytesIO(extranet_bytes), sheet_name=extranet_sheet, header=h)
            if tmp.shape[1] > 1:
                df = tmp
                break
        except Exception:
            pass

    if df is None:
        raise RuntimeError("No fue posible determinar la fila de encabezados en EXTRANET.")

    # Eliminar columnas duplicadas manteniendo la primera ocurrencia
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Eliminar columnas "Unnamed"
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]]

    colmap = build_normalized_map(df.columns)

    # Día y hora -> dataPesada / Hora
    dia_hora_col = safe_get_col(colmap, ["Día y hora:", "Día y hora", "Dia y hora:", "Dia y hora"])
    if dia_hora_col:
        dt = pd.to_datetime(df[dia_hora_col], errors="coerce", dayfirst=True)
        if dt.isna().mean() > 0.5:
            dt = pd.to_datetime(df[dia_hora_col], errors="coerce", dayfirst=False)
        df["dataPesada"] = dt.dt.strftime("%d/%m/%Y")
        df["Hora"] = dt.dt.strftime("%H:%M:%S")
        df.drop(columns=[dia_hora_col], inplace=True)

    # Renombrados
    rename_candidates = {
        "Num. tiquet de báscula": "tiquetBascula",
        "NIPBD": "nipd",
        "Nombre y Apellidos Viticultor": "nomLliurador",
        "Nif Viticultor": "nifLliurador",
        "Razón Social": "nomCeller",

    }
    actual_renames = {}
    colmap = build_normalized_map(df.columns)
    for src, dst in rename_candidates.items():
        found = safe_get_col(colmap, [src])
        if found: actual_renames[found] = dst
    if actual_renames:
        df.rename(columns=actual_renames, inplace=True)

    # Tratar nipd como texto sin separadores de miles
    if 'nipd' in df.columns:
        # Convertir a string preservando el formato original
        df['nipd'] = df['nipd'].apply(lambda x: str(x).replace('.', '').replace(',', '').strip() if pd.notna(x) else '')
        
        # Eliminar el último cero si se añadió incorrectamente durante la conversión
        # Esto corrige el problema donde '2501200003' se convierte en '2501200030'
        df['nipd'] = df['nipd'].apply(lambda x: x[:-1] if x.endswith('0') and len(x) > 9 else x)

    # Eliminar columnas no deseadas
    drop_targets = [
        "modificado por", "fecha modificacion", "observaciones",
        "descarga en caja", "ecologico", "gluconico",
        "grado alcoholico verificado", "grado", "estado"
    ]
    to_drop = [col for col in df.columns if normalize_text(col) in drop_targets]
    if to_drop:
        df.drop(columns=to_drop, inplace=True)

    # Filtrar Zona ≠ {Almendralejo, Requena, Cariñena}
    zona_col = safe_get_col(build_normalized_map(df.columns), ["Zona"])
    if zona_col:
        df = df[~df[zona_col].astype(str).str.strip().str.lower().isin(
            ["almendralejo", "requena", "cariñena", "carinena"]
        )]

    # Eliminar Variedad
    var_col = safe_get_col(build_normalized_map(df.columns), ["Variedad"])
    if var_col and var_col in df.columns:
        df.drop(columns=[var_col], inplace=True)

    # Verificador en mayúsculas
    verificador_col = safe_get_col(build_normalized_map(df.columns), ["Verificador"])
    if verificador_col:
        df[verificador_col] = df[verificador_col].astype(str).str.upper()

    # Orden de columnas como MASTER
    master_cols = list(eRVC_df.columns)
    common_in_master = [c for c in master_cols if c in df.columns]
    extras = [c for c in df.columns if c not in master_cols]
    ordered = common_in_master + extras
    df = df[ordered]

    return df

def generar_reporte_errores(df):
    errores = []
    pattern = r'^[A-Z]\d{8}$|^\d{8}[A-Z]$'

    if "nifLliurador" in df.columns and "Verificador" in df.columns:
        mask_nif = ~df["nifLliurador"].astype(str).str.strip().str.upper().str.fullmatch(pattern, na=False)
        if mask_nif.any():
            err_nif = (
                df[mask_nif]
                .groupby("Verificador").size()
                .reset_index(name="Cantidad")
            )
            err_nif["Tipo error"] = "Error Introducción NIF"
            errores.append(err_nif)

    if "tiquetBascula" in df.columns and "Verificador" in df.columns:
        mask_tiquet = df["tiquetBascula"].isna() | (df["tiquetBascula"].astype(str).str.strip() == "")
        if mask_tiquet.any():
            err_tiq = (
                df[mask_tiquet]
                .groupby("Verificador").size()
                .reset_index(name="Cantidad")
            )
            err_tiq["Tipo error"] = "Error Introducción tiquet"
            errores.append(err_tiq)

    if errores:
        return pd.concat(errores, ignore_index=True)
    else:
        return pd.DataFrame(columns=["Verificador", "Cantidad", "Tipo error"])

# ------------------------------
# Streamlit UI
# ------------------------------
st.set_page_config(
    page_title="📊 Análisis Discrepancias CAT", 
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# CSS personalizado para mejorar la apariencia
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
        margin: 0.5rem 0;
    }
    .success-card {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .success-card h3 {
        margin: 0 0 0.5rem 0;
        font-size: 1.2rem;
    }
    .success-card p {
        margin: 0;
        opacity: 0.9;
    }
    
    /* Mejorar apariencia de elementos Streamlit */
    .stSelectbox > div > div {
        background-color: #f8f9fa;
        border-radius: 8px;
    }
    .stMultiSelect > div > div {
        border-radius: 8px;
    }
    .stFileUploader > div {
        border-radius: 8px;
        border: 2px dashed #667eea;
    }
    
    /* Mejorar tablas */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)

# Encabezado principal con diseño mejorado
st.markdown("""
<div class="main-header">
    <h1>📊 Análisis de Discrepancias CAT</h1>
    <p>Sistema de comparación entre eRVC y Extranet</p>
</div>
""", unsafe_allow_html=True)

# Función para crear métricas modernas
def create_metric_card(title, value, description="", icon="📊"):
    if ui is not None:
        try:
            return ui.metric_card(
                title=title,
                content=str(value),
                description=description,
                key=f"metric_{title.replace(' ', '_').lower()}"
            )
        except:
            pass
    
    # Fallback a métricas estándar con estilo mejorado
    st.markdown(f"""
    <div class="metric-card">
        <h4 style="margin: 0; color: #667eea;">{icon} {title}</h4>
        <h2 style="margin: 0.5rem 0; color: #333;">{value}</h2>
        <p style="margin: 0; color: #666; font-size: 0.9rem;">{description}</p>
    </div>
    """, unsafe_allow_html=True)

# Sidebar para carga de archivos
with st.sidebar:
    st.header("📁 Carga de Archivos")
    
    eRVC_file = st.file_uploader(
        "📋 Archivo eRVC (MASTER)", 
        type=["xlsx", "xls"],
        help="Archivo principal de referencia"
    )
    
    extranet_file = st.file_uploader(
        "🌐 Archivo Extranet", 
        type=["xlsx", "xls"],
        help="Archivo de declaraciones del verificador"
    )

if eRVC_file and extranet_file:
    try:
        # Cargar y limpiar eRVC
        eRVC_original = pd.read_excel(eRVC_file)
        eRVC_df = limpiar_eRVC(eRVC_original)
        st.success(f"✅ eRVC cargado y procesado: {len(eRVC_df)} registros (de {len(eRVC_original)} originales)")
        
        # Limpiar Extranet
        extranet_df = limpiar_extranet(extranet_file.read(), eRVC_original)
        st.success(f"✅ Extranet procesado: {len(extranet_df)} registros")
        
        # Métricas principales con diseño moderno
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_metric_card(
                "Registros eRVC", 
                f"{len(eRVC_df):,}",
                "Archivo maestro",
                "📋"
            )
        
        with col2:
            create_metric_card(
                "Registros Extranet", 
                f"{len(extranet_df):,}",
                "Después de limpieza",
                "🌐"
            )
        
        with col3:
            common_cols = set(eRVC_df.columns) & set(extranet_df.columns)
            create_metric_card(
                "Columnas Comunes", 
                len(common_cols),
                "Para comparación",
                "🔗"
            )
        
        with col4:
            if 'nipd' in extranet_df.columns:
                unique_nipd = extranet_df['nipd'].nunique()
                create_metric_card(
                    "NIPD Únicos", 
                    f"{unique_nipd:,}",
                    "En Extranet",
                    "🏷️"
                )
        
        # Reporte de errores con diseño mejorado
        st.header("🚨 Reporte de Errores")
        errores_df = generar_reporte_errores(extranet_df)
        
        if not errores_df.empty:
            # Mostrar tabla de errores con iconos
            st.dataframe(
                errores_df,
                use_container_width=True,
                column_config={
                    "Verificador": st.column_config.TextColumn(
                        "👤 Verificador",
                        help="Identificador del verificador"
                    ),
                    "Cantidad": st.column_config.NumberColumn(
                        "📊 Cantidad",
                        help="Número de errores detectados"
                    ),
                    "Tipo error": st.column_config.TextColumn(
                        "⚠️ Tipo de Error",
                        help="Categoría del error encontrado"
                    )
                }
            )
            
            # Gráfico de errores con colores modernos
            import plotly.express as px
            fig = px.bar(
                errores_df, 
                x="Verificador", 
                y="Cantidad", 
                color="Tipo error",
                title="📊 Distribución de Errores por Verificador",
                color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4"]
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Arial, sans-serif")
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Mensaje de éxito mejorado
            st.markdown("""
            <div class="success-card">
                <h3>✅ Excelente!</h3>
                <p>No se detectaron errores de introducción en los datos</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Análisis de discrepancias con diseño mejorado
        st.header("🔍 Análisis de Discrepancias")
        
        # Tip visual mejorado
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; margin: 10px 0; border-left: 4px solid #007bff;">
            <p style="margin: 0; color: #495057; font-size: 12px;">💡 <strong>Tip:</strong> Selecciona bodegas específicas para un análisis más detallado de las discrepancias</p>
        </div>
        """, unsafe_allow_html=True)
        
        if 'nipd' in extranet_df.columns and 'nomCeller' in extranet_df.columns:
            # Crear opciones combinando nipd y nomCeller
            extranet_unique = extranet_df[['nipd', 'nomCeller']].drop_duplicates().dropna()
            opciones_bodegas = [f"{row['nipd']} - {row['nomCeller']}" for _, row in extranet_unique.iterrows()]
            opciones_bodegas = sorted(opciones_bodegas)
            
            # Opción para seleccionar todas las bodegas
            col1, col2 = st.columns([3, 1])
            
            with col1:
                bodegas_seleccionadas = st.multiselect(
                    "🏭 Seleccionar Bodegas",
                    opciones_bodegas,
                    default=opciones_bodegas,  # Todas las bodegas seleccionadas por defecto
                    help="Elige las bodegas que quieres analizar (nipd - nombre)"
                )
            
            with col2:
                if st.button("✅ Seleccionar Todas", key="select_all_bodegas"):
                    st.session_state.bodegas_seleccionadas = opciones_bodegas
                    st.rerun()
                if st.button("❌ Deseleccionar Todas", key="deselect_all_bodegas"):
                    st.session_state.bodegas_seleccionadas = []
                    st.rerun()
            
            # Usar session state si existe
            if 'bodegas_seleccionadas' in st.session_state:
                bodegas_seleccionadas = st.session_state.bodegas_seleccionadas
            
            if bodegas_seleccionadas:
                # Extraer los nipd de las opciones seleccionadas
                nipds_seleccionados = [opcion.split(' - ')[0] for opcion in bodegas_seleccionadas]
                extranet_filtrado = extranet_df[extranet_df['nipd'].isin(nipds_seleccionados)]
                
                # Métricas de selección con diseño moderno
                col1, col2 = st.columns(2)
                with col1:
                    create_metric_card(
                        "Bodegas Seleccionadas", 
                        len(bodegas_seleccionadas),
                        f"De {len(opciones_bodegas)} disponibles",
                        "🏭"
                    )
                with col2:
                    create_metric_card(
                        "Registros Filtrados", 
                        f"{len(extranet_filtrado):,}",
                        f"De {len(extranet_df):,} totales",
                        "📊"
                    )
        
        # Vista de datos
        st.header("📋 Vista de Datos")
        
        # Pestaña para datos originales
        with st.expander("📋 Ver eRVC Original", expanded=False):
            st.dataframe(eRVC_original.head(100), use_container_width=True)
        
        # Mostrar datos procesados lado a lado
        st.subheader("📊 Datos Procesados")
        col1, col2 = st.columns(2)
        
        # Aplicar el mismo filtro de bodegas a eRVC si existe
        eRVC_display = eRVC_df
        if 'bodegas_seleccionadas' in locals() and bodegas_seleccionadas and 'nipd' in eRVC_df.columns:
            # Usar los mismos nipd seleccionados
            nipds_seleccionados = [opcion.split(' - ')[0] for opcion in bodegas_seleccionadas]
            eRVC_display = eRVC_df[eRVC_df['nipd'].isin(nipds_seleccionados)]
        
        with col1:
            st.markdown("**🧹 eRVC Procesado**")
            if 'bodegas_seleccionadas' in locals() and bodegas_seleccionadas:
                st.caption(f"Filtrado por {len(bodegas_seleccionadas)} bodegas seleccionadas")
            st.dataframe(eRVC_display.head(50), use_container_width=True, height=400)
            
        with col2:
            st.markdown("**🌐 Extranet Procesado**")
            display_df = extranet_filtrado if 'extranet_filtrado' in locals() else extranet_df
            if 'bodegas_seleccionadas' in locals() and bodegas_seleccionadas:
                st.caption(f"Filtrado por {len(bodegas_seleccionadas)} bodegas seleccionadas")
            st.dataframe(display_df.head(50), use_container_width=True, height=400)
        
        # Exportación con botón mejorado
        st.header("💾 Exportar Resultados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📥 Descargar Extranet Procesado", type="primary"):
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    extranet_df.to_excel(writer, sheet_name='Extranet_Procesado', index=False)
                    if not errores_df.empty:
                        errores_df.to_excel(writer, sheet_name='Reporte_Errores', index=False)
                
                st.download_button(
                    label="📁 Descargar archivo Excel",
                    data=output.getvalue(),
                    file_name="extranet_procesado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with col2:
            if st.button("🧹 Descargar eRVC Procesado", type="primary"):
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    # Usar los datos filtrados si existe el filtro
                    data_to_export = eRVC_display if 'eRVC_display' in locals() else eRVC_df
                    data_to_export.to_excel(writer, sheet_name='eRVC_Procesado', index=False)
                
                # Nombre de archivo dinámico según filtro
                filename = "eRVC_procesado_filtrado.xlsx" if 'bodegas_seleccionadas' in locals() and bodegas_seleccionadas else "eRVC_procesado.xlsx"
                
                st.download_button(
                    label="📁 Descargar archivo Excel",
                    data=output.getvalue(),
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        # Nueva sección: Análisis Detallada Discrepancias
        st.header("🔍 Análisis Detallada Discrepancias")
        
        # Verificar que tenemos las columnas necesarias
        if ('dataPesada' in eRVC_df.columns and 'kgTotals' in eRVC_df.columns and 
            'nipd' in eRVC_df.columns and 'nomCeller' in eRVC_df.columns and
            'dataPesada' in extranet_df.columns and 'Kg:' in extranet_df.columns and
            'nipd' in extranet_df.columns):
            
            # Convertir fechas a datetime si no lo están ya
            try:
                eRVC_df['dataPesada'] = pd.to_datetime(eRVC_df['dataPesada'], format='%d/%m/%Y', errors='coerce')
                extranet_df['dataPesada'] = pd.to_datetime(extranet_df['dataPesada'], format='%d/%m/%Y', errors='coerce')
            except:
                pass
            
            # Obtener fechas únicas disponibles
            fechas_eRVC = eRVC_df['dataPesada'].dropna().dt.date.unique()
            fechas_extranet = extranet_df['dataPesada'].dropna().dt.date.unique()
            fechas_disponibles = sorted(set(fechas_eRVC) | set(fechas_extranet))
            
            if fechas_disponibles:
                # Inicializar session state para fechas
                if 'fechas_seleccionadas' not in st.session_state:
                    st.session_state.fechas_seleccionadas = fechas_disponibles[:5] if len(fechas_disponibles) > 5 else fechas_disponibles
                
                # Opción para seleccionar todas las fechas
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    # Selector de fechas
                    fechas_seleccionadas = st.multiselect(
                        "📅 Seleccionar Fechas para Análisis",
                        fechas_disponibles,
                        default=st.session_state.fechas_seleccionadas,
                        help="Elige las fechas que quieres analizar",
                        key="selector_fechas"
                    )
                    # Actualizar session state
                    st.session_state.fechas_seleccionadas = fechas_seleccionadas
                
                with col2:
                    st.markdown("<br>", unsafe_allow_html=True)  # Espaciado
                    if st.button("🗓️ Seleccionar Todas", help="Selecciona todas las fechas disponibles"):
                        st.session_state.fechas_seleccionadas = fechas_disponibles
                        st.rerun()
                    
                    if st.button("🗑️ Limpiar Selección", help="Deselecciona todas las fechas"):
                        st.session_state.fechas_seleccionadas = []
                        st.rerun()
                
                if fechas_seleccionadas:
                    # Filtrar datos por fechas seleccionadas
                    eRVC_filtrado_fecha = eRVC_df[eRVC_df['dataPesada'].dt.date.isin(fechas_seleccionadas)]
                    extranet_filtrado_fecha = extranet_df[extranet_df['dataPesada'].dt.date.isin(fechas_seleccionadas)]
                    
                    # Agrupar y sumar kg por nipd para eRVC
                    eRVC_agrupado = eRVC_filtrado_fecha.groupby('nipd').agg({
                        'nomCeller': 'first',
                        'kgTotals': 'sum'
                    }).reset_index()
                    
                    # Agrupar y sumar kg por nipd para Extranet
                    extranet_agrupado = extranet_filtrado_fecha.groupby('nipd').agg({
                        'Kg:': 'sum'
                    }).reset_index()
                    
                    # Combinar ambos DataFrames
                    analisis_general = pd.merge(eRVC_agrupado, extranet_agrupado, on='nipd', how='outer')
                    
                    # Rellenar valores NaN con 0
                    analisis_general['kgTotals'] = analisis_general['kgTotals'].fillna(0)
                    analisis_general['Kg:'] = analisis_general['Kg:'].fillna(0)
                    
                    # Calcular porcentaje de discrepancia (negativo si extranet > eRVC)
                    analisis_general['% Discrepancia'] = np.where(
                        analisis_general['kgTotals'] != 0,
                        (analisis_general['kgTotals'] - analisis_general['Kg:']) / analisis_general['kgTotals'] * 100,
                        np.where(analisis_general['Kg:'] != 0, -100, 0)
                    )
                    
                    # Renombrar columnas para mejor visualización
                    analisis_general = analisis_general.rename(columns={
                        'kgTotals': 'Kg eRVC',
                        'Kg:': 'Kg Extranet'
                    })
                    
                    # Reordenar columnas
                    analisis_general = analisis_general[['nipd', 'nomCeller', 'Kg eRVC', 'Kg Extranet', '% Discrepancia']]
                    
                    # Crear análisis para bodegas controladas (solo nipd coincidentes)
                    nipds_extranet = set(extranet_agrupado['nipd'].unique())
                    analisis_controladas = analisis_general[analisis_general['nipd'].isin(nipds_extranet)].copy()
                    
                    # Mostrar tabla con pestañas
                    tab1, tab2, tab3 = st.tabs(["📊 Análisis General", "🎯 Bodegas Controladas", "📋 Detalle por Pesada"])
                     
                    with tab1:
                        st.subheader("📊 Análisis General")
                        st.caption(f"Análisis para {len(fechas_seleccionadas)} fechas seleccionadas - Todas las bodegas")
                        
                        # Función para colorear filas con discrepancia > 15%
                        def highlight_discrepancia(row):
                            if row['% Discrepancia'] > 15:
                                return ['background-color: #ffebee'] * len(row)
                            return [''] * len(row)
                        
                        # Aplicar formato y mostrar tabla
                        styled_df = analisis_general.style.apply(highlight_discrepancia, axis=1)
                        styled_df = styled_df.format({
                            'Kg eRVC': '{:.2f}',
                            'Kg Extranet': '{:.2f}',
                            '% Discrepancia': '{:.2f}%'
                        })
                        
                        st.dataframe(styled_df, use_container_width=True, height=400)
                        
                        # Métricas resumen para Análisis General
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            total_discrepancias = len(analisis_general[abs(analisis_general['% Discrepancia']) > 15])
                            create_metric_card(
                                "Discrepancias >15%",
                                total_discrepancias,
                                f"De {len(analisis_general)} registros",
                                "⚠️"
                            )
                        with col2:
                            promedio_discrepancia = analisis_general['% Discrepancia'].mean()
                            create_metric_card(
                                "Promedio Discrepancia",
                                f"{promedio_discrepancia:.2f}%",
                                "Todas las bodegas",
                                "📊"
                            )
                        with col3:
                            max_discrepancia = analisis_general['% Discrepancia'].max()
                            create_metric_card(
                                "Máxima Discrepancia",
                                f"{max_discrepancia:.2f}%",
                                "Peor caso",
                                "🔴"
                            )
                    
                    with tab2:
                        st.subheader("🎯 Bodegas Controladas")
                        st.caption(f"Análisis para {len(fechas_seleccionadas)} fechas seleccionadas - Solo bodegas con datos en Extranet")
                        
                        if len(analisis_controladas) > 0:
                            # Aplicar formato y mostrar tabla
                            styled_df_controladas = analisis_controladas.style.apply(highlight_discrepancia, axis=1)
                            styled_df_controladas = styled_df_controladas.format({
                                'Kg eRVC': '{:.2f}',
                                'Kg Extranet': '{:.2f}',
                                '% Discrepancia': '{:.2f}%'
                            })
                            
                            st.dataframe(styled_df_controladas, use_container_width=True, height=400)
                            
                            # Métricas resumen para Bodegas Controladas
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                total_discrepancias_ctrl = len(analisis_controladas[abs(analisis_controladas['% Discrepancia']) > 15])
                                create_metric_card(
                                    "Discrepancias >15%",
                                    total_discrepancias_ctrl,
                                    f"De {len(analisis_controladas)} registros",
                                    "⚠️"
                                )
                            with col2:
                                promedio_discrepancia_ctrl = analisis_controladas['% Discrepancia'].mean()
                                create_metric_card(
                                    "Promedio Discrepancia",
                                    f"{promedio_discrepancia_ctrl:.2f}%",
                                    "Bodegas controladas",
                                    "📊"
                                )
                            with col3:
                                max_discrepancia_ctrl = analisis_controladas['% Discrepancia'].max()
                                create_metric_card(
                                    "Máxima Discrepancia",
                                    f"{max_discrepancia_ctrl:.2f}%",
                                    "Peor caso controlado",
                                    "🔴"
                                )
                        else:
                            st.warning("⚠️ No se encontraron bodegas controladas para las fechas seleccionadas")
                    
                    with tab3:
                        st.subheader("📋 Detalle por Pesada - Bodegas Controladas")
                        st.caption(f"Comparación detallada por tickets para {len(fechas_seleccionadas)} fechas seleccionadas")
                        
                        if len(analisis_controladas) > 0:
                            # Filtrar datos originales por nipd de bodegas controladas y fechas seleccionadas
                            nipds_controladas = set(analisis_controladas['nipd'].unique())
                            
                            # Filtrar eRVC por bodegas controladas y fechas
                            eRVC_detalle = eRVC_filtrado_fecha[eRVC_filtrado_fecha['nipd'].isin(nipds_controladas)].copy()
                            
                            # Filtrar extranet por bodegas controladas y fechas
                            extranet_detalle = extranet_filtrado_fecha[extranet_filtrado_fecha['nipd'].isin(nipds_controladas)].copy()
                            
                            if not eRVC_detalle.empty and not extranet_detalle.empty:
                                try:
                                    # Verificar columnas necesarias
                                    required_extranet_cols = ['dataPesada', 'nipd', 'nomCeller', 'tiquetBascula', 'Kg:', 'Hora']
                                    required_ervc_cols = ['dataPesada', 'nipd', 'nomCeller', 'tiquetBascula', 'kgTotals']
                                    
                                    missing_extranet = [col for col in required_extranet_cols if col not in extranet_detalle.columns]
                                    missing_ervc = [col for col in required_ervc_cols if col not in eRVC_detalle.columns]
                                    
                                    if missing_extranet:
                                        st.error(f"❌ Columnas faltantes en Extranet: {missing_extranet}")
                                        st.stop()
                                    if missing_ervc:
                                        st.error(f"❌ Columnas faltantes en eRVC: {missing_ervc}")
                                        st.stop()
                                    
                                    # Preparar datos de extranet
                                    extranet_prep = extranet_detalle[required_extranet_cols].copy()
                                    extranet_prep['Fecha'] = extranet_prep['dataPesada'].dt.strftime('%d/%m/%Y')
                                    
                                    # Preparar datos de eRVC
                                    eRVC_prep = eRVC_detalle[required_ervc_cols].copy()
                                    eRVC_prep['Fecha'] = eRVC_prep['dataPesada'].dt.strftime('%d/%m/%Y')
                                    
                                    # PASO 1: Agrupar por ticket y sumar kilos
                                    # Extranet: agrupar por ticket, sumar Kg: y obtener hora min/max
                                    extranet_por_ticket = extranet_prep.groupby(['Fecha', 'nipd', 'nomCeller', 'tiquetBascula']).agg({
                                        'Kg:': 'sum',
                                        'Hora': ['min', 'max']
                                    }).reset_index()
                                    
                                    # Aplanar columnas multi-nivel
                                    extranet_por_ticket.columns = ['Fecha', 'nipd', 'nomCeller', 'tiquetBascula', 'kg_extranet', 'hora_min', 'hora_max']
                                    
                                    # eRVC: agrupar por ticket y sumar kgTotals
                                    ervc_por_ticket = eRVC_prep.groupby(['Fecha', 'nipd', 'nomCeller', 'tiquetBascula']).agg({
                                        'kgTotals': 'sum'
                                    }).reset_index()
                                    ervc_por_ticket = ervc_por_ticket.rename(columns={'kgTotals': 'kg_ervc'})
                                    
                                    # PASO 2: Agrupar por fecha y nipd
                                    def procesar_grupo_fecha_nipd(fecha, nipd):
                                        # Filtrar datos para este grupo
                                        extranet_grupo = extranet_por_ticket[
                                            (extranet_por_ticket['Fecha'] == fecha) & 
                                            (extranet_por_ticket['nipd'] == nipd)
                                        ].copy()
                                        
                                        ervc_grupo = ervc_por_ticket[
                                            (ervc_por_ticket['Fecha'] == fecha) & 
                                            (ervc_por_ticket['nipd'] == nipd)
                                        ].copy()
                                        
                                        if extranet_grupo.empty and ervc_grupo.empty:
                                            return pd.DataFrame()
                                        
                                        # Ordenar tickets por número ascendente
                                        if not extranet_grupo.empty:
                                            extranet_grupo['ticket_num'] = pd.to_numeric(extranet_grupo['tiquetBascula'], errors='coerce')
                                            extranet_grupo = extranet_grupo.sort_values('ticket_num').drop('ticket_num', axis=1)
                                        
                                        if not ervc_grupo.empty:
                                            ervc_grupo['ticket_num'] = pd.to_numeric(ervc_grupo['tiquetBascula'], errors='coerce')
                                            ervc_grupo = ervc_grupo.sort_values('ticket_num').drop('ticket_num', axis=1)
                                        
                                        # Emparejar fila a fila
                                        max_rows = max(len(extranet_grupo), len(ervc_grupo))
                                        resultado_grupo = []
                                        
                                        for i in range(max_rows):
                                            # Solo mostrar fecha y nipd en la primera fila del grupo
                                            fecha_display = fecha if i == 0 else ''
                                            nipd_display = nipd if i == 0 else ''
                                            nomCeller_display = (extranet_grupo.iloc[0]['nomCeller'] if not extranet_grupo.empty else 
                                                               (ervc_grupo.iloc[0]['nomCeller'] if not ervc_grupo.empty else '')) if i == 0 else ''
                                            
                                            fila = {
                                                'Fecha': fecha_display,
                                                'nipd': nipd_display,
                                                'nomCeller': nomCeller_display,
                                                'tiquetBascula_extranet': extranet_grupo.iloc[i]['tiquetBascula'] if i < len(extranet_grupo) else '',
                                                'kg_extranet': extranet_grupo.iloc[i]['kg_extranet'] if i < len(extranet_grupo) else 0,
                                                'tiquetBascula_eRVC': ervc_grupo.iloc[i]['tiquetBascula'] if i < len(ervc_grupo) else '',
                                                'kg_eRVC': ervc_grupo.iloc[i]['kg_ervc'] if i < len(ervc_grupo) else 0,
                                                'Discrepancia': '',  # Vacío para filas individuales
                                                'Hora_primera_pesada': '',  # Vacío para filas individuales
                                                'Hora_ultima_pesada': ''  # Vacío para filas individuales
                                            }
                                            resultado_grupo.append(fila)
                                        
                                        # Calcular totales del grupo
                                        total_extranet = extranet_grupo['kg_extranet'].sum() if not extranet_grupo.empty else 0
                                        total_ervc = ervc_grupo['kg_ervc'].sum() if not ervc_grupo.empty else 0
                                        
                                        # Calcular discrepancia del grupo (negativo si extranet > eRVC)
                                        if total_ervc > 0:
                                            discrepancia_grupo = (total_ervc - total_extranet) / total_ervc * 100
                                        else:
                                            discrepancia_grupo = -100 if total_extranet > 0 else 0
                                        
                                        # Obtener hora más temprana y más tardía del grupo
                                        if not extranet_grupo.empty:
                                            hora_primera_grupo = extranet_grupo['hora_min'].min()
                                            hora_ultima_grupo = extranet_grupo['hora_max'].max()
                                        else:
                                            hora_primera_grupo = ''
                                            hora_ultima_grupo = ''
                                        
                                        # Añadir fila de TOTALES
                                        fila_totales = {
                                            'Fecha': '',
                                            'nipd': '',
                                            'nomCeller': 'TOTALES',
                                            'tiquetBascula_extranet': '',
                                            'kg_extranet': total_extranet,
                                            'tiquetBascula_eRVC': '',
                                            'kg_eRVC': total_ervc,
                                            'Discrepancia': f"{round(discrepancia_grupo, 0)}%",
                                            'Hora_primera_pesada': hora_primera_grupo,
                                            'Hora_ultima_pesada': hora_ultima_grupo
                                        }
                                        resultado_grupo.append(fila_totales)
                                        
                                        return pd.DataFrame(resultado_grupo)
                                    
                                    # Procesar todos los grupos
                                    todos_grupos = []
                                    grupos_fecha_nipd = set()
                                    
                                    # Obtener todos los grupos únicos de fecha/nipd
                                    for df in [extranet_por_ticket, ervc_por_ticket]:
                                        if not df.empty:
                                            for _, row in df[['Fecha', 'nipd']].drop_duplicates().iterrows():
                                                grupos_fecha_nipd.add((row['Fecha'], row['nipd']))
                                    
                                    # Procesar cada grupo
                                    for fecha, nipd in sorted(grupos_fecha_nipd):
                                        grupo_resultado = procesar_grupo_fecha_nipd(fecha, nipd)
                                        if not grupo_resultado.empty:
                                            todos_grupos.append(grupo_resultado)
                                    
                                    if todos_grupos:
                                        # Combinar todos los grupos
                                        resultado_final = pd.concat(todos_grupos, ignore_index=True)
                                        
                                        # Función para resaltar discrepancias > 15% y filas TOTALES
                                        def highlight_row(row):
                                            if row['nomCeller'] == 'TOTALES':
                                                # Extraer el número de la discrepancia
                                                disc_str = str(row['Discrepancia']).replace('%', '')
                                                try:
                                                    disc_val = float(disc_str) if disc_str else 0
                                                    if abs(disc_val) > 15:
                                                        return ['background-color: #ffcdd2'] * len(row)
                                                    else:
                                                        return ['background-color: #e8f5e8'] * len(row)
                                                except:
                                                    return ['background-color: #e8f5e8'] * len(row)
                                            return [''] * len(row)
                                        
                                        # Mostrar tabla
                                        st.dataframe(
                                            resultado_final.style.apply(highlight_row, axis=1),
                                            use_container_width=True,
                                            height=500,
                                            column_config={
                                                "Fecha": st.column_config.TextColumn("Fecha", width="small"),
                                                "nipd": st.column_config.TextColumn("nipd", width="small"),
                                                "nomCeller": st.column_config.TextColumn("nomCeller", width="medium"),
                                                "tiquetBascula_extranet": st.column_config.TextColumn("tiquetBascula extranet", width="medium"),
                                                "kg_extranet": st.column_config.NumberColumn("kg extranet", format="%.0f"),
                                                "tiquetBascula_eRVC": st.column_config.TextColumn("tiquetBascula eRVC", width="medium"),
                                                "kg_eRVC": st.column_config.NumberColumn("kg eRVC", format="%.0f"),
                                                "Discrepancia": st.column_config.TextColumn("Discrepancia", width="small"),
                                                "Hora_primera_pesada": st.column_config.TextColumn("Hora primera pesada", width="small"),
                                                "Hora_ultima_pesada": st.column_config.TextColumn("Hora última pesada", width="small")
                                            }
                                        )
                                        
                                        # Botón para descargar Excel
                                        if st.button("💾 Descargar comparacion_pesadas.xlsx"):
                                            # Crear archivo Excel con formato
                                            output = BytesIO()
                                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                                resultado_final.to_excel(writer, sheet_name='Comparacion_Pesadas', index=False)
                                                
                                                # Obtener workbook y worksheet para formato
                                                workbook = writer.book
                                                worksheet = writer.sheets['Comparacion_Pesadas']
                                                
                                                # Formato para discrepancias > 15%
                                                red_format = workbook.add_format({'bg_color': '#ffcdd2'})
                                                
                                                # Aplicar formato condicional
                                                worksheet.conditional_format('H2:H{}'.format(len(resultado_final) + 1), {
                                                    'type': 'cell',
                                                    'criteria': '>',
                                                    'value': 15,
                                                    'format': red_format
                                                })
                                            
                                            st.download_button(
                                                label="📥 Descargar archivo",
                                                data=output.getvalue(),
                                                file_name="comparacion_pesadas.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            )
                                        
                                        # Métricas resumen
                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            total_registros = len(resultado_final[resultado_final['nomCeller'] != 'TOTALES'])
                                            create_metric_card(
                                                "Total Registros",
                                                total_registros,
                                                "Pesadas individuales",
                                                "📋"
                                            )
                                        with col2:
                                            grupos_totales = len(resultado_final[resultado_final['nomCeller'] == 'TOTALES'])
                                            create_metric_card(
                                                "Grupos Fecha/NIPD",
                                                grupos_totales,
                                                "Agrupaciones",
                                                "📊"
                                            )
                                        with col3:
                                            discrepancias_altas = len(resultado_final[
                                                (resultado_final['Discrepancia'] > 15) & 
                                                (resultado_final['nomCeller'] == 'TOTALES')
                                            ])
                                            create_metric_card(
                                                "Grupos >15%",
                                                discrepancias_altas,
                                                f"De {grupos_totales} grupos",
                                                "⚠️"
                                            )
                                        with col4:
                                            total_kg_extranet = resultado_final[resultado_final['nomCeller'] == 'TOTALES']['kg_extranet'].sum()
                                            create_metric_card(
                                                "Total Kg Extranet",
                                                f"{total_kg_extranet:,.0f}",
                                                "Suma total",
                                                "🏭"
                                            )
                                    else:
                                        st.warning("⚠️ No se pudieron procesar los datos")
                                        
                                except Exception as e:
                                    # Error silencioso - no mostrar en la interfaz
                                    pass
                            else:
                                st.warning("⚠️ No hay datos detallados disponibles para las bodegas controladas")
                        else:
                            st.info("📊 No hay bodegas controladas para mostrar el detalle")

                else:
                    st.info("📅 Selecciona al menos una fecha para ver el análisis")
            else:
                st.warning("⚠️ No se encontraron fechas válidas en los datos")
        else:
            # Verificación silenciosa - no mostrar error
            pass
            
    except Exception as e:
        # Error silencioso - no mostrar en la interfaz
        pass
else:
    # Mensaje de bienvenida mejorado
    st.info("👋 **¡Bienvenido!** Sube los archivos eRVC y Extranet para comenzar el análisis.")
    
    # Información adicional con diseño atractivo
    st.markdown("""
    ### 📖 Instrucciones de Uso
    
    1. **📋 Carga el archivo eRVC** - Este será tu archivo de referencia (MASTER)
    2. **🌐 Carga el archivo Extranet** - Declaraciones del verificador para procesar
    3. **🔍 Analiza los resultados** - Revisa discrepancias y errores detectados
    4. **💾 Exporta los datos** - Descarga los resultados procesados
    
    ### ✨ Características
    
    - 🧹 **Limpieza automática** de datos Extranet
    - 🔍 **Detección de errores** en NIF y tickets
    - 📊 **Visualizaciones interactivas** de discrepancias
    - 🏭 **Filtrado por bodegas** para análisis específicos
    - 💾 **Exportación** de resultados en Excel
    """)
