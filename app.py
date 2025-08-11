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
    for p in pkgs:
        if pkgutil.find_loader(p) is None:
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
    if pd.isna(s): return ""
    s = str(s).strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'[:;]+$', '', s).strip()
    return s

def build_normalized_map(columns):
    return {normalize_text(c): c for c in columns}

def safe_get_col(map_norm, candidates):
    for cand in candidates:
        key = normalize_text(cand)
        if key in map_norm:
            return map_norm[key]
    return None

def detect_header_row(xl_bytes, sheet_index=0, max_probe_rows=20, min_hits=3):
    probe = pd.read_excel(BytesIO(xl_bytes), sheet_name=sheet_index, header=None, nrows=max_probe_rows)
    targets = set([
        "dia y hora", "razon social", "nipbd", "nombre y apellidos viticultor",
        "nif viticultor", "variedad", "total kg", "%", "grado", "zona"
    ])
    for r in range(len(probe)):
        row_vals = set(normalize_text(v) for v in probe.iloc[r].tolist())
        if len(targets.intersection(row_vals)) >= min_hits:
            return r
    return None

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

# Header principal con diseño moderno
st.markdown("""
<div class="main-header">
    <h1>📊 Análisis de Discrepancias CAT</h1>
    <p>Sistema de comparación entre eRVC (MASTER) y DeclaracionVerificador (EXTRANET)</p>
</div>
""", unsafe_allow_html=True)

file_ervc = st.file_uploader("Sube el archivo eRVC (MASTER)", type=["xlsx"])
file_ext = st.file_uploader("Sube el archivo DeclaracionVerificador (EXTRANET)", type=["xlsx"])

if file_ervc and file_ext:
    try:
        eRVC_df = pd.ExcelFile(file_ervc).parse(0)
        if 'nipd' in eRVC_df.columns:
            # Convertir a string preservando el formato original
            eRVC_df['nipd'] = eRVC_df['nipd'].apply(lambda x: str(x).replace('.', '').replace(',', '').strip() if pd.notna(x) else '')
        extranet_df = limpiar_extranet(file_ext.read(), eRVC_df)

        st.session_state.eRVC_df = eRVC_df
        st.session_state.extranet = extranet_df

        st.subheader("Vista previa Extranet")
        st.dataframe(extranet_df.head(20), use_container_width=True)

        # Cálculos para cards
        if 'nomCeller' in extranet_df.columns and 'nomCeller' in eRVC_df.columns:
            unique_extranet = extranet_df['nomCeller'].nunique()
            unique_ervc = eRVC_df['nomCeller'].nunique()
            total_kg_extranet = extranet_df.get('Kg:', pd.Series()).sum()
            total_kg_ervc = eRVC_df.get('kgTotals', pd.Series()).sum()
            diff_porcentual = 0
            if total_kg_ervc != 0:
                diff_porcentual = ((total_kg_ervc - total_kg_extranet) / total_kg_ervc) * 100

            # Usar metric cards modernas si está disponible shadcn-ui
            if ui is not None:
                cols = st.columns(5)
                with cols[0]:
                    ui.metric_card(
                        title="Bodegas Extranet", 
                        content=str(unique_extranet), 
                        description="Bodegas únicas registradas", 
                        key="card_extranet"
                    )
                with cols[1]:
                    ui.metric_card(
                        title="Bodegas eRVC", 
                        content=str(unique_ervc), 
                        description="Bodegas únicas en sistema", 
                        key="card_ervc"
                    )
                with cols[2]:
                    ui.metric_card(
                        title="Total Kg Extranet", 
                        content=f"{total_kg_extranet:,.0f}", 
                        description="Kilogramos registrados", 
                        key="card_kg_ext"
                    )
                with cols[3]:
                    ui.metric_card(
                        title="Total Kg eRVC", 
                        content=f"{total_kg_ervc:,.0f}", 
                        description="Kilogramos en sistema", 
                        key="card_kg_ervc"
                    )
                with cols[4]:
                    ui.metric_card(
                        title="Diferencia %", 
                        content=f"{diff_porcentual:.2f}%", 
                        description="Variación entre sistemas", 
                        key="card_diff"
                    )
            else:
                # Fallback a métricas estándar de Streamlit
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("nomCeller únicos Extranet", unique_extranet)
                col2.metric("nomCeller únicos eRVC", unique_ervc)
                col3.metric("Total Kg Extranet", total_kg_extranet)
                col4.metric("kgTotals eRVC", total_kg_ervc)
                col5.metric("Diferencia Porcentual Kg (%)", f"{diff_porcentual:.2f}%")

        # Mostrar errores con diseño mejorado
        df_errores = generar_reporte_errores(extranet_df)
        if not df_errores.empty:
            st.markdown("### 🚨 Reporte de Errores de Introducción")
            
            # Mostrar tabla de errores con mejor formato
            st.dataframe(
                df_errores, 
                use_container_width=True,
                column_config={
                    "Verificador": st.column_config.TextColumn("👤 Verificador"),
                    "Cantidad": st.column_config.NumberColumn("📊 Cantidad", format="%d"),
                    "Tipo error": st.column_config.TextColumn("⚠️ Tipo de Error")
                }
            )

            # Gráfico interactivo mejorado
            import plotly.express as px
            fig = px.bar(
                df_errores, 
                x='Verificador', 
                y='Cantidad', 
                color='Tipo error',
                title='📈 Distribución de Errores por Verificador',
                labels={'Cantidad': 'Número de Errores', 'Verificador': 'Verificador'},
                hover_data=['Tipo error', 'Cantidad'],
                color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
            )
            fig.update_layout(
                barmode='stack',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Arial, sans-serif", size=12),
                title_font_size=16
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("""
            <div class="success-card">
                <h3>✅ Excelente!</h3>
                <p>No se detectaron errores de introducción en los datos</p>
            </div>
            """, unsafe_allow_html=True)

        # Selector múltiple de nipd + nomCeller con diseño mejorado
        st.sidebar.markdown("### 🎯 Análisis Detallado por Bodegas")
        
        st.sidebar.markdown("""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; margin: 10px 0; border-left: 4px solid #007bff;">
            <p style="margin: 0; color: #495057; font-size: 12px;">💡 <strong>Tip:</strong> Selecciona bodegas específicas para un análisis más detallado de las discrepancias</p>
        </div>
        """, unsafe_allow_html=True)
        
        if "nipd" in eRVC_df.columns and "nomCeller" in eRVC_df.columns:
            opciones = (
                eRVC_df[["nipd", "nomCeller"]]
                .drop_duplicates()
                .sort_values("nipd")
                .apply(lambda x: f"{x['nipd']} – {x['nomCeller']}", axis=1)
                .tolist()
            )
            seleccion = st.sidebar.multiselect(
                "🏭 Selecciona bodegas para análisis detallado:",
                opciones,
                help="Puedes seleccionar múltiples bodegas para comparar sus métricas específicas"
            )
            if seleccion:
                st.write(f"Has seleccionado {len(seleccion)} nipd para comparar")
                selected_nipds = [s.split(' – ')[0] for s in seleccion]
                
                # Filtrar exactamente por los nipd seleccionados
                filtered_ervc = eRVC_df[eRVC_df['nipd'].isin(selected_nipds)]
                filtered_extranet = extranet_df[extranet_df['nipd'].isin(selected_nipds)]
                
                # Verificar qué nipd están presentes en cada dataset
                nipds_en_ervc = set(filtered_ervc['nipd'].unique())
                nipds_en_extranet = set(filtered_extranet['nipd'].unique())
                missing_in_extranet = set(selected_nipds) - nipds_en_extranet
                missing_in_ervc = set(selected_nipds) - nipds_en_ervc
                
                if missing_in_extranet:
                    st.warning(f"Los siguientes nipd no se encuentran en Extranet: {', '.join(missing_in_extranet)}")
                if missing_in_ervc:
                    st.warning(f"Los siguientes nipd no se encuentran en eRVC: {', '.join(missing_in_ervc)}")
                if not filtered_extranet.empty:
                    unique_extranet_sel = filtered_extranet['nomCeller'].nunique()
                    unique_ervc_sel = filtered_ervc['nomCeller'].nunique()
                    total_kg_extranet_sel = filtered_extranet.get('Kg:', pd.Series()).sum()
                    total_kg_ervc_sel = filtered_ervc.get('kgTotals', pd.Series()).sum()
                    diff_porcentual_sel = 0
                    if total_kg_ervc_sel != 0:
                        diff_porcentual_sel = ((total_kg_ervc_sel - total_kg_extranet_sel) / total_kg_ervc_sel) * 100
                    st.markdown("### 📊 Métricas para Bodegas Seleccionadas")
                    
                    # Usar metric cards modernas para selección
                    if ui is not None:
                        cols = st.columns(5)
                        with cols[0]:
                            ui.metric_card(
                                title="Bodegas Extranet", 
                                content=str(unique_extranet_sel), 
                                description="Seleccionadas", 
                                key="card_extranet_sel"
                            )
                        with cols[1]:
                            ui.metric_card(
                                title="Bodegas eRVC", 
                                content=str(unique_ervc_sel), 
                                description="Seleccionadas", 
                                key="card_ervc_sel"
                            )
                        with cols[2]:
                            ui.metric_card(
                                title="Kg Extranet", 
                                content=f"{total_kg_extranet_sel:,.0f}", 
                                description="Selección", 
                                key="card_kg_ext_sel"
                            )
                        with cols[3]:
                            ui.metric_card(
                                title="Kg eRVC", 
                                content=f"{total_kg_ervc_sel:,.0f}", 
                                description="Selección", 
                                key="card_kg_ervc_sel"
                            )
                        with cols[4]:
                            ui.metric_card(
                                title="Diferencia %", 
                                content=f"{diff_porcentual_sel:.2f}%", 
                                description="Variación", 
                                key="card_diff_sel"
                            )
                    else:
                        # Fallback a métricas estándar
                        col1, col2, col3, col4, col5 = st.columns(5)
                        col1.metric("nomCeller únicos Extranet (sel)", unique_extranet_sel)
                        col2.metric("nomCeller únicos eRVC (sel)", unique_ervc_sel)
                        col3.metric("Total Kg Extranet (sel)", total_kg_extranet_sel)
                        col4.metric("kgTotals eRVC (sel)", total_kg_ervc_sel)
                        col5.metric("Diferencia Porcentual Kg (%) (sel)", f"{diff_porcentual_sel:.2f}%")
        else:
            st.warning("No se encontraron columnas nipd o nomCeller en eRVC")

    except Exception as e:
        st.error(f"Error procesando archivos: {e}")
