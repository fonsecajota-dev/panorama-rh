# app.py (Versão Refatorada - Design System NT Transportes)

import bcrypt
import locale
import gspread
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo
import os

# ==============================================================================
# 🎨 DESIGN TOKENS E CONFIGURAÇÃO DA PÁGINA
# ==============================================================================
st.set_page_config(
    page_title="Recursos Humanos - NT Transportes",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

C = {
    "deep":   "#0D1B2A",
    "mid":    "#1E3A5F",
    "cyan":   "#00B4D8",
    "green":  "#2DC653",
    "amber":  "#F4A261",
    "red":    "#E63946",
    "sky":    "#8ECAE6",
    "muted":  "#5C677D",
    "border": "#E4E9F0",
}

# --- CSS Global ---
st.markdown(
    f"""<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;600;700&family=IBM+Plex+Mono:wght@400;500;700&display=swap');
    html, body, [class*="css"] {{ font-family: 'IBM Plex Sans', sans-serif !important; }}
    
    /* Customização da Sidebar */
    [data-testid="stSidebar"] {{ background-color: #F8F9FA !important; border-right: 1px solid {C['border']} !important; }}
    [data-testid="stSidebar"] hr {{ border-color: {C['border']} !important; margin: 1.2rem 0 !important; }}
    
    /* Customização de Expanders */
    div[data-testid="stExpander"] summary {{
        background-color: #f8f9fa; border: 1px solid {C['border']}; border-radius: 8px;
        padding: 12px; font-size: 0.95rem; font-weight: 600; color: {C['mid']};
    }}
    div[data-testid="stExpander"] summary:hover {{ background-color: #f1f3f5; }}
    </style>""",
    unsafe_allow_html=True,
)

# ==============================================================================
# 🛠️ HELPERS DE INTERFACE (UI)
# ==============================================================================
def kpi_card(col, icon: str, label: str, value: str, sub: str = "", accent: str = "#00B4D8"):
    col.markdown(
        f"""<div style="background:#fff; border:1px solid {C['border']}; 
                border-top:4px solid {accent}; border-radius:12px; 
                padding:18px 20px 14px 20px; height: 100%;
                box-shadow:0 2px 8px rgba(0,0,0,0.04);">
            <div style="font-size:1.25rem; margin-bottom:4px;">{icon}</div>
            <div style="font-size:0.67rem; font-weight:700; letter-spacing:0.07em; 
                        text-transform:uppercase; color:{C['muted']}; margin-bottom:5px;">
                {label}</div>
            <div style="font-size:1.45rem; font-weight:700; color:{C['deep']}; 
                        font-family:'IBM Plex Mono',monospace; line-height:1.15;">
                {value}</div>
            <div style="font-size:0.75rem; color:{C['muted']}; margin-top:8px;">{sub}</div>
        </div>""",
        unsafe_allow_html=True,
    )

def sec(title: str):
    st.markdown(
        f"""<div style="font-size:0.67rem; font-weight:800; letter-spacing:0.1em; 
                text-transform:uppercase; color:{C['muted']}; 
                border-bottom:2px solid {C['border']}; 
                padding-bottom:7px; margin:32px 0 16px 0;">{title}</div>""",
        unsafe_allow_html=True,
    )

def plotly_layout(**kw) -> dict:
    base = dict(
        font=dict(family="IBM Plex Sans, sans-serif", color=C["deep"]),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        separators=",.",
        margin=dict(t=44, b=36, l=40, r=20),
        xaxis=dict(showgrid=False, linecolor=C["border"]),
        yaxis=dict(gridcolor="#f0f0f5", linecolor=C["border"]),
        colorway=[C["cyan"], C["mid"], C["amber"], C["red"], C["sky"], C["green"]],
    )
    base.update(kw)
    return base

# ==============================================================================
# 🔐 AUTENTICAÇÃO E CONEXÃO DB
# ==============================================================================
try:
    connection_string = st.secrets["supabase"]["connection_string"]
    engine = create_engine(connection_string)
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    engine = None

def verify_password(plain_password: str, hashed_password_from_db: bytes) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password_from_db)

def authenticate_user(email, senha):
    if not email or not senha: return None
    if not engine: return None
    try:
        with engine.connect() as conn:
            query = text("SELECT id, nome, email, senha, departamento FROM usuarios WHERE email = :email")
            result = conn.execute(query, {"email": email}).fetchone()
            if result:
                user_data = dict(result._mapping)
                if verify_password(senha, user_data['senha'].encode('utf-8')):
                    del user_data['senha']
                    return user_data
    except Exception as e:
        st.error(f"Erro na autenticação: {e}")
    return None

def get_logged_user():
    return st.session_state.get('user')

def logout():
    if 'user' in st.session_state:
        del st.session_state['user']
    st.rerun()

DEPARTAMENTOS_AUTORIZADOS_PARA_ACOES = ["gerencia", "master", "rh"]

# ==============================================================================
# 🔄 FUNÇÕES DE DADOS (PANDAS E DB)
# ==============================================================================
def reset_app_state(engine):
    st.cache_data.clear() 
    if 'df_principal' in st.session_state:
        df_principal_antigo = st.session_state['df_principal']
        df_anotacoes_novo, df_contratacoes_novo = carregar_dados_banco(engine)
        colunas_anotacao = ['nome_usuario', 'categoria', 'justificativa']
        df_sem_anotacoes = df_principal_antigo.drop(columns=[col for col in colunas_anotacao if col in df_principal_antigo.columns])
        df_principal_atualizado = pd.merge(df_sem_anotacoes, df_anotacoes_novo, on='id_registro_original', how='left')
        for col in colunas_anotacao:
            if col in df_principal_atualizado.columns:
                df_principal_atualizado[col] = df_principal_atualizado[col].fillna('')
        st.session_state['df_principal'] = df_principal_atualizado
        st.session_state['df_contratacoes'] = df_contratacoes_novo
        if 'df_anotacao_original_indexed' in st.session_state:
            del st.session_state['df_anotacao_original_indexed']
    st.rerun()

@st.cache_data
def converte_df_para_csv(df):
    return df.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')

@st.cache_data
def converter_hora_para_decimal(tempo_str):
    if pd.isna(tempo_str) or tempo_str in ['00:00:00', '']: return 0.0
    try:
        partes = str(tempo_str).split(':')
        horas = int(partes[0]); minutos = int(partes[1]); segundos = int(partes[2]) if len(partes) > 2 else 0
        return horas + (minutos / 60) + (segundos / 3600)
    except: return 0.0

def format_BRL(valor):
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        return locale.currency(valor, grouping=True, symbol=True)
    except:
        if isinstance(valor, (int, float)):
            return f"R$ {valor:_.2f}".replace('.', ',').replace('_', '.')
        return "R$ 0,00"

def format_horas_decimal(horas_decimais):
    try:
        if pd.isna(horas_decimais) or horas_decimais < 0.01: return "0:00h"
        horas_inteiras = int(horas_decimais)
        minutos = int((horas_decimais - horas_inteiras) * 60)
        return f"{horas_inteiras:,}".replace(",", ".") + f":{minutos:02d}h"
    except: return "Inválido"

@st.cache_data(ttl=300, show_spinner="Carregando dados de horas extras...")
def carregar_horas_e_operacao(_gs_client, nome_planilha):
    try:
        planilha = _gs_client.open(nome_planilha)
        MAPA_FILIAIS = {'VAL': 'Valinhos', 'RIB': 'Ribeirão', 'MAR': 'Marília', 'JAC': 'Jacareí', 'GRU': 'Guarulhos'}        
        lista_dfs = []
        for nome_aba in MAPA_FILIAIS:
            registros = planilha.worksheet(nome_aba).get_all_records(head=1)
            if registros:
                df_temp = pd.DataFrame(registros)
                df_temp['filial'] = nome_aba
                lista_dfs.append(df_temp)

        if not lista_dfs: return pd.DataFrame()
        
        df_horas = pd.concat(lista_dfs, ignore_index=True)
        df_operacao = pd.DataFrame(planilha.worksheet('OPERACAO').get_all_records(head=1))

        df_horas.columns = [str(col).strip().lower() for col in df_horas.columns]
        df_operacao.columns = [str(col).strip().lower() for col in df_operacao.columns]
        
        df_horas.rename(columns={'colaborador': 'nome', 'função': 'funcao', 'salario base': 'salario_base', 'qtd he 50%': 'qtd_he_50%', 'qtd he 100%': 'qtd_he_100%', 'valor he 50%': 'valor_he_50%', 'valor he 100%': 'valor_he_100%', 'valor total': 'valor_total'}, inplace=True)
        df_operacao.rename(columns={'função': 'funcao'}, inplace=True)
        
        df_horas['nome'] = df_horas['nome'].astype(str).str.strip().str.upper()
        df_operacao['nome'] = df_operacao['nome'].astype(str).str.strip().str.upper()

        for col in ['valor_he_50%', 'valor_he_100%', 'valor_total']:
            if col in df_horas.columns:
                df_horas[col] = pd.to_numeric(df_horas[col].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip(), errors='coerce').fillna(0)

        df_horas['data'] = pd.to_datetime(df_horas['data'], errors='coerce', dayfirst=True)
        df_horas.dropna(subset=['data', 'nome'], inplace=True)
        
        df_completo = pd.merge(df_horas, df_operacao[['nome', 'cargo']], on='nome', how='left')
        df_completo['cargo'] = df_completo['cargo'].fillna('Não Classificado')
        return df_completo
    except Exception as e:
        st.error(f"Erro ao carregar dados de Horas Extras: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner="Carregando quadro de colaboradores...")
def carregar_colaboradores(_gs_client, nome_planilha):
    try:
        registros = _gs_client.open(nome_planilha).worksheet('COLABORADORES').get_all_records(head=1)
        if not registros: return pd.DataFrame()
        df = pd.DataFrame(registros)
        df.columns = [str(c).lower().strip() for c in df.columns]
        for col in ['filial', 'situação', 'colaborador', 'função']:
            if col in df.columns: df[col] = df[col].str.strip()
        df['situação'] = df['situação'].str.upper()
        df['colaborador'] = df['colaborador'].str.upper()
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner="Buscando dados do banco...")
def carregar_dados_banco(_engine):
    df_ano, df_cont = pd.DataFrame(), pd.DataFrame()
    if not _engine: return df_ano, df_cont
    try:
        with _engine.connect() as conn:
            df_ano = pd.DataFrame(conn.execute(text("SELECT id_registro_original, nome_usuario, categoria, justificativa FROM anotacoes")).fetchall(), columns=['id_registro_original', 'nome_usuario', 'categoria', 'justificativa'])
            df_cont = pd.DataFrame(conn.execute(text("""WITH RankedRH AS (SELECT filial_descricao, contratacoes_pendentes, ROW_NUMBER() OVER(PARTITION BY filial_descricao ORDER BY data_registro DESC, id DESC) as rn FROM rh_duplicate) SELECT filial_descricao, contratacoes_pendentes FROM RankedRH WHERE rn = 1;""")).fetchall(), columns=['filial_descricao', 'contratacoes_pendentes'])
    except Exception as e: st.error(f"Erro ao buscar dados: {e}")
    return df_ano, df_cont

@st.cache_data(ttl=300, show_spinner="Sincronizando dados...")
def carregar_e_processar_dados_iniciais(_gs_client, _engine, nome_planilha):
    df_horas = carregar_horas_e_operacao(_gs_client, nome_planilha)
    df_colab = carregar_colaboradores(_gs_client, nome_planilha)
    df_ano, df_cont = carregar_dados_banco(_engine)

    if df_horas.empty: return None, None, None

    df_horas['qtd_he_50%_dec'] = df_horas['qtd_he_50%'].apply(converter_hora_para_decimal)
    df_horas['qtd_he_100%_dec'] = df_horas['qtd_he_100%'].apply(converter_hora_para_decimal)
    df_horas['id_registro_original'] = df_horas['nome'].astype(str) + '_' + df_horas['data'].dt.strftime('%Y-%m-%d')
    
    if not df_ano.empty:
        df = pd.merge(df_horas, df_ano, on='id_registro_original', how='left')
    else:
        df = df_horas.copy()
        df['nome_usuario'] = None; df['categoria'] = ''; df['justificativa'] = ''

    df['nome_usuario'] = df['nome_usuario'].fillna('')
    df['categoria'] = df['categoria'].fillna('')
    df['justificativa'] = df['justificativa'].fillna('')
    df = df.loc[:, ~df.columns.duplicated()]

    def det_periodo(data):
        if data.day > 20:
            dp = data + pd.DateOffset(months=1)
            return dp.year, dp.month
        return data.year, data.month
    
    df[['ano_comercial', 'mes_comercial']] = df['data'].apply(lambda d: pd.Series(det_periodo(d)))
    return df, df_colab, df_cont

# ==============================================================================
# 📊 LÓGICA DO DASHBOARD PRINCIPAL
# ==============================================================================
def run_dashboard():
    # ─── HEADER EXECUTIVO ───────────────────────────────
    st.markdown(
        f"""<div style="background:linear-gradient(135deg,{C['deep']},{C['mid']});
                border-radius:16px; padding:26px 30px; margin-bottom:26px; color:#fff;">
            <div style="font-size:1.55rem; font-weight:700; margin-bottom:5px;
                        letter-spacing:-0.01em;">👥 Dashboard de Recursos Humanos</div>
            <div style="font-size:0.83rem; opacity:.70; max-width:580px;">
                Gestão de horas extras, quadro de colaboradores, anotações de gestores e projeção de custos com encargos.
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    usuario_logado = get_logged_user()
    departamento_usuario = usuario_logado.get("departamento", "").strip().lower() 
    NOME_DA_PLANILHA = "bdBANCO DE HORAS"

    if 'data_loaded' not in st.session_state:
        gs_client = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        df, df_colab, df_cont = carregar_e_processar_dados_iniciais(gs_client, engine, NOME_DA_PLANILHA)
        
        if df is None:
            st.warning("Não há dados de horas extras válidos para exibir.")
            st.stop()
            
        st.session_state['df_principal'] = df
        st.session_state['df_colaboradores'] = df_colab
        st.session_state['df_contratacoes'] = df_cont
        st.session_state['data_loaded'] = True

    df = st.session_state['df_principal']
    df_colaboradores = st.session_state['df_colaboradores']
    df_contratacoes = st.session_state['df_contratacoes']

    # --- BARRA LATERAL (SIDEBAR) ---
    with st.sidebar:
        st.info(f"Olá, **{usuario_logado.get('nome', 'Usuário')}**")
        
        if st.button("🚪 Sair", use_container_width=True): logout()
        if st.button("🔄 Forçar Sincronização", use_container_width=True):
            st.cache_data.clear()
            for k in ['data_loaded', 'df_principal', 'df_colaboradores', 'df_contratacoes']:
                if k in st.session_state: del st.session_state[k]
            st.rerun()
            
        st.markdown("---")
        st.markdown("<div style='font-size:0.75rem; font-weight:700; color:#5C677D; text-transform:uppercase; margin-bottom:8px;'>📅 Período de Análise</div>", unsafe_allow_html=True)

        meses_pt = {1:'Janeiro', 2:'Fevereiro', 3:'Março', 4:'Abril', 5:'Maio', 6:'Junho', 7:'Julho', 8:'Agosto', 9:'Setembro', 10:'Outubro', 11:'Novembro', 12:'Dezembro'}
        anos_disp = sorted(df['ano_comercial'].unique(), reverse=True)
        ano_sel = st.selectbox("Ano", anos_disp, index=0)
        
        df_ano = df[df['ano_comercial'] == ano_sel]
        meses_disp = sorted(df_ano['mes_comercial'].unique())
        meses_nomes = ['Todos'] + [meses_pt[m] for m in meses_disp]

        hoje = datetime.now()
        dt_ref = hoje + pd.DateOffset(months=1) if hoje.day > 20 else hoje
        idx_padrao = 0
        if ano_sel == dt_ref.year and meses_pt.get(dt_ref.month) in meses_nomes:
            idx_padrao = meses_nomes.index(meses_pt.get(dt_ref.month))
            
        mes_sel = st.selectbox("Mês", meses_nomes, index=idx_padrao)
        
        if mes_sel == 'Todos':
            df_periodo = df_ano.copy()
            st.caption(f"Exibindo ano **{ano_sel}**.")
        else:
            mes_num = next(k for k, v in meses_pt.items() if v == mes_sel)
            df_periodo = df_ano[df_ano['mes_comercial'] == mes_num].copy()
            dt_fim = pd.to_datetime(f'{ano_sel}-{mes_num}-20')
            dt_ini = (dt_fim - pd.DateOffset(months=1)).replace(day=21)
            st.caption(f"Período: **{dt_ini.strftime('%d/%m/%Y')}** a **{dt_fim.strftime('%d/%m/%Y')}**")

        mapa_filiais = {'Valinhos': 'Valinhos', 'Ribeirao': 'Ribeirão', 'Marilia': 'Marília', 'Jacareí': 'Jacareí', 'Guarulhos': 'Guarulhos'}
        filiais_disp = sorted(df_periodo['filial'].unique().tolist())
        nomes_filiais = ['Todas'] + [mapa_filiais.get(c, c) for c in filiais_disp]
        filial_sel = st.selectbox("Filial", nomes_filiais)

        df_filtrado = df_periodo.copy()
        if filial_sel != 'Todas':
            rev_map = {v: k for k, v in mapa_filiais.items()}
            cod_sel = rev_map.get(filial_sel, filial_sel)
            df_filtrado = df_periodo[df_periodo['filial'] == cod_sel].copy()

    if df_filtrado.empty:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
        st.stop()

    # --- CÁLCULOS KPI ---
    total_he_geral = df_filtrado['valor_total'].sum()
    custo_c_encargos = total_he_geral * 1.16 
    total_he_50 = df_filtrado['valor_he_50%'].sum()
    total_he_100 = df_filtrado['valor_he_100%'].sum()
    total_horas_dec = df_filtrado['qtd_he_50%_dec'].sum() + df_filtrado['qtd_he_100%_dec'].sum()
    colabs_he = df_filtrado[df_filtrado['valor_total'] > 0]['nome'].nunique()

    tot_pendentes = 0
    if not df_contratacoes.empty:
        if filial_sel == 'Todas': tot_pendentes = int(df_contratacoes['contratacoes_pendentes'].sum())
        else:
            df_c_f = df_contratacoes[df_contratacoes['filial_descricao'] == filial_sel]
            if not df_c_f.empty: tot_pendentes = int(df_c_f['contratacoes_pendentes'].iloc[0])

    tot_ativos, tot_inativos, tot_geral = 0, 0, 0
    lista_ativos_df, lista_inativos_df = pd.DataFrame(), pd.DataFrame()
    if not df_colaboradores.empty:
        df_c_f = df_colaboradores[df_colaboradores['filial'] == filial_sel] if filial_sel != 'Todas' else df_colaboradores.copy()
        if not df_c_f.empty:
            lista_ativos_df = df_c_f[df_c_f['situação'] == 'TRABALHANDO']
            lista_inativos_df = df_c_f[df_c_f['situação'] != 'TRABALHANDO']
            tot_ativos = len(lista_ativos_df)
            tot_inativos = len(lista_inativos_df)
            tot_geral = len(df_c_f)

    # --- SEÇÃO 1: KPIs PRINCIPAIS ---
    c1, c2, c3 = st.columns(3)
    kpi_card(c1, "💰", "Custo Total (HE)", format_BRL(total_he_geral), f"Com Encargos (16%): {format_BRL(custo_c_encargos)}", C["cyan"])
    kpi_card(c2, "⏳", "Total de Horas Extras", format_horas_decimal(total_horas_dec), "Somatório de 50% e 100%", C["mid"])
    kpi_card(c3, "👷", "Colaboradores com HE", str(colabs_he), "Realizaram horas no período", C["amber"])

    st.markdown("<div style='margin-bottom:16px'></div>", unsafe_allow_html=True)

    # --- SEÇÃO 2: KPIs SECUNDÁRIOS ---
    sec("📊 Distribuição de Custos e Equipe")
    k1, k2, k3, k4 = st.columns(4)
    kpi_card(k1, "📈", "Custo HE 50%", format_BRL(total_he_50), "Horas úteis", C["sky"])
    kpi_card(k2, "🔥", "Custo HE 100%", format_BRL(total_he_100), "Domingos e Feriados", C["red"])
    kpi_card(k3, "🟢", "Colaboradores Ativos", str(tot_ativos), f"De {tot_geral} cadastrados", C["green"])
    kpi_card(k4, "📝", "Contratações Pendentes", str(tot_pendentes), "Vagas abertas no RH", C["amber"])

    # Listas de Colaboradores em Expanders
    c_exp1, c_exp2 = st.columns(2)
    with c_exp1:
        with st.expander(f"🟢 Ver Lista de Ativos ({tot_ativos})"):
            if not lista_ativos_df.empty:
                df_exib = lista_ativos_df[['colaborador', 'filial', 'situação']].rename(columns=lambda x: x.title())
                st.dataframe(df_exib, use_container_width=True, hide_index=True)
            else: st.info("Nenhum ativo.")
    with c_exp2:
        with st.expander(f"🔴 Ver Lista de Inativos ({tot_inativos})"):
            if not lista_inativos_df.empty:
                df_exib = lista_inativos_df[['colaborador', 'função', 'filial', 'situação']].rename(columns=lambda x: x.title())
                st.dataframe(df_exib, use_container_width=True, hide_index=True)
            else: st.info("Nenhum inativo.")

    st.markdown("<div style='margin-bottom:32px'></div>", unsafe_allow_html=True)

    # --- SEÇÃO 3: GRÁFICOS ---
    cg1, cg2 = st.columns(2)
    
    with cg1:
        sec("💼 Custo de HE por Cargo")
        if 'selected_cargo' not in st.session_state: st.session_state.selected_cargo = None

        if st.session_state.selected_cargo:
            st.markdown(f"**Detalhamento: {st.session_state.selected_cargo}**")
            df_det = df_filtrado[(df_filtrado['cargo'] == st.session_state.selected_cargo) & (df_filtrado['valor_total'] > 0)].copy()
            df_det['Data'] = pd.to_datetime(df_det['data']).dt.strftime('%d/%m/%Y')
            df_det['Valor'] = df_det['valor_total'].apply(format_BRL)
            st.dataframe(df_det[['Data', 'nome', 'filial', 'Valor']].rename(columns={'nome':'Colaborador','filial':'Filial'}), use_container_width=True, hide_index=True)
            if st.button("⬅️ Voltar"):
                st.session_state.selected_cargo = None
                st.rerun()
        else:
            custo_cargo = df_filtrado.groupby('cargo')['valor_total'].sum().sort_values(ascending=True).reset_index()
            fig_bar = go.Figure(go.Bar(
                x=custo_cargo['valor_total'], y=custo_cargo['cargo'], orientation='h',
                marker_color=C["cyan"], text=custo_cargo['valor_total'].apply(format_BRL), textposition='auto',
                hovertemplate='<b>%{y}</b><br>Custo: R$ %{x:,.2f}<extra></extra>'
            ))
            fig_bar.update_layout(**plotly_layout(height=400, margin=dict(l=0, r=0, t=10, b=0)))
            st.plotly_chart(fig_bar, use_container_width=True)

            cargos = ["-- Ver detalhes de um cargo --"] + custo_cargo['cargo'].tolist()
            sel_cargo = st.selectbox("Análise Detalhada:", options=cargos, label_visibility="collapsed")
            if sel_cargo != cargos[0]:
                st.session_state.selected_cargo = sel_cargo
                st.rerun()

    with cg2:
        sec("📈 Evolução do Custo Diário")
        if filial_sel == 'Todas':
            custo_dia = df_periodo.groupby(['data', 'filial'])['valor_total'].sum().reset_index()
            custo_dia['filial'] = custo_dia['filial'].map(mapa_filiais).fillna(custo_dia['filial'])
            fig_line = px.line(custo_dia, x='data', y='valor_total', color='filial', markers=True)
        else:
            custo_dia = df_filtrado.groupby('data')['valor_total'].sum().reset_index()
            fig_line = px.line(custo_dia, x='data', y='valor_total', markers=True)
            fig_line.update_traces(line_color=C["cyan"])

        fig_line.update_traces(hovertemplate='<b>%{x|%d/%m/%Y}</b><br>Custo: R$ %{y:,.2f}<extra></extra>')
        fig_line.update_layout(**plotly_layout(height=400, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Custo Diário (R$)"))
        st.plotly_chart(fig_line, use_container_width=True)

    # --- SEÇÃO 4: ANOTAÇÕES E JUSTIFICATIVAS ---
    st.markdown("<div style='margin-bottom:32px'></div>", unsafe_allow_html=True)
    sec("📝 Registro de Ocorrências e Justificativas")

    cf1, cf2 = st.columns([1.5, 3])
    with cf1: dt_anotacao = st.date_input("Filtrar Ocorrências por data:", value=date.today(), format="DD/MM/YYYY")
    with cf2: 
        st.write("")
        ver_todas = st.checkbox("Exibir mês completo (ignora filtro de data)")

    df_anotar = df_filtrado[df_filtrado['valor_total'] > 0].copy() if ver_todas else df_filtrado[(df_filtrado['data'].dt.date == dt_anotacao) & (df_filtrado['valor_total'] > 0)].copy()

    if df_anotar.empty:
        st.info("Nenhuma hora extra registrada para os filtros selecionados.")
    else:
        df_edit = df_anotar.copy()
        df_edit['Data'] = pd.to_datetime(df_edit['data']).dt.strftime('%d/%m/%Y')
        df_edit['Valor'] = df_edit['valor_total'].apply(format_BRL)
        df_edit.rename(columns={'nome': 'Colaborador', 'cargo': 'Cargo', 'qtd_he_50%': 'HE 50%', 'qtd_he_100%': 'HE 100%', 'categoria': 'Categoria', 'justificativa': 'Justificativa'}, inplace=True)
        df_edit.set_index('id_registro_original', inplace=True)
        
        st.session_state['df_anotacao_original_indexed'] = df_edit.copy()
        usr_atual = usuario_logado.get('nome', '').strip()
        
        mask_edit = (df_edit['nome_usuario'].fillna('').str.strip() == '') | (df_edit['nome_usuario'].fillna('').str.casefold() == usr_atual.casefold())
        df_meus = df_edit[mask_edit].copy()
        df_outros = df_edit[~mask_edit].copy()
        
        df_meus['Gestor'] = df_meus['nome_usuario'].apply(lambda x: x.strip() if str(x).strip() else '—')
        df_outros['Gestor'] = df_outros['nome_usuario'].apply(lambda x: x.strip() if str(x).strip() else '—')
        
        st.session_state['df_anotacao_original_indexed_meus'] = df_meus.copy()
        cols_exib = ['Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor', 'Categoria', 'Justificativa', 'Gestor']
        ops_cat = ["", "Absenteísmo", "Quadro de colaboradores", "Cliente", "Operações", "Outros"]

        st.markdown("###### ✏️ Suas Pendências / Linhas Livres")
        df_editado_meus = st.data_editor(
            df_meus[cols_exib], use_container_width=True, hide_index=True,
            column_config={
                "Categoria": st.column_config.SelectboxColumn("Motivo", options=ops_cat),
                "Justificativa": st.column_config.TextColumn("Justificativa (Obrigatória)")
            },
            disabled=['Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor', 'Gestor']
        )

        if not df_outros.empty:
            st.markdown("###### 🔒 Justificativas de outros Gestores")
            st.dataframe(df_outros[cols_exib], use_container_width=True, hide_index=True)

        if st.button("✔️ Salvar Anotações", type="primary"):
            try:
                df_orig_m = st.session_state['df_anotacao_original_indexed_meus']
                df_editado_meus.index = df_orig_m.index
                
                alt = df_editado_meus[
                    (df_editado_meus['Categoria'].fillna('') != df_orig_m['Categoria'].fillna('')) |
                    (df_editado_meus['Justificativa'].fillna('') != df_orig_m['Justificativa'].fillna(''))
                ]
                
                #tem_cat = alt['Categoria'].fillna('').str.strip() != ''
                tem_jus = alt['Justificativa'].fillna('').str.strip() != ''
                if not alt[tem_jus].empty:
                    st.error("❌ Motivo e Justificativa devem ser preenchidas juntas.")
                    st.stop()

                if not alt.empty:
                    with st.spinner("Salvando..."), engine.begin() as conn:
                        for id_reg, linha in alt.iterrows():
                            cat = (linha['Categoria'] or "").strip()
                            jus = (linha['Justificativa'] or "").strip()
                            if not cat and not jus:
                                conn.execute(text("DELETE FROM anotacoes WHERE id_registro_original = :id"), {"id": id_reg})
                            else:
                                conn.execute(text("""
                                    INSERT INTO anotacoes (id_registro_original, nome_usuario, categoria, justificativa) 
                                    VALUES (:id, :usr, :cat, :jus)
                                    ON CONFLICT (id_registro_original) DO UPDATE SET 
                                        categoria = EXCLUDED.categoria, justificativa = EXCLUDED.justificativa,
                                        nome_usuario = EXCLUDED.nome_usuario, data_modificacao = NOW();
                                """), {"id": id_reg, "usr": usr_atual, "cat": cat, "jus": jus})
                    st.success("Anotações salvas com sucesso!")
                    reset_app_state(engine)
                else:
                    st.info("Nenhuma alteração identificada.")
            except Exception as e: st.error(f"Erro ao salvar: {e}")

        # --- DIAGNÓSTICO DE NOMES ---
        df_nc = df_filtrado[df_filtrado['cargo'] == 'Não Classificado']
        if not df_nc.empty:
            st.markdown("---")
            sec("🚨 Colaboradores Não Mapeados")
            st.caption("Nomes que realizaram HE mas não foram encontrados na aba OPERACAO do sistema.")
            res = df_nc.groupby(['nome', 'filial']).agg(Custo=('valor_total','sum'), Ocorrencias=('nome','count')).reset_index()
            res['Custo'] = res['Custo'].apply(format_BRL)
            st.dataframe(res.rename(columns={'nome':'Colaborador','filial':'Filial'}), use_container_width=True, hide_index=True)

# ==============================================================================
# INÍCIO DO APLICATIVO
# ==============================================================================
# Injeção CSS para tela de login (Esconde a Sidebar)
if not get_logged_user():
    st.markdown("""<style>[data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none !important; }</style>""", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown(f"<div style='text-align:center; margin-bottom:20px;'><h2 style='color:{C['deep']};'>🔐 Autenticação</h2><p style='color:{C['muted']};'>Insira as suas credenciais para acessar o painel de RH.</p></div>", unsafe_allow_html=True)
            with st.form("login_form_central"):
                email = st.text_input("📧 **Email**")
                senha = st.text_input("🔑 **Senha**", type="password")
                st.markdown("<br>", unsafe_allow_html=True)
                if st.form_submit_button("Acessar Painel", use_container_width=True, type="primary"):
                    user_info = authenticate_user(email, senha)
                    if user_info:
                        st.session_state['user'] = user_info
                        st.rerun()
                    else: st.error("Email ou senha inválidos.")
else:
    run_dashboard()
