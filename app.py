# app.py (Versão Otimizada)

from sqlalchemy import create_engine, text
from datetime import datetime, date
import plotly.express as px
import streamlit as st
import pandas as pd
import gspread
import bcrypt
import locale

# ==============================================================================
# CONFIGURAÇÃO DA PÁGINA E AUTENTICAÇÃO
# ==============================================================================
st.set_page_config(
    page_title="Recursos Humanos - NT Transportes",
    page_icon="👥",
    layout="wide"
)

# --- Conexão com Banco de Dados ---
try:
    connection_string = st.secrets["supabase"]["connection_string"]
    engine = create_engine(connection_string)
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    engine = None

# --- Funções de Segurança e Conexão ---
def verify_password(plain_password: str, hashed_password_from_db: bytes) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password_from_db)

def authenticate_user(email, senha):
    if not email or not senha: return None
    if not engine:
        st.error("Conexão com o banco de dados não estabelecida.")
        return None
    try:
        with engine.connect() as conn:
            query = text("SELECT id, nome, email, senha, departamento FROM usuarios WHERE email = :email")
            result = conn.execute(query, {"email": email}).fetchone()
            if result:
                user_data = dict(result._mapping)
                hashed_password_bytes = user_data['senha'].encode('utf-8')
                if verify_password(senha, hashed_password_bytes):
                    del user_data['senha']
                    return user_data
    except Exception as e:
        st.error(f"Erro na autenticação: {e}")
        return None
    return None

def get_logged_user():
    return st.session_state.get('user')

def logout():
    if 'user' in st.session_state:
        del st.session_state['user']
    st.rerun()

# ==============================================================================
# CONFIGURAÇÃO DE FUNÇÕES AUXILIARES E DE CARREGAMENTO MODULAR
# ==============================================================================
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
    except (ValueError, IndexError): return 0.0

def format_BRL(valor):
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        return locale.currency(valor, grouping=True, symbol=True)
    except (ValueError, TypeError, locale.Error):
        if isinstance(valor, (int, float)):
            return f"R$ {valor:_.2f}".replace('.', ',').replace('_', '.')
        return "R$ 0,00"

def format_horas_decimal(horas_decimais):
    try:
        if pd.isna(horas_decimais) or horas_decimais < 0.01: return "0:00h"
        horas_inteiras = int(horas_decimais)
        minutos = int((horas_decimais - horas_inteiras) * 60)
        horas_formatadas = f"{horas_inteiras:,}".replace(",", ".")
        minutos_formatados = f"{minutos:02d}"
        return f"{horas_formatadas}:{minutos_formatados}h"
    except (ValueError, TypeError): return "Inválido"

def exibir_kpi_secundario(label, value, icon=""):
    label_com_icon = f"{icon} {label}" if icon else label
    html = f"""
    <div style="background-color: #f0f2f6; border-radius: 10px; padding: 20px; text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
        <p style="font-size: 1.0em; font-weight: 600; color: #555; margin: 0; padding: 0;">{label_com_icon}</p>
        <p style="font-size: 1.8em; font-weight: bold; color: #004080; margin: 5px 0 0 0; padding: 0;">{value}</p>
    </div>
    """
    return html

# ==============================================================================
# FUNÇÕES DE CARREGAMENTO MODULAR
# ==============================================================================
@st.cache_data(ttl=300, show_spinner="Carregando dados de horas extras...")
def carregar_horas_e_operacao(_gs_client, nome_planilha):
    """Carrega e processa apenas os dados de Horas Extras e Operação."""
    try:
        planilha = _gs_client.open(nome_planilha)
        nomes_abas_filiais = ['VAL', 'RIB', 'MAR', 'JAC', 'GRU']
        lista_dfs_horas = []
        for nome_aba in nomes_abas_filiais:
            aba = planilha.worksheet(nome_aba)
            registros = aba.get_all_records(head=1)
            if not registros: continue
            df_temp = pd.DataFrame(registros)
            df_temp['filial'] = nome_aba
            lista_dfs_horas.append(df_temp)

        if not lista_dfs_horas: return pd.DataFrame()
        
        df_horas = pd.concat(lista_dfs_horas, ignore_index=True)
        aba_operacao = planilha.worksheet('OPERACAO')
        df_operacao = pd.DataFrame(aba_operacao.get_all_records(head=1))

        # Limpeza e transformação
        df_horas.columns = [str(col).strip().lower() for col in df_horas.columns]
        df_operacao.columns = [str(col).strip().lower() for col in df_operacao.columns]
        
        mapeamento = {'colaborador': 'nome', 'função': 'funcao', 'salario base': 'salario_base', 'qtd he 50%': 'qtd_he_50%', 'qtd he 100%': 'qtd_he_100%', 'valor he 50%': 'valor_he_50%', 'valor he 100%': 'valor_he_100%', 'valor total': 'valor_total'}
        df_horas.rename(columns=mapeamento, inplace=True)
        df_operacao.rename(columns={'função': 'funcao'}, inplace=True)
        
        df_horas['nome'] = df_horas['nome'].astype(str).str.strip().str.upper()
        df_operacao['nome'] = df_operacao['nome'].astype(str).str.strip().str.upper()

        colunas_valor = ['valor_he_50%', 'valor_he_100%', 'valor_total']
        for col in colunas_valor:
            if col in df_horas.columns:
                df_horas[col] = df_horas[col].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
                df_horas[col] = pd.to_numeric(df_horas[col], errors='coerce').fillna(0)

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
        # MODIFICAÇÃO: Adicionado 'função' à lista
        for col in ['filial', 'situação', 'colaborador', 'função']:
            if col in df.columns:
                df[col] = df[col].str.strip()
        df['situação'] = df['situação'].str.upper()
        df['colaborador'] = df['colaborador'].str.upper()
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.warning("Aba 'COLABORADORES' não encontrada."); return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar Colaboradores: {e}"); return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner="Buscando dados do banco...")
def carregar_dados_banco(_engine):
    """Carrega dados de anotações e contratações do banco de dados."""
    df_anotacoes = pd.DataFrame()
    df_contratacoes = pd.DataFrame()

    if not _engine:
        return df_anotacoes, df_contratacoes

    try:
        with _engine.connect() as conn:
            # Anotações
            query_anotacoes = text("SELECT id_registro_original, texto_anotacao, nome_usuario FROM anotacoes")
            df_anotacoes = pd.DataFrame(conn.execute(query_anotacoes).fetchall(), columns=['id_registro_original', 'texto_anotacao', 'nome_usuario'])

            # Contratações
            query_contratacoes = text("""
                WITH RankedRH AS (
                    SELECT filial_descricao, contratacoes_pendentes, ROW_NUMBER() OVER(PARTITION BY filial_descricao ORDER BY data_registro DESC, id DESC) as rn
                    FROM rh_duplicate
                ) SELECT filial_descricao, contratacoes_pendentes FROM RankedRH WHERE rn = 1;
            """)
            df_contratacoes = pd.DataFrame(conn.execute(query_contratacoes).fetchall(), columns=['filial_descricao', 'contratacoes_pendentes'])
            
    except Exception as e:
        st.error(f"Erro ao buscar dados do banco: {e}")
    
    return df_anotacoes, df_contratacoes

# ==============================================================================
# LÓGICA DO DASHBOARD
# ==============================================================================
def run_dashboard():
    st.title("👥 Dashboard de Recursos Humanos")

# --- NOVO BLOCO DE CSS PARA ESTILIZAR O EXPANDER ---
    st.markdown("""
    <style>
        /* Alvo é o cabeçalho do expander (o elemento <summary>) */
        div[data-testid="stExpander"] summary {
            background-color: #f0f2f6;
            border: 1px solid #e6eaf1;
            border-radius: 10px;
            padding: 12px;
            font-size: 1.05em;
            font-weight: 600;
            color: #004080;
            transition: background-color 0.2s ease-in-out;
        }
        /* Efeito hover para melhor feedback visual */
        div[data-testid="stExpander"] summary:hover {
            background-color: #e6eaf1;
        }
    </style>
    """, unsafe_allow_html=True)

    usuario_logado = get_logged_user()
    NOME_DA_PLANILHA = "bdBANCO DE HORAS"

    @st.cache_resource(ttl=300)
    def autenticar_google_sheets():
        try:
            return gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        except Exception as e:
            st.error(f"Falha na autenticação com o Google Sheets: {e}")
            return None

    # --- CARREGAMENTO MODULAR DOS DADOS ---
    gs_client = autenticar_google_sheets()
    if not gs_client: st.stop()

    df_horas = carregar_horas_e_operacao(gs_client, NOME_DA_PLANILHA)
    df_colaboradores = carregar_colaboradores(gs_client, NOME_DA_PLANILHA)
    df_anotacoes, df_contratacoes = carregar_dados_banco(engine)
    
    # --- PONTO DE PARADA SE DADOS ESSENCIAIS NÃO FOREM CARREGADOS ---
    if df_horas.empty:
        st.warning("Não há dados de horas extras válidos para exibir. O dashboard não pode continuar.")
        st.stop()
        
    # --- JUNÇÃO E PREPARAÇÃO FINAL DOS DADOS ---
    with st.spinner("Finalizando preparação dos dados..."):
        df_horas['qtd_he_50%_dec'] = df_horas['qtd_he_50%'].apply(converter_hora_para_decimal)
        df_horas['qtd_he_100%_dec'] = df_horas['qtd_he_100%'].apply(converter_hora_para_decimal)
        df_horas['id_registro_original'] = df_horas['nome'].astype(str) + '_' + df_horas['data'].dt.strftime('%Y-%m-%d')
        
        if not df_anotacoes.empty:
            df = pd.merge(df_horas, df_anotacoes, on='id_registro_original', how='left')
        else:
            df = df_horas.copy()
            df['texto_anotacao'] = ''
            df['nome_usuario'] = None
            
        df['texto_anotacao'] = df['texto_anotacao'].fillna('')
        df['nome_usuario'] = df['nome_usuario'].fillna('')
        df = df.loc[:, ~df.columns.duplicated()]

        # --- LÓGICA DO PERÍODO COMERCIAL ADICIONADA AQUI ---
        def determinar_periodo_comercial(data):
            if data.day > 20:
                data_periodo = data + pd.DateOffset(months=1)
                return data_periodo.year, data_periodo.month
            else:
                return data.year, data.month
        
        df[['ano_comercial', 'mes_comercial']] = df['data'].apply(
            lambda data: pd.Series(determinar_periodo_comercial(data))
        )

    # --- Interface Principal do Dashboard ---
    if 'data' in df.columns and not df['data'].isnull().all():
        ultima_atualizacao = pd.to_datetime(df['data']).max().strftime('%d/%m')
        st.sidebar.info(f"🗓️ Atualizado até **{ultima_atualizacao}**")

    # Botão para limpar cache / forçar sincronização
    if st.sidebar.button("🔄 Forçar Sincronização", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache limpo! Os dados serão recarregados do banco.")
        st.rerun()

    st.sidebar.header("🔍 Filtros Principais")
    
    meses_pt = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
    # Filtro de Ano: Usa a nova coluna 'ano_comercial'
    anos_disponiveis = sorted(df['ano_comercial'].unique(), reverse=True)
    ano_selecionado = st.sidebar.selectbox("**Ano**", anos_disponiveis, index=0)
    
    # Filtra o DataFrame pelo ano comercial selecionado
    df_ano = df[df['ano_comercial'] == ano_selecionado]
    
    # Filtro de Mês: Usa a nova coluna 'mes_comercial'
    meses_disponiveis_ano = sorted(df_ano['mes_comercial'].unique())
    meses_nomes_disponiveis = ['Todos'] + [meses_pt[m] for m in meses_disponiveis_ano]

    # Lógica para definir o mês padrão corretamente
    hoje = datetime.now()
    data_referencia = hoje + pd.DateOffset(months=1) if hoje.day > 20 else hoje
    indice_padrao = 0
    if ano_selecionado == data_referencia.year:
        mes_comercial_nome_atual = meses_pt.get(data_referencia.month)
        if mes_comercial_nome_atual in meses_nomes_disponiveis:
            indice_padrao = meses_nomes_disponiveis.index(mes_comercial_nome_atual)
        
    mes_selecionado_nome = st.sidebar.selectbox("**Mês**", meses_nomes_disponiveis, index=indice_padrao)
    
    # Lógica de filtragem final
    if mes_selecionado_nome == 'Todos':
        df_periodo = df_ano.copy()
        info_periodo = f"Exibindo dados de todo o ano de **{ano_selecionado}**."
    else:
        mes_num = next(k for k, v in meses_pt.items() if v == mes_selecionado_nome)
        # Filtra o DataFrame usando a coluna 'mes_comercial'
        df_periodo = df_ano[df_ano['mes_comercial'] == mes_num].copy()
        
        # Calcula as datas de início e fim apenas para o texto informativo
        data_fim_info = pd.to_datetime(f'{ano_selecionado}-{mes_num}-20')
        data_inicio_info = (data_fim_info - pd.DateOffset(months=1)).replace(day=21)
        info_periodo = f"Período de **{data_inicio_info.strftime('%d/%m/%Y')}** a **{data_fim_info.strftime('%d/%m/%Y')}**."

    mapa_filiais = {'Guarulhos': 'Guarulhos', 'Valinhos': 'Valinhos', 'Ribeirao': 'Ribeirão', 'Marilia': 'Marília', 'Jacareí': 'Jacareí'}
    filiais_disponiveis = sorted(df_periodo['filial'].unique().tolist())
    nomes_filiais_display = ['Todas'] + [mapa_filiais.get(f, f) for f in filiais_disponiveis]
    filial_selecionada_nome = st.sidebar.selectbox("**Filial**", nomes_filiais_display)

    df_filtrado = df_periodo.copy()
    if filial_selecionada_nome != 'Todas':
        cod_selecionado = [k for k, v in mapa_filiais.items() if v == filial_selecionada_nome][0]
        df_filtrado = df_periodo[df_periodo['filial'] == cod_selecionado].copy()

    st.info(info_periodo)
    
    if df_filtrado.empty:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
    else:
        # --- SEÇÃO DE KPIs ---
        total_he_geral = df_filtrado['valor_total'].sum()
        custo_total_com_encargos = total_he_geral * 1.16  # <-- NOVO CALCULO
        total_he_50 = df_filtrado['valor_he_50%'].sum()
        total_he_100 = df_filtrado['valor_he_100%'].sum()
        total_horas_50_dec = df_filtrado['qtd_he_50%_dec'].sum()
        total_horas_100_dec = df_filtrado['qtd_he_100%_dec'].sum()
        total_horas_dec = total_horas_50_dec + total_horas_100_dec
        total_colaboradores_he = df_filtrado[df_filtrado['valor_total'] > 0]['nome'].nunique()

        # --- Lógica para calcular as contratações pendentes ---
        total_contratacoes_pendentes = 0
        if not df_contratacoes.empty:
            if filial_selecionada_nome == 'Todas':
                # Soma o valor de todas as filiais
                total_contratacoes_pendentes = int(df_contratacoes['contratacoes_pendentes'].sum())
            else:
                # Busca o valor da filial específica
                df_contratacoes_filtrado = df_contratacoes[df_contratacoes['filial_descricao'] == filial_selecionada_nome]
                if not df_contratacoes_filtrado.empty:
                    total_contratacoes_pendentes = int(df_contratacoes_filtrado['contratacoes_pendentes'].iloc[0])


        # --- LÓGICA PARA CALCULAR KPIs DE COLABORADORES
        total_colaboradores_geral = 0
        total_colaboradores_ativos = 0
        total_colaboradores_inativos = 0
        lista_ativos_df, lista_inativos_df = pd.DataFrame(), pd.DataFrame()

        if not df_colaboradores.empty:
            df_colab_filtrado = df_colaboradores.copy()
            # Aplica o filtro de filial, se não for "Todas"
            if filial_selecionada_nome != 'Todas':
                df_colab_filtrado = df_colaboradores[df_colaboradores['filial'] == filial_selecionada_nome]
            
            # Realiza os cálculos sobre o DataFrame (filtrado ou não)
            if not df_colab_filtrado.empty:
                lista_ativos_df = df_colab_filtrado[df_colab_filtrado['situação'] == 'TRABALHANDO'][['colaborador', 'função', 'filial']]
                lista_inativos_df = df_colab_filtrado[df_colab_filtrado['situação'] != 'TRABALHANDO'][['colaborador', 'função', 'filial', 'situação']]
                total_colaboradores_ativos = df_colab_filtrado[df_colab_filtrado['situação'] == 'TRABALHANDO']['colaborador'].nunique()
                total_colaboradores_inativos = df_colab_filtrado[df_colab_filtrado['situação'] != 'TRABALHANDO']['colaborador'].nunique()
                total_colaboradores_geral = df_colab_filtrado['colaborador'].nunique()

        # --- KPI PRINCIPAL ---
        html_kpi_principal = f"""
        <div style="display: flex; justify-content: space-around; align-items: center; background-color: #004080; color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
            <div style="text-align: center; flex-grow: 1;">
                <p style="font-size: 1.2em; color: #ffffff; margin-bottom: 0;">CUSTO TOTAL COM HORAS EXTRAS</p>
                <p style="font-size: 3.0em; font-weight: bold; margin-bottom: 0;">{format_BRL(total_he_geral)}</p>
                <p style="font-size: 1.0em; color: #ffffff; margin-top: 2px;">Projeção c/ Encargos (16%): <b>{format_BRL(custo_total_com_encargos)}</b></p>
            </div>
            <div style="border-left: 2px solid #aab; height: 80px; margin: 0 20px;"></div>
            <div style="text-align: center; flex-grow: 1;">
                <p style="font-size: 1.2em; color: #ffffff; margin-bottom: 0;">QUANTIDADE TOTAL DE HORAS</p>
                <p style="font-size: 3.0em; font-weight: bold; margin-bottom: 0;">{format_horas_decimal(total_horas_dec)}</p>
            </div>
            <div style="border-left: 2px solid #aab; height: 80px; margin: 0 20px;"></div>
            <div style="text-align: center; flex-grow: 1;">
                <p style="font-size: 1.2em; color: #ffffff; margin-bottom: 0;">TOTAL COLABORADORES COM HE</p>
                <p style="font-size: 3.0em; font-weight: bold; margin-bottom: 0;">{(total_colaboradores_he)}</p>
            </div>

        </div>
        """
        st.markdown(html_kpi_principal, unsafe_allow_html=True)

        # Primeira Linha de KPIs
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        with kpi_col1: st.markdown(exibir_kpi_secundario("Custo HE 50%", format_BRL(total_he_50), icon="💰"), unsafe_allow_html=True)
        with kpi_col2: st.markdown(exibir_kpi_secundario("Total Horas 50%", format_horas_decimal(total_horas_50_dec), icon="⏰"), unsafe_allow_html=True)
        with kpi_col3: st.markdown(exibir_kpi_secundario("Custo HE 100%", format_BRL(total_he_100), icon="💰"), unsafe_allow_html=True)
        with kpi_col4: st.markdown(exibir_kpi_secundario("Total Horas 100%", format_horas_decimal(total_horas_100_dec), icon="⏰"), unsafe_allow_html=True)

        # Segunda Linha de KPIs
        st.write("") 
        kpi_col5, kpi_col6, kpi_col7, kpi_col8 = st.columns(4)
        with kpi_col5: st.markdown(exibir_kpi_secundario("Total de Colaboradores", f"{total_colaboradores_geral}", icon="👥"), unsafe_allow_html=True)
        with kpi_col6: st.markdown(exibir_kpi_secundario("Colaboradores Ativos", f"{total_colaboradores_ativos}", icon="🟢"), unsafe_allow_html=True)
        with kpi_col7: st.markdown(exibir_kpi_secundario("Colaboradores Inativos", f"{total_colaboradores_inativos}", icon="🔴"), unsafe_allow_html=True)
        with kpi_col8: st.markdown(exibir_kpi_secundario("Contratações Pendentes", f"{total_contratacoes_pendentes}", icon="📝"), unsafe_allow_html=True)
        st.markdown("---")

        # --- SEÇÃO PARA LISTAR E EXTRAIR COLABORADORES ---
        def formatar_df_para_exibicao(df_original):
            if df_original.empty: return pd.DataFrame()
            df_display = df_original.sort_values(by='colaborador').copy()
            for col in ['colaborador', 'função', 'filial', 'situação']:
                if col in df_display.columns:
                    df_display[col] = df_display[col].str.title()

            mapa_nomes = {'colaborador': 'Colaborador', 'função': 'Função', 'filial': 'Filial', 'situação': 'Situação'}
            return df_display.rename(columns=mapa_nomes)

        df_ativos_display = formatar_df_para_exibicao(df_colab_filtrado[df_colab_filtrado['situação'] == 'TRABALHANDO'])
        df_inativos_display = formatar_df_para_exibicao(df_colab_filtrado[df_colab_filtrado['situação'] != 'TRABALHANDO'])

        st.subheader("Relação de Colaboradores")
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            with st.expander(f"🟢 Listar Colaboradores Ativos ({total_colaboradores_ativos})"):
                if not df_ativos_display.empty:
                    st.dataframe(df_ativos_display[['Colaborador', 'Filial', 'Situação']], use_container_width=True, hide_index=True)
                    st.download_button(label="📥 Baixar Lista de Ativos (.csv)", data=converte_df_para_csv(df_ativos_display), file_name=f"colaboradores_ativos.csv", mime='text/csv')
                else: st.info("Nenhum colaborador ativo para os filtros.")
        
        with col_exp2:
            with st.expander(f"🔴 Listar Colaboradores Inativos ({total_colaboradores_inativos})"):
                if not df_inativos_display.empty:
                    st.dataframe(df_inativos_display[['Colaborador', 'Função', 'Filial', 'Situação']], use_container_width=True, hide_index=True)
                    st.download_button(label="📥 Baixar Lista de Inativos (.csv)", data=converte_df_para_csv(df_inativos_display), file_name=f"colaboradores_inativos.csv", mime='text/csv')
                else: st.info("Nenhum colaborador inativo para os filtros.")
        st.markdown("---")

        # --- SEÇÃO DE GRÁFICOS ---
        col_graf1, col_graf2 = st.columns(2)

        # GRÁFICO 1: Custo de HE por Cargo (Gráfico de Barras)
        with col_graf1:
            st.subheader("Custo de HE por Cargo")
            custo_por_cargo = df_filtrado.groupby('cargo')['valor_total'].sum().sort_values(ascending=False).reset_index()
            custo_por_cargo['valor_formatado'] = custo_por_cargo['valor_total'].apply(format_BRL)

            fig_bar = px.bar(
                custo_por_cargo,
                x='cargo',
                y='valor_total',
                title='Custo Total de Horas Extras por Cargo',
                labels={'cargo': 'Cargo', 'valor_total': 'Custo Total (R$)'},
                text_auto='.2s',
                color_discrete_sequence=px.colors.qualitative.Plotly,  # Paleta de cores profissional
                text='valor_formatado'
            )

            # --- Aprimoramentos do Layout e Tooltip ---
            fig_bar.update_layout(
                title_x=0.5,  # Centraliza o título
                xaxis_title=None,  # Remove o título do eixo x para um look mais limpo
                yaxis_title="Custo Total (R$)",
                legend_title_text='Cargo',
                plot_bgcolor='rgba(0,0,0,0)',  # Fundo transparente
                paper_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=40, r=40, t=40, b=40)
            )
            fig_bar.update_traces(
                textposition='outside',
                # Personaliza o que aparece ao passar o mouse
                hovertemplate='<b>Cargo:</b> %{x}<br><b>Custo Total:</b> %{text}<extra></extra>'
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # GRÁFICO 2: Evolução do Custo por Filial (Gráfico de Linha com Tooltip Consolidado)
        with col_graf2:
            st.subheader("Evolução do Custo por Filial")
            custo_diario_filial = df_periodo.groupby(['data', 'filial'])['valor_total'].sum().reset_index()
            custo_diario_filial['filial'] = custo_diario_filial['filial'].map(mapa_filiais).fillna(custo_diario_filial['filial'])

            # --- ETAPA 1: Pivotar os dados ---
            df_pivot = custo_diario_filial.pivot_table(
                index='data',
                columns='filial',
                values='valor_total',
                fill_value=0
            ).reset_index()

            # --- ETAPA 2: Criar o texto personalizado para o tooltip ---
            tooltip_texts = []
            for index, row in df_pivot.iterrows():
                hover_text = f"<b>Data: {row['data'].strftime('%d/%m/%Y')}</b><br>--------------------<br>"
                
                for filial in df_pivot.columns[1:]: # Pula a coluna 'data'
                    valor_formatado = format_BRL(row[filial])
                    hover_text += f"<b>{filial}:</b> {valor_formatado}<br>"
                
                tooltip_texts.append(hover_text)
            
            df_pivot['tooltip_text'] = tooltip_texts

            # --- ETAPA 3: Criar o gráfico ---
            fig_line = px.line(
                df_pivot,
                x='data',
                y=df_pivot.columns[1:-1],
                title='Evolução Diária do Custo de HE por Filial',
                labels={'data': 'Data', 'value': 'Custo Total (R$)', 'variable': 'Filial'},
                markers=True,
                color_discrete_sequence=px.colors.qualitative.Plotly
            )

            # --- ETAPA 4: Aprimorar Layout e Tooltip ---
            fig_line.update_layout(
                title_x=0.5,
                xaxis_title="Período",
                yaxis_title="Custo Total (R$)",
                legend_title_text='Filial',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
            )

            fig_line.update_traces(
                customdata=df_pivot['tooltip_text'],
                hovertemplate='%{customdata}<extra></extra>'
            )
            
            st.plotly_chart(fig_line, use_container_width=True)

        # --- SEÇÃO DE ANOTAÇÕES ---
        st.markdown("---")
        st.markdown("#### 📝 Tabela de Registros e Anotações")

        # Verifica se o dataframe filtrado (que respeita a filial) não está vazio
        df_com_valor = df_filtrado[df_filtrado['valor_total'] > 0]
        if not df_com_valor.empty:
            # Encontra a data do último registro para a filial/período selecionado
            ultimo_registro_data = df_com_valor['data'].max().strftime('%d/%m/%Y')
            
            # Define o nome da filial para exibição na mensagem
            nome_filial_display = filial_selecionada_nome if filial_selecionada_nome != 'Todas' else 'todas as filiais'
            
            # Exibe a informação usando st.caption para um texto mais sutil
            st.caption(f"ℹ️ Último registro para **{nome_filial_display}** no período selecionado: **{ultimo_registro_data}**")

        # Filtros para a seção de anotações
        col_filtro_data, col_filtro_check = st.columns([1.5, 3])
        with col_filtro_data:
            data_anotacao_filtro = st.date_input(
                "**Filtrar por data:**", 
                value=date.today(), 
                key="anotacao_filtro_data", 
                format="DD/MM/YYYY"
            )
        with col_filtro_data:
            marcar_todos = st.checkbox(
                "Exibir o período completo (ignora o filtro de data)", 
                key="anotacao_marcar_todos_check"
            )        

        # --- LÓGICA DE FILTRAGEM CONDICIONAL ---
        if marcar_todos:
            # Se marcado, usa o df_filtrado que já respeita os filtros principais (ano, mês, filial)
            df_para_anotar = df_filtrado[df_filtrado['valor_total'] > 0].copy()
            st.info("Exibindo todos os registros para o período e filial selecionados nos filtros principais.")
        else:
            # Se desmarcado, aplica o filtro de data específico
            df_para_anotar = df_filtrado[
                (df_filtrado['data'].dt.date == data_anotacao_filtro) & 
                (df_filtrado['valor_total'] > 0)
            ].copy()

        if df_para_anotar.empty:
            st.warning(f"Nenhum registro encontrado para a data **{data_anotacao_filtro.strftime('%d/%m/%Y')}** com os filtros principais selecionados.")
        else:
            # --- Início da Lógica de Download ---
            
            # 1. Prepara o DataFrame para download a partir dos dados já filtrados
            df_para_download = df_para_anotar[[
                'data', 'nome', 'cargo', 'qtd_he_50%', 'qtd_he_100%', 'valor_total', 'texto_anotacao', 'nome_usuario'
            ]].copy()
            
            # 2. Renomeia as colunas para o formato final do relatório
            df_para_download = df_para_download.rename(columns={
                'data': 'Data',
                'nome': 'Colaborador',
                'cargo': 'Cargo',
                'qtd_he_50%': 'HE 50%',
                'qtd_he_100%': 'HE 100%',
                'valor_total': 'Valor Total (R$)',
                'texto_anotacao': 'Anotação',
                'nome_usuario': 'Gestor Responsavel'
            })
            
            # 3. Garante a ordem exata das colunas
            df_para_download = df_para_download[[
                'Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor Total (R$)', 'Anotação', 'Gestor Responsavel'
            ]]

            # 4. Formata a coluna de Data
            df_para_download['Data'] = pd.to_datetime(df_para_download['Data']).dt.strftime('%d/%m/%Y')
            df_para_download['Valor Total (R$)'] = df_para_download['Valor Total (R$)'].apply(format_BRL)

            # 5. Exibe o botão de download
            st.download_button(
                label="📥 Baixar Relatório Filtrado (.csv)",
                data=converte_df_para_csv(df_para_download),
                file_name=f"relatorio_anotacoes_{data_anotacao_filtro.strftime('%d/%m/%Y')}.csv",
                mime='text/csv',
                help=f"Baixa os registros do dia {data_anotacao_filtro.strftime('%d/%m/%Y')} em formato CSV"
            )
            # --- Fim da Lógica de Download ---

            # Lógica para exibir e editar a tabela
            df_editor_pronto = df_para_anotar.copy()
            df_editor_pronto.rename(columns={
                'texto_anotacao': 'Anotação',
                'nome': 'Colaborador',
                'data': 'Data',
                'qtd_he_50%': 'HE 50%',
                'qtd_he_100%': 'HE 100%',
                'cargo': 'Cargo',
                'valor_total': 'Valor Total (R$)'
            }, inplace=True)
            df_editor_pronto['Data'] = pd.to_datetime(df_editor_pronto['Data']).dt.strftime('%d/%m/%Y')
            df_editor_pronto['Valor Total (R$)'] = df_editor_pronto['Valor Total (R$)'].apply(format_BRL)
            colunas_para_exibir = ['Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor Total (R$)', 'Anotação']

            if 'df_anotacao_original' not in st.session_state or st.session_state.df_anotacao_original.empty or pd.to_datetime(st.session_state.df_anotacao_original['data'].iloc[0]).date() != data_anotacao_filtro:
                st.session_state.df_anotacao_original = df_para_anotar.copy()

            df_editado = st.data_editor(
                df_editor_pronto[colunas_para_exibir],
                use_container_width=True, hide_index=True,
                disabled=[col for col in colunas_para_exibir if col != 'Anotação'],
                key="data_editor_anotacoes"
            )

            if st.button("✔️ Salvar Anotações Editadas", use_container_width=True, type="primary"):
                try:
                    df_original_dia = st.session_state.df_anotacao_original[['id_registro_original', 'nome', 'texto_anotacao']].copy()
                    df_original_dia.rename(columns={'nome': 'Colaborador'}, inplace=True)
                    df_editado_usuario = df_editado.copy()

                    df_comparacao = pd.merge(df_editado_usuario, df_original_dia, on='Colaborador', how='left')
                    alteracoes = df_comparacao[df_comparacao['Anotação'] != df_comparacao['texto_anotacao']].copy()

                    registros_para_deletar = alteracoes[alteracoes['Anotação'].str.strip() == '']
                    registros_para_upsert = alteracoes[alteracoes['Anotação'].str.strip() != '']

                    if not registros_para_deletar.empty or not registros_para_upsert.empty:
                        with st.spinner("Salvando alterações..."), engine.begin() as conn:
                            # 1. Deleta os registros que ficaram em branco
                            if not registros_para_deletar.empty:
                                for _, linha in registros_para_deletar.iterrows():
                                    query_delete = text("DELETE FROM anotacoes WHERE id_registro_original = :id")
                                    conn.execute(query_delete, {"id": linha['id_registro_original']})

                            # 2. Insere ou atualiza os registros com conteúdo
                            if not registros_para_upsert.empty:
                                for _, linha in registros_para_upsert.iterrows():
                                    query_upsert = text("""
                                        INSERT INTO anotacoes (id_registro_original, nome_usuario, texto_anotacao) VALUES (:id, :usuario, :texto)
                                        ON CONFLICT (id_registro_original) DO UPDATE SET texto_anotacao = EXCLUDED.texto_anotacao, nome_usuario = EXCLUDED.nome_usuario, data_modificacao = NOW();
                                    """)
                                    conn.execute(query_upsert, {
                                        "id": linha['id_registro_original'],
                                        "usuario": usuario_logado.get('nome', 'Usuário do Sistema'),
                                        "texto": linha['Anotação']
                                    })

                        # Mensagem de sucesso
                        msg_sucesso = []
                        if not registros_para_upsert.empty:
                            msg_sucesso.append(f"{len(registros_para_upsert)} anotações salvas/atualizadas")
                        if not registros_para_deletar.empty:
                            msg_sucesso.append(f"{len(registros_para_deletar)} anotações removidas")
                        
                        st.success(" e ".join(msg_sucesso) + "!")
                        
                        st.cache_data.clear()
                        del st.session_state.df_anotacao_original
                        st.rerun()

                    elif not alteracoes.empty and not engine:
                        st.error("Não foi possível salvar. A conexão com o banco de dados falhou.")
                    else:
                        st.info("Nenhuma alteração nas anotações foi detectada.")
                except Exception as e:
                    st.error(f"Ocorreu um erro ao salvar as alterações: {e}")

        ### PAINEL DE DIAGNÓSTICOS
        df_nao_classificados = df_filtrado[df_filtrado['cargo'] == 'Não Classificado'].copy()
        if not df_nao_classificados.empty:

            st.markdown("---")
            st.markdown('#### 🚨 Painel de Diagnóstico: Colaborador Não Identificado na Aba "OPERAÇÃO"')
            # Agrupa por nome e filial para maior detalhe
            resumo_problemas = df_nao_classificados.groupby(['nome', 'filial']).agg(
                Custo_Total=('valor_total', 'sum'), 
                Ocorrencias=('nome', 'count')
            ).reset_index().sort_values(by='Custo_Total', ascending=False)
            
            resumo_problemas['Custo_Total'] = resumo_problemas['Custo_Total'].apply(
                lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            )

            resumo_problemas.rename(columns={
                'nome': 'Colaborador',
                'filial': 'Filial',
                'Custo_Total': 'Custo Total Não Classificado',
                'Ocorrencias': 'Nº de Lançamentos'
            }, inplace=True)
            
            st.write("Resumo dos Nomes com Problemas de Correspondência:")
            st.dataframe(resumo_problemas, use_container_width=True, hide_index=True)

            # BOTÃO PARA DOWNLOAD .CSV
            csv_data = converte_df_para_csv(resumo_problemas)
            st.download_button(
                label="📥 Baixar dados como CSV",
                data=csv_data,
                file_name=f"relatorio_nomes_nao_classificados_{date.today().strftime('%d-%m-%Y')}.csv",
                mime='text/csv',
            )

# ==============================================================================
# 4. CONTROLE DE FLUXO PRINCIPAL
# ==============================================================================
if not get_logged_user():   
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        # Adiciona um espaço no topo para um melhor alinhamento vertical
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        
        # Container para criar um efeito de "card" com borda
        with st.container(border=True):
            st.title("🔐 Autenticação de Usuário")
            st.markdown("Por favor, insira suas credenciais para acessar o dashboard.")
            
            # Formulário de login
            with st.form("login_form_central"):
                email = st.text_input("📧 **Email**", key="login_email")
                senha = st.text_input("🔑 **Senha**", type="password", key="login_senha")
                
                st.markdown("<br>", unsafe_allow_html=True) # Espaçador
                
                # Botão de submit centralizado e destacado
                submitted = st.form_submit_button(
                    "Entrar", 
                    use_container_width=True, 
                    type="primary"
                )

                if submitted:
                    user_info = authenticate_user(email, senha)
                    if user_info:
                        st.session_state['user'] = user_info
                        st.rerun()
                    else:
                        st.error("Email ou senha inválidos. Por favor, tente novamente.")

        # Rodapé simples
        st.markdown(
            """
            <div style="text-align: center; margin-top: 20px; color: grey;">
                <p>NT Transportes - Dashboard Recursos Humano © 2025</p>
            </div>
            """,
            unsafe_allow_html=True
        )
else:
    run_dashboard()
