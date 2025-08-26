# app.py (Versão Otimizada)

import bcrypt
import locale
import gspread
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, date
from sqlalchemy import create_engine, text
from streamlit_plotly_events import plotly_events

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

def reset_app_state(engine):
    """
    Função otimizada que recarrega APENAS as anotações do banco,
    refaz o merge com os dados principais em memória e atualiza a tela.
    """
    # 1. Limpa o cache APENAS da função que busca no banco.
    st.cache_data.clear() # Limpa o cache de todas as funções, incluindo a do banco.
    
    # 2. Pega o DataFrame principal que já está carregado na sessão
    if 'df_principal' in st.session_state:
        df_principal_antigo = st.session_state['df_principal']
        
        # 3. Re-busca os dados do banco (a função será executada pois o cache foi limpo)
        df_anotacoes_novo, df_contratacoes_novo = carregar_dados_banco(engine)

        # 4. Refaz o merge com as anotações atualizadas
        # Remove colunas de anotações antigas para evitar conflitos no merge
        colunas_anotacao = ['texto_anotacao', 'nome_usuario', 'categoria', 'justificativa']
        df_sem_anotacoes = df_principal_antigo.drop(columns=[col for col in colunas_anotacao if col in df_principal_antigo.columns])
        
        df_principal_atualizado = pd.merge(df_sem_anotacoes, df_anotacoes_novo, on='id_registro_original', how='left')
        
        # Garante que colunas de anotação não tenham valores nulos (NaN)
        for col in colunas_anotacao:
            if col in df_principal_atualizado.columns:
                df_principal_atualizado[col] = df_principal_atualizado[col].fillna('')

        # 5. Atualiza o DataFrame principal e de contratações na sessão
        st.session_state['df_principal'] = df_principal_atualizado
        st.session_state['df_contratacoes'] = df_contratacoes_novo
        
        # 6. Limpa o estado específico da tabela de anotações para evitar inconsistências
        if 'df_anotacao_original_indexed' in st.session_state:
            del st.session_state['df_anotacao_original_indexed']

    # 7. Recarrega a página
    st.rerun()

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
            query_anotacoes = text("SELECT id_registro_original, texto_anotacao, nome_usuario, categoria, justificativa FROM anotacoes")
            df_anotacoes = pd.DataFrame(conn.execute(query_anotacoes).fetchall(), columns=['id_registro_original', 'texto_anotacao', 'nome_usuario', 'categoria', 'justificativa'])

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
# NOVA FUNÇÃO PARA CENTRALIZAR O CARREGAMENTO E PROCESSAMENTO PESADO
# ==============================================================================
@st.cache_data(ttl=300, show_spinner="Sincronizando e processando todos os dados...")
def carregar_e_processar_dados_iniciais(_gs_client, _engine, nome_planilha):
    """
    Centraliza o carregamento de todas as fontes de dados e o processamento pesado
    que só precisa ser feito uma vez.
    """
    # 1. Carregar dados das fontes
    df_horas = carregar_horas_e_operacao(_gs_client, nome_planilha)
    df_colaboradores = carregar_colaboradores(_gs_client, nome_planilha)
    df_anotacoes, df_contratacoes = carregar_dados_banco(_engine)

    # Ponto de parada se dados essenciais não forem carregados
    if df_horas.empty:
        return None, None, None, None

    # 2. Processamento pesado (merges, apply, etc.)
    df_horas['qtd_he_50%_dec'] = df_horas['qtd_he_50%'].apply(converter_hora_para_decimal)
    df_horas['qtd_he_100%_dec'] = df_horas['qtd_he_100%'].apply(converter_hora_para_decimal)
    df_horas['id_registro_original'] = df_horas['nome'].astype(str) + '_' + df_horas['data'].dt.strftime('%Y-%m-%d')
    
    if not df_anotacoes.empty:
        df = pd.merge(df_horas, df_anotacoes, on='id_registro_original', how='left')
    else:
        df = df_horas.copy()
        df['texto_anotacao'] = ''
        df['nome_usuario'] = None
        df['categoria'] = ''
        df['justificativa'] = ''

    df['texto_anotacao'] = df['texto_anotacao'].fillna('')
    df['nome_usuario'] = df['nome_usuario'].fillna('')
    df['categoria'] = df['categoria'].fillna('')
    df['justificativa'] = df['justificativa'].fillna('')
    df = df.loc[:, ~df.columns.duplicated()]

    def determinar_periodo_comercial(data):
        if data.day > 20:
            data_periodo = data + pd.DateOffset(months=1)
            return data_periodo.year, data_periodo.month
        else:
            return data.year, data.month
    
    df[['ano_comercial', 'mes_comercial']] = df['data'].apply(
        lambda data: pd.Series(determinar_periodo_comercial(data))
    )
    
    return df, df_colaboradores, df_contratacoes

# ==============================================================================
# LÓGICA DO DASHBOARD
# ==============================================================================
def run_dashboard():
    st.title("👥 Dashboard de Recursos Humanos")

    # --- NOVO BLOCO DE CSS PARA ESTILIZAR O EXPANDER (pode manter o seu) ---
    st.markdown("""
    <style>
        div[data-testid="stExpander"] summary {
            background-color: #f0f2f6; border: 1px solid #e6eaf1; border-radius: 10px;
            padding: 12px; font-size: 1.05em; font-weight: 600; color: #004080;
        }
        div[data-testid="stExpander"] summary:hover { background-color: #e6eaf1; }
    </style>
    """, unsafe_allow_html=True)

    usuario_logado = get_logged_user()
    NOME_DA_PLANILHA = "bdBANCO DE HORAS"

    # --- LÓGICA DE CARREGAMENTO ÚNICO COM st.session_state ---
    if 'data_loaded' not in st.session_state:
        gs_client = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        df, df_colaboradores, df_contratacoes = carregar_e_processar_dados_iniciais(gs_client, engine, NOME_DA_PLANILHA)
        
        if df is None:
            st.warning("Não há dados de horas extras válidos para exibir. O dashboard não pode continuar.")
            st.stop()
            
        # Armazena os dados processados na sessão
        st.session_state['df_principal'] = df
        st.session_state['df_colaboradores'] = df_colaboradores
        st.session_state['df_contratacoes'] = df_contratacoes
        st.session_state['data_loaded'] = True
        st.toast("Dados carregados e processados!", icon="✅")

    # A partir daqui, sempre usamos os dados da sessão
    df = st.session_state['df_principal']
    df_colaboradores = st.session_state['df_colaboradores']
    df_contratacoes = st.session_state['df_contratacoes']

    # --- Interface Principal do Dashboard ---
    if 'data' in df.columns and not df['data'].isnull().all():
        ultima_atualizacao = pd.to_datetime(df['data']).max().strftime('%d/%m')
        st.sidebar.info(f"🗓️ Atualizado até **{ultima_atualizacao}**")

    # Botão para limpar cache / forçar sincronização
    if st.sidebar.button("🔄 Forçar Sincronização", use_container_width=True):
        # Limpa o cache de dados e o estado da sessão para forçar o recarregamento
        st.cache_data.clear()
        keys_to_delete = ['data_loaded', 'df_principal', 'df_colaboradores', 'df_contratacoes']
        for key in keys_to_delete:
            if key in st.session_state:
                del st.session_state[key]
        st.success("Sincronização forçada! Os dados serão recarregados na próxima ação.")
        st.rerun()

    # RESTANTE DO SEU CÓDIGO DO DASHBOARD PERMANECE IGUAL
    # ... (cole todo o resto da sua função run_dashboard() aqui, começando pelos filtros) ...
    # Exemplo:
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
        with col_graf1:
            if 'selected_cargo' not in st.session_state:
                st.session_state.selected_cargo = None

            # Título principal é exibido em ambas as visões
            st.subheader("Custo de HE por Cargo")

            # SE UM CARGO FOI SELECIONADO (VISÃO DE DETALHE)
            if st.session_state.selected_cargo:
                st.markdown(f"#### Detalhes para: **{st.session_state.selected_cargo}**")
                
                df_detalhes = df_filtrado[(df_filtrado['cargo'] == st.session_state.selected_cargo) & (df_filtrado['valor_total'] > 0)].copy()
                df_detalhes['data'] = pd.to_datetime(df_detalhes['data']).dt.strftime('%d/%m/%Y')
                df_detalhes['valor_total_fmt'] = df_detalhes['valor_total'].apply(format_BRL)
                df_detalhes_display = df_detalhes[['data', 'nome', 'filial', 'valor_total_fmt']].rename(columns={
                    'data': 'Data', 'nome': 'Colaborador', 'filial': 'Filial', 'valor_total_fmt': 'Valor Total'
                }).sort_values(by='Data')
                
                st.dataframe(df_detalhes_display, use_container_width=True, hide_index=True)
                
                if st.button("⬅️ Voltar para a visão geral"):
                    st.session_state.selected_cargo = None
                    st.rerun()

            # SE NENHUM CARGO FOI SELECIONADO (VISÃO PRINCIPAL)
# SE NENHUM CARGO FOI SELECIONADO (VISÃO PRINCIPAL)
            else:
                custo_por_cargo = df_filtrado.groupby('cargo')['valor_total'].sum().sort_values(ascending=False).reset_index()
                custo_por_cargo['valor_formatado'] = custo_por_cargo['valor_total'].apply(format_BRL)
                
                # --- ABORDAGEM FINAL COM PLOTLY.GRAPH_OBJECTS (go) ---
                import plotly.graph_objects as go
                fig_bar = go.Figure()
                for index, row in custo_por_cargo.iterrows():
                    fig_bar.add_trace(go.Bar(
                        x=[row['cargo']],
                        y=[row['valor_total']],
                        name=row['cargo'],
                        customdata=[[row['cargo'], row['valor_formatado']]],
                        hovertemplate='<b>Cargo:</b> %{customdata[0]}<br><b>Custo Total:</b> %{customdata[1]}<extra></extra>'
                    ))

                # Aplica o layout e a formatação de texto na barra
                fig_bar.update_layout(
                    barmode='group',
                    hovermode='closest',
                    showlegend=False,
                    xaxis_title=None,
                    yaxis_title="Custo Total (R$)",
                    hoverlabel=dict(bgcolor="white", font_size=14, font_family="Arial, sans-serif", font_color="black"),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=40, r=40, t=20, b=40)
                )

                fig_bar.update_traces(
                    texttemplate='%{y:.2s}',
                    textposition='outside'
                )

                st.plotly_chart(fig_bar, use_container_width=True)

                st.write("") 
                lista_cargos = custo_por_cargo['cargo'].tolist()
                opcoes_menu = ["-- Selecione um cargo para ver detalhes --"] + lista_cargos
                cargo_selecionado_menu = st.selectbox(
                    "**Análise Detalhada por Cargo:**",
                    options=opcoes_menu,
                    key="selectbox_cargo_drilldown" # Adicionada uma chave para evitar conflitos
                )

                if cargo_selecionado_menu != opcoes_menu[0]:
                    st.session_state.selected_cargo = cargo_selecionado_menu
                    st.rerun()
                
        # GRÁFICO 2: Evolução do Custo por Filial (Gráfico de Linha com Tooltip Consolidado)
        with col_graf2:
            st.subheader("Evolução do Custo por Filial")
            if filial_selecionada_nome == 'Todas':
                custo_diario_filial = df_periodo.groupby(['data', 'filial'])['valor_total'].sum().reset_index()
                custo_diario_filial['filial'] = custo_diario_filial['filial'].map(mapa_filiais).fillna(custo_diario_filial['filial'])

                fig_line = px.line(
                    custo_diario_filial,
                    x='data',
                    y='valor_total',
                    color='filial',
                    title=None,
                    labels={'data': 'Período', 'valor_total': 'Custo Total (R$)', 'filial': 'Filial'},
                    markers=True,
                    hover_data={'filial': True}
                )
                
                fig_line.update_layout(
                    hovermode='x unified',
                    xaxis_title=None,
                    yaxis_title="Custo Total (R$)",
                    legend_title_text='Filial',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                
                fig_line.update_traces(
                    hovertemplate='<b>%{customdata[0]}:</b> R$ %{y:,.2f}<extra></extra>'
                )

            else:
                custo_diario_filial = df_filtrado.groupby('data')['valor_total'].sum().reset_index()

                fig_line = px.line(
                    custo_diario_filial,
                    x='data',
                    y='valor_total',
                    title=f"Evolução Diária do Custo - {filial_selecionada_nome}",
                    labels={'data': 'Período', 'valor_total': 'Custo Total (R$)'},
                    markers=True
                )
                
                fig_line.update_layout(
                    title_x=0.5,
                    xaxis_title=None,
                    yaxis_title="Custo Total (R$)",
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    hovermode='x unified'
                )

                fig_line.update_traces(
                    hovertemplate=f'<b>{filial_selecionada_nome}:</b> R$ %{{y:,.2f}}<extra></extra>'
                )
            st.plotly_chart(fig_line, use_container_width=True)

        # --- SEÇÃO DE ANOTAÇÕES ---
        st.markdown("---")
        st.markdown("#### 📝 Tabela de Registros e Anotações")

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

        if marcar_todos:
            df_para_anotar = df_filtrado[df_filtrado['valor_total'] > 0].copy()
        else:
            df_para_anotar = df_filtrado[
                (df_filtrado['data'].dt.date == data_anotacao_filtro) & 
                (df_filtrado['valor_total'] > 0)
            ].copy()

        if df_para_anotar.empty:
            st.warning(f"Nenhum registro encontrado para a data **{data_anotacao_filtro.strftime('%d/%m/%Y')}** com os filtros principais selecionados.")
        else:
            df_para_download = df_para_anotar[[
                'data', 'nome', 'cargo', 'qtd_he_50%', 'qtd_he_100%', 'valor_total', 'categoria', 'justificativa', 'nome_usuario'
            ]].copy()
            df_para_download = df_para_download.rename(columns={
                'data': 'Data', 'nome': 'Colaborador', 'cargo': 'Cargo', 'qtd_he_50%': 'HE 50%',
                'qtd_he_100%': 'HE 100%', 'valor_total': 'Valor Total (R$)', 'categoria': 'Categoria',
                'justificativa': 'Justificativa', 'nome_usuario': 'Gestor Responsavel'
            })
            df_para_download = df_para_download[['Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor Total (R$)', 'Categoria', 'Justificativa', 'Gestor Responsavel']]
            df_para_download['Data'] = pd.to_datetime(df_para_download['Data']).dt.strftime('%d/%m/%Y')
            df_para_download['Valor Total (R$)'] = df_para_download['Valor Total (R$)'].apply(format_BRL)

            st.download_button(
                label="📥 Baixar Relatório Filtrado (.csv)", data=converte_df_para_csv(df_para_download),
                file_name=f"relatorio_anotacoes_{data_anotacao_filtro.strftime('%d/%m/%Y')}.csv", mime='text/csv'
            )

            # --- INÍCIO DA LÓGICA CORRIGIDA ---
            df_editor_pronto = df_para_anotar.copy()
            
            # 1. Renomeia colunas para exibição amigável
            df_editor_pronto.rename(columns={
                'categoria': 'Categoria', 'justificativa': 'Justificativa', 'nome': 'Colaborador',
                'data': 'Data', 'qtd_he_50%': 'HE 50%', 'qtd_he_100%': 'HE 100%',
                'cargo': 'Cargo', 'valor_total': 'Valor Total (R$)'
            }, inplace=True)
            
            # 2. Formata as colunas e preenche nulos
            df_editor_pronto['Data'] = pd.to_datetime(df_editor_pronto['Data']).dt.strftime('%d/%m/%Y')
            df_editor_pronto['Valor Total (R$)'] = df_editor_pronto['Valor Total (R$)'].apply(format_BRL)
            df_editor_pronto['Categoria'] = df_editor_pronto['Categoria'].fillna('')
            df_editor_pronto['Justificativa'] = df_editor_pronto['Justificativa'].fillna('')

            # 3. **PONTO CRÍTICO**: Define o ID único como índice do DataFrame
            df_editor_pronto.set_index('id_registro_original', inplace=True)
            
            # 4. Guarda uma cópia do estado original, já com o índice correto, para comparação
            st.session_state['df_anotacao_original_indexed'] = df_editor_pronto.copy()

            colunas_para_exibir = ['Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor Total (R$)', 'Categoria', 'Justificativa']
            opcoes_categoria = ["", "Absenteísmo", "Quadro de colaboradores", "Cliente", "Operações", "Outros"]

            df_editado = st.data_editor(
                df_editor_pronto[colunas_para_exibir],
                use_container_width=True, hide_index=True,
                column_config={
                    "Categoria": st.column_config.SelectboxColumn("Motivo", options=opcoes_categoria),
                    "Justificativa": st.column_config.TextColumn("Justificativa (Obrigatória)")
                },
                disabled=['Data', 'Colaborador', 'Cargo', 'HE 50%', 'HE 100%', 'Valor Total (R$)'],
                key="data_editor_anotacoes"
            )

            if st.button("✔️ Salvar Anotações Editadas", use_container_width=True, type="primary"):
                try:
                    df_original_indexed = st.session_state['df_anotacao_original_indexed']
                    
                    # 5. Restaura o índice original no DataFrame editado
                    df_editado.index = df_original_indexed.index

                    # 6. Compara os DataFrames (agora alinhados pelo índice único) para encontrar as alterações
                    alteracoes = df_editado[
                        (df_editado['Categoria'].fillna('') != df_original_indexed['Categoria'].fillna('')) |
                        (df_editado['Justificativa'].fillna('') != df_original_indexed['Justificativa'].fillna(''))
                    ]

                    # Validação: Justificativa é obrigatória para qualquer categoria escolhida
                    erro_geral = alteracoes[(alteracoes['Categoria'].fillna('').str.strip() != '') & 
                                            (alteracoes['Justificativa'].fillna('').str.strip() == '')]
                    if not erro_geral.empty:
                        st.error("❌ Erro: Toda anotação deve ter justificativa preenchida.")
                    
                    elif not alteracoes.empty:
                        with st.spinner("Salvando alterações..."), engine.begin() as conn:
                            # 7. Itera sobre as linhas alteradas. O índice (id_registro) agora é o ID correto!
                            for id_registro, linha in alteracoes.iterrows():
                                categoria_val = (linha['Categoria'] or "").strip()
                                justificativa_val = (linha['Justificativa'] or "").strip()

                                if not categoria_val and not justificativa_val:
                                    query = text("DELETE FROM anotacoes WHERE id_registro_original = :id")
                                else:
                                    query = text("""
                                        INSERT INTO anotacoes (id_registro_original, nome_usuario, categoria, justificativa) 
                                        VALUES (:id, :usuario, :categoria, :justificativa)
                                        ON CONFLICT (id_registro_original) DO UPDATE SET 
                                            categoria = EXCLUDED.categoria, justificativa = EXCLUDED.justificativa, 
                                            nome_usuario = EXCLUDED.nome_usuario, data_modificacao = NOW();
                                    """)
                                
                                conn.execute(query, {
                                    "id": id_registro,
                                    "usuario": usuario_logado.get('nome', 'Usuário do Sistema'),
                                    "categoria": categoria_val,
                                    "justificativa": justificativa_val
                                })
                       
                        st.success(f"{len(alteracoes)} alterações foram salvas com sucesso!")
                        reset_app_state(engine)
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

