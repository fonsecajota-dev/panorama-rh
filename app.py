# app.py

from sqlalchemy import create_engine, text
from datetime import datetime, date
import plotly.express as px
import streamlit as st
import pandas as pd
import gspread
import bcrypt
import locale

# ==============================================================================
# 1. CONFIGURAÇÃO DA PÁGINA E AUTENTICAÇÃO
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
# CONFIGURAÇÃO DE FUNÇÕES AUXILIARES
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

@st.cache_data(ttl=300, show_spinner=False)
def carregar_e_preparar_dados(_gs_client, nome_planilha, _engine):
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

        if not lista_dfs_horas:
            st.warning("Nenhuma aba de filial com dados foi encontrada.")
            return pd.DataFrame()
        
        df_horas = pd.concat(lista_dfs_horas, ignore_index=True)
        aba_operacao = planilha.worksheet('OPERACAO')
        df_operacao = pd.DataFrame(aba_operacao.get_all_records(head=1))
        
        df_horas.columns = [str(col).strip().lower() for col in df_horas.columns]
        df_operacao.columns = [str(col).strip().lower() for col in df_operacao.columns]
        
        mapeamento_nomes = {
            'colaborador': 'nome', 'função': 'funcao', 'salario base': 'salario_base',
            'qtd he 50%': 'qtd_he_50%', 'qtd he 100%': 'qtd_he_100%',
            'valor he 50%': 'valor_he_50%', 'valor he 100%': 'valor_he_100%', 'valor total': 'valor_total'
        }
        df_horas.rename(columns=mapeamento_nomes, inplace=True)
        df_operacao.rename(columns={'função': 'funcao'}, inplace=True)
        
        df_horas['nome'] = df_horas['nome'].astype(str).str.strip().str.upper()
        df_operacao['nome'] = df_operacao['nome'].astype(str).str.strip().str.upper()
        df_horas['qtd_he_50%_dec'] = df_horas['qtd_he_50%'].apply(converter_hora_para_decimal)
        df_horas['qtd_he_100%_dec'] = df_horas['qtd_he_100%'].apply(converter_hora_para_decimal)
        
        colunas_valor = ['valor_he_50%', 'valor_he_100%', 'valor_total']
        for col in colunas_valor:
            if col in df_horas.columns:
                df_horas[col] = df_horas[col].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
                df_horas[col] = pd.to_numeric(df_horas[col], errors='coerce').fillna(0)
        
        df_horas['data'] = pd.to_datetime(df_horas['data'], errors='coerce', dayfirst=True)
        df_horas.dropna(subset=['data', 'nome'], inplace=True)
        
        df_completo = pd.merge(df_horas, df_operacao[['nome', 'cargo']], on='nome', how='left')

        df_completo['cargo'] = df_completo['cargo'].fillna('Não Classificado')
        df_completo['id_registro_original'] = df_completo['nome'].astype(str) + '_' + df_completo['data'].dt.strftime('%Y-%m-%d')
        
        if _engine:
            try:
                from sqlalchemy import text
                query_anotacoes = text("SELECT id_registro_original, texto_anotacao, nome_usuario FROM anotacoes")
                
                with _engine.connect() as conn:
                    result_anotacoes = conn.execute(query_anotacoes)
                    df_anotacoes = pd.DataFrame(result_anotacoes.fetchall(), columns=result_anotacoes.keys())

                if not df_anotacoes.empty:
                    df_completo = pd.merge(df_completo, df_anotacoes, on='id_registro_original', how='left')
                else:
                    df_completo['texto_anotacao'] = ''
                    df_completo['nome_usuario'] = None 
            except Exception as e:
                st.error(f"Erro detalhado ao buscar anotações: {e}")
                df_completo['texto_anotacao'] = ''
                df_completo['nome_usuario'] = None
        else:
            df_completo['texto_anotacao'] = ''
            df_completo['nome_usuario'] = None

        df_completo['texto_anotacao'] = df_completo['texto_anotacao'].fillna('')
        df_completo['nome_usuario'] = df_completo['nome_usuario'].fillna('')
        
        df_completo = df_completo.loc[:, ~df_completo.columns.duplicated()]
        return df_completo
    except Exception as e:
        st.error(f"Ocorreu um erro crítico ao processar os dados da planilha: {e}")
        return pd.DataFrame()
    
def format_BRL(valor):
    """Formata um valor numérico para o padrão de moeda brasileiro (R$)."""
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        return locale.currency(valor, grouping=True, symbol=True)
    except (ValueError, TypeError, locale.Error):
        if isinstance(valor, (int, float)):
            return f"R$ {valor:_.2f}".replace('.', ',').replace('_', '.')
        return "R$ 0,00"
    
# ==============================================================================
# 2. LÓGICA DO DASHBOARD
# ==============================================================================
def run_dashboard():
    st.title("👥 Dashboard de Recursos Humanos")

    usuario_logado = get_logged_user()
    NOME_DA_PLANILHA = "bdBANCO DE HORAS"

    @st.cache_resource(ttl=300)
    def autenticar_google_sheets():
        try:
            return gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        except Exception as e:
            st.error(f"Falha na autenticação com o Google Sheets: {e}")
            return None

    # --- Interface Principal do Dashboard ---
    gs_client = autenticar_google_sheets()
    if not gs_client:
        st.stop()

    with st.spinner("Carregando e processando dados..."):
        df = carregar_e_preparar_dados(gs_client, NOME_DA_PLANILHA, engine)

    if df.empty:
        st.warning("Não há dados válidos para exibir.")
        st.stop()
    
    # --- FILTROS ---
    # Captura e exibe a data da última atualização na sidebar
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
    anos_disponiveis = sorted(df['data'].dt.year.unique(), reverse=True)
    ano_selecionado = st.sidebar.selectbox("**Ano**", anos_disponiveis, index=0)
    
    df_ano = df[df['data'].dt.year == ano_selecionado]
    meses_disponiveis_ano = sorted(df_ano['data'].dt.month.unique())
    meses_nomes_disponiveis = ['Todos'] + [meses_pt[m] for m in meses_disponiveis_ano]

    # Define o índice padrão para o filtro de mês
    mes_atual_num = datetime.now().month
    mes_atual_nome = meses_pt.get(mes_atual_num)
    indice_padrao = 0 # Padrão é 'Todos'
    if mes_atual_nome in meses_nomes_disponiveis:
        indice_padrao = meses_nomes_disponiveis.index(mes_atual_nome)
        
    mes_selecionado_nome = st.sidebar.selectbox("**Mês**", meses_nomes_disponiveis, index=indice_padrao)
    if mes_selecionado_nome == 'Todos':
        df_periodo = df_ano.copy()
        info_periodo = f"Exibindo dados de todo o ano de **{ano_selecionado}**."
    else:
        mes_num = [k for k, v in meses_pt.items() if v == mes_selecionado_nome][0]
        data_fim = pd.to_datetime(f'{ano_selecionado}-{mes_num}-20')
        data_inicio = (data_fim - pd.DateOffset(months=1)).replace(day=21)
        df_periodo = df[(df['data'] >= data_inicio) & (df['data'] <= data_fim)].copy()
        info_periodo = f"Período de **{data_inicio.strftime('%d/%m/%Y')}** a **{data_fim.strftime('%d/%m/%Y')}**."

    mapa_filiais = {'Guarulhos': 'Guarulhos', 'Valinhos': 'Valinhos', 'Ribeirao': 'Ribeirão Preto', 'Marilia': 'Marília', 'Jacareí': 'Jacareí'}
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
        total_he_50 = df_filtrado['valor_he_50%'].sum()
        total_he_100 = df_filtrado['valor_he_100%'].sum()
        total_horas_50_dec = df_filtrado['qtd_he_50%_dec'].sum()
        total_horas_100_dec = df_filtrado['qtd_he_100%_dec'].sum()
        total_colaboradores = df_filtrado[df_filtrado['valor_total'] > 0]['nome'].nunique()

        st.markdown(f"""
        <div style="text-align: center; background-color: #004080; color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
            <p style="font-size: 1.2em; color: #d0d0d0; margin-bottom: 0;">CUSTO TOTAL COM HORAS EXTRAS</p>
            <p style="font-size: 3.0em; font-weight: bold; margin-bottom: 0;">{'R$ {:,.2f}'.format(total_he_geral).replace(',', 'X').replace('.', ',').replace('X', '.')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric(label="**💰 Custo HE 50%**", value=f"R$ {total_he_50:,.2f}")
        kpi2.metric(label="**💰 Custo HE 100%**", value=f"R$ {total_he_100:,.2f}")
        kpi3.metric(label="**👥 Colaboradores com HE**", value=f"{total_colaboradores}")

        kpi4, kpi5, kpi6 = st.columns(3)
        kpi4.metric(label="**⏰ Total Horas 50%**", value=f"{total_horas_50_dec:,.2f}h")
        kpi5.metric(label="**⏰ Total Horas 100%**", value=f"{total_horas_100_dec:,.2f}h")
        kpi6.metric(label="**⚙️ Total Horas (50% + 100%)**", value=f"{(total_horas_50_dec + total_horas_100_dec):,.2f}h")
        
        st.markdown("---")

        # --- SEÇÃO DE GRÁFICOS ---
        col_graf1, col_graf2 = st.columns(2)

        # GRÁFICO 1: Custo de HE por Cargo (Gráfico de Barras)
        with col_graf1:
            st.subheader("Custo de HE por Cargo")
            custo_por_cargo = df_filtrado.groupby('cargo')['valor_total'].sum().sort_values(ascending=False).reset_index()

            fig_bar = px.bar(
                custo_por_cargo,
                x='cargo',
                y='valor_total',
                title='Custo Total de Horas Extras por Cargo',
                labels={'cargo': 'Cargo', 'valor_total': 'Custo Total (R$)'},
                text_auto='.2s',
                color_discrete_sequence=px.colors.qualitative.Plotly  # Paleta de cores profissional
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
                hovertemplate='<b>Cargo:</b> %{x}<br><b>Custo Total:</b> R$ %{y:,.2f}<extra></extra>'
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
                    valor_formatado = f"R$ {row[filial]:,.2f}"
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

        st.info("Para facilitar a inserção da anotação/justificativa, filtre por um dia específico abaixo.")        
        data_anotacao_filtro = st.date_input("**Filtrar por data:**", value=date.today(), key="anotacao_filtro_data", format="DD/MM/YYYY")
        
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
                'data', 'nome', 'cargo', 'valor_total', 'texto_anotacao', 'nome_usuario'
            ]].copy()
            
            # 2. Renomeia as colunas para o formato final do relatório
            df_para_download = df_para_download.rename(columns={
                'data': 'Data',
                'nome': 'Colaborador',
                'cargo': 'Cargo',
                'valor_total': 'Valor Total (R$)',
                'texto_anotacao': 'Anotação',
                'nome_usuario': 'Usuario Responsavel'
            })
            
            # 3. Garante a ordem exata das colunas
            df_para_download = df_para_download[[
                'Data', 'Colaborador', 'Cargo', 'Valor Total (R$)', 'Anotação', 'Usuario Responsavel'
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

            # O restante do código para exibir e editar a tabela continua o mesmo
            df_editor_pronto = df_para_anotar.copy()
            df_editor_pronto.rename(columns={'texto_anotacao': 'Anotação', 'nome': 'Colaborador', 'data': 'Data', 'cargo': 'Cargo', 'valor_total': 'Valor Total (R$)'}, inplace=True)
            df_editor_pronto['Data'] = pd.to_datetime(df_editor_pronto['Data']).dt.strftime('%d/%m/%Y')
            df_editor_pronto['Valor Total (R$)'] = df_editor_pronto['Valor Total (R$)'].apply(format_BRL)
            colunas_para_exibir = ['Data', 'Colaborador', 'Cargo', 'Valor Total (R$)', 'Anotação']
            
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
# 3. CONTROLE DE FLUXO PRINCIPAL
# ==============================================================================
if not get_logged_user():
    st.title("🔐 Autenticação de Usuário")
    with st.form("login_form"):
        email = st.text_input("Email")
        senha = st.text_input("Senha", type="password")
        if st.form_submit_button("Entrar"):
            user_info = authenticate_user(email, senha)
            if user_info:
                st.session_state['user'] = user_info
                st.rerun()
            else:
                st.error("Email ou senha inválidos.")
else:
    run_dashboard()
