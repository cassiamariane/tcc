import streamlit as st
import yfinance as yf
import pandas as pd
import datetime as dt
import plotly.express as px
import json
from services import tickers, utils, database, leads, news
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title='Oportunidades de negócios brasileiros')

@st.cache_data
def get_data():
    return tickers.get_most_active_tickers()

df = get_data()

@st.cache_data
def get_leads(query):
    df = database.read_from_database(query)
    df = leads.format_leads(df)
    return df

if df is not None and not df.empty:
    
    tab_1, tab_2, tab_3, tab_4, tab_5 = st.tabs(["Visão Geral", 
                                          "Análise Histórica", 
                                          "Análise de Reputação", 
                                          "Distribuição Geográfica", 
                                          "Empresas"])
    
    with tab_1:
        most_active_tickers = tickers.clean_symbols(df)
        period = st.selectbox("Selecione o período", ["Últimos 15 dias", "Último mês", "Últimos 6 meses", "Último ano"])
        final_df = tickers.get_tickets(df, most_active_tickers)
        final_df = final_df[final_df['Faixa'] == period]

        with open('models/setores.json', 'r', encoding='utf-8') as file:
            setores = json.load(file)
            
        options = list(setores.keys())
        select = st.sidebar.selectbox('Setor:', options)
        st.header("Ranking de Empresas Mais Ativas do Setor")
        text = """As empresas são classificadas pelo volume diário de negociação, indicando o quanto estão sendo transacionadas <br> 
                no mercado brasileiro."""

        st.markdown(text, unsafe_allow_html=True)

        filtered_df = final_df.loc[final_df["Setor"] == select]
        
        filtered_df = filtered_df.sort_values(by='Vol * Preço BRL', ascending=False)
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)
        st.header("Comparativo entre os setores")
        st.markdown("""A classificação dos setores brasileiros inclui os top gainers, que são os ativos com a maior variação percentual positiva <br>
                    no preço durante um determinado período. Por outro lado, os setores mais valorizados são aqueles com o maior valor de mercado.""", unsafe_allow_html=True)
        col_1, col_2 = st.columns([1, 1])

        with col_1:
            top_gainers = final_df.groupby('Setor')['Variação %'].sum().reset_index()
            top_gainers = top_gainers.sort_values(by='Variação %', ascending=False).head(50)
            cores_top = ['red' if categoria != select else 'blue' for categoria in top_gainers["Setor"]]
          
            st.subheader('Top Gainers')
            fig = px.bar(top_gainers, 
                        x='Variação %', 
                        y='Setor', 
                        orientation='h')
            fig.update_traces(marker_color=['blue' if categoria == select else 'red' for categoria in top_gainers['Setor']])
            st.plotly_chart(fig, use_container_width=True)

        with col_2:
            most_valued = final_df.groupby('Setor')['Valor'].sum().reset_index()
            most_valued = most_valued.sort_values(by='Valor', ascending=False).head(50)
            cores_most = ['red' if categoria != select else 'blue' for categoria in most_valued["Setor"]]
           
            st.subheader('Ativos Mais Valorizados')
            fig = px.bar(most_valued, 
                        x='Valor', 
                        y='Setor', 
                        orientation='h')
            fig.update_traces(marker_color=['blue' if categoria == select else 'red' for categoria in most_valued['Setor']])
            st.plotly_chart(fig, use_container_width=True)
            
    with tab_2:
        codigos = setores.get(select)
        codigos = ', '.join(map(str, codigos))
        query = f'''
                SELECT e.data_inicio_atividade
                FROM cnpj2.estabelecimentos e
                JOIN cnpj2.cnaes c 
                    ON e.cnae_fiscal_principal = c.codigo
                WHERE c.codigo IN ({codigos});
                '''
        dates = database.read_from_database(query)
        dates_list = dates['data_inicio_atividade'].tolist()

        df = pd.DataFrame(dates_list, columns=['Data'])

        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Ano'] = df['Data'].dt.year
        
        date_counts = df.groupby('Ano').size().reset_index(name='Quantidade')
        
        st.header("Linha do Tempo")
        st.markdown("""A representação indica a quantidade de abertura de empresas no setor ao decorrer dos anos.""", unsafe_allow_html=True)
        
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=date_counts['Ano'],
            y=date_counts['Quantidade'],
            name='Quantidade (Barras)',
            marker_color='#D3D3D3'
        ))

        fig.add_trace(go.Scatter(
            x=date_counts['Ano'],
            y=date_counts['Quantidade'],
            name='Quantidade (Linha)',
            mode='lines+markers',
            line=dict(color='red'),
            marker=dict(color='red')
        ))

        fig.update_layout(
            yaxis=dict(range=[0, date_counts['Quantidade'].max() + 1]),
            barmode='overlay'
        )

        st.plotly_chart(fig)

    with tab_3:
        st.header("Reputação do Setor")
        st.markdown("A reputação é estimada com base no equilíbrio entre termos positivos e negativos presentes nas notícias mais recentes sobre o setor.", unsafe_allow_html=True)

        reputation, data, word_cloud_text = news.sentiment_analysis(select)

        if data.empty:
            st.warning("Nenhuma notícia recente foi encontrada para esse setor.")
        else:
            st.subheader("Sentimento Predominante")
            
            if reputation == "Neutro":
                color = "#4A90E2"
            elif reputation in ["Positivo", "Muito Positivo"]:
                color = "#2FBF42"
            else:
                color = "#CB3131"

            st.markdown(f"<h2 style='color: {color};'>{reputation}</h2>", unsafe_allow_html=True)

            st.subheader("Notícias analisadas")
            data['Título da notícia'] = data.apply(
                lambda row: f'<a href="{row.URL}" target="_blank" style="color:black; text-decoration:none;">{row.Notícia}</a>', axis=1
            )

            data = data[['Título da notícia', 'Sentimento']]

            styled_data = data.style.map(utils.highlight_sentiment, subset=["Sentimento"])
            st.write(styled_data.to_html(escape=False, index=False), unsafe_allow_html=True)

            st.subheader("Nuvem de Palavras")
            st.markdown("Visualize as palavras mais frequentes nas notícias analisadas, destacando os termos com maior relevância e recorrência.", unsafe_allow_html=True)
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(word_cloud_text)

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis("off")
            st.pyplot(fig)
        
    with tab_4:
        query = f'''
                SELECT m.descricao AS municipio
                FROM cnpj2.estabelecimentos e
                JOIN cnpj2.cnaes c 
                    ON e.cnae_fiscal_principal = c.codigo
                JOIN cnpj2.municipios m
                    ON e.municipio = m.codigo
                WHERE c.codigo IN ({codigos});
                '''
        citys = database.read_from_database(query)
        df_coords = pd.read_csv('../app/models/municipios.csv')

        citys['municipio'] = citys['municipio'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip()
        df_coords['municipio'] = df_coords['nome'].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip()

        merged_df = citys.merge(df_coords, left_on='municipio', right_on='municipio', how='left')

        df_map = merged_df[['municipio', 'latitude', 'longitude']].dropna()

        st.header("Mapa de Localizações das Empresas do Setor")
        st.markdown("""O mapa fornece uma visualização da distribuição geográfica das empresas em um setor específico, <br> destacando sua localização em todo o território brasileiro.""", unsafe_allow_html=True)

        if not df_map.empty:
            st.map(df_map, latitude='latitude', longitude='longitude')
        else:
            st.warning("Nenhuma coordenada geográfica encontrada para os municípios informados.")

    with tab_5:
        query = f'''
                SELECT es.cnpj_basico, cnpj_ordem, cnpj_dv,
                razao_social AS Empresa, data_inicio_atividade AS "Data de abertura", 
                municipios.descricao AS Cidade, uf AS Estado, porte, situacao_cadastral, cnaes.descricao AS "CNAE"
                FROM cnpj2.estabelecimentos AS es
                INNER JOIN cnpj2.municipios ON municipio = municipios.codigo
                INNER JOIN cnpj2.empresas ON es.cnpj_basico = empresas.cnpj_basico
                INNER JOIN cnpj2.cnaes ON cnae_fiscal_principal = cnaes.codigo
                WHERE cnaes.codigo IN ({codigos})
                ORDER BY RAND(42)
                LIMIT 100;
                '''
        leads = get_leads(query)
        st.header("Empresas do setor")
        text = """Lista atualizada de empresas-chave utilizadas nas análises para ampliar sua rede."""
        st.markdown(text, unsafe_allow_html=True)
        col_1, col_2 , col_3 = st.columns([1, 1, 1])

        with col_1:
            estados = st.multiselect("Filtrar por Estado", sorted(leads["Estado"].dropna().unique()), placeholder="Selecione o estado")
        with col_2:
            portes = st.multiselect("Filtrar por Porte", sorted(leads["Porte"].dropna().unique()), placeholder="Selecione o porte")
        with col_3:
            setores = st.multiselect("Filtrar por Setor (CNAE)", sorted(leads["CNAE"].dropna().unique()), placeholder="Selecione o setor")

        filtered_leads = leads.copy()
        if estados:
            filtered_leads = filtered_leads[filtered_leads["Estado"].isin(estados)]
        if portes:
            filtered_leads = filtered_leads[filtered_leads["Porte"].isin(portes)]
        if setores:
            filtered_leads = filtered_leads[filtered_leads["CNAE"].isin(setores)]

        cols = [col for col in filtered_leads.columns if col != "CNAE"] + ["CNAE"]
        filtered_leads = filtered_leads[cols]

        st.dataframe(filtered_leads, use_container_width=True, hide_index=True)
else:
    st.write("Não foi possível carregar os dados.")