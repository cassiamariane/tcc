import streamlit as st
import yfinance as yf
import pandas as pd
import datetime as dt
import plotly.express as px
import json
from services import tickers, utils, database, leads, news
from wordcloud import WordCloud
import matplotlib.pyplot as plt

st.set_page_config(layout="wide", page_title='Oportunidades de negócios brasileiros')

@st.cache_data
def get_data():
    return tickers.get_most_active_tickers()

df = get_data()

@st.cache_data
def carregar_leads(query):
    df = database.read_from_database(query)
    df = leads.formatar_leads(df)
    return df


if df is not None and not df.empty:
    
    tab_1, tab_2, tab_3, tab_4, tab_5 = st.tabs(["Visão Geral", 
                                          "Análise Histórica", 
                                          "Análise de Reputação", 
                                          "Distribuição Geográfica", 
                                          "Leads"])
    
    with tab_1:
        # Obter tickers
        most_active_tickers = tickers.clean_symbols(df)
        period = st.selectbox("Selecione o período", ["Últimos 15 dias", "Último mês", "Últimos 6 meses", "Último ano"])
        final_df = tickers.get_tickets(df, most_active_tickers)
        final_df = final_df[final_df['Faixa'] == period]
        # Criar a barra lateral para seleção do setor
        with open('setores.json', 'r', encoding='utf-8') as file:
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
            # Ordenar os 50 maiores ganhadores por variação %
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
            # Ordenar os 50 maiores valores
            most_valued = final_df.groupby('Setor')['Valor'].sum().reset_index()
            most_valued = most_valued.sort_values(by='Valor', ascending=False).head(50)
            cores_most = ['red' if categoria != select else 'blue' for categoria in most_valued["Setor"]]
           
            # Plotar gráfico de setores para ativos mais valorizados
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
                FROM cnpj.estabelecimentos e
                JOIN cnpj.cnaes c 
                    ON e.cnae_fiscal_principal = c.codigo
                WHERE c.codigo IN ({codigos});
                '''
        datas = database.read_from_database(query)
        lista_datas = datas['data_inicio_atividade'].tolist()

        df = pd.DataFrame(lista_datas, columns=['Data'])

        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Ano'] = df['Data'].dt.year
        
        date_counts = df.groupby('Ano').size().reset_index(name='Quantidade')
        
        st.header("Linha do Tempo")
        st.markdown("""A representação indica a quantidade de abertura de empresas no setor ao decorrer dos anos.""", unsafe_allow_html=True)
        
        fig = px.line(date_counts, x='Ano', y='Quantidade', markers=True)
        fig.update_traces(line_color='red', marker_color='red')
        # Definindo o limite inferior do eixo Y como 0
        fig.update_yaxes(range=[0, date_counts['Quantidade'].max() + 1])
        st.plotly_chart(fig)

    with tab_3:
        st.header("Reputação do Setor")
        st.markdown("A reputação é construída com base nas notícias recentemente divulgadas.", unsafe_allow_html=True)

        reputation, data, word_cloud_text = news.sentiment_analysis(select)

        if data.empty:
            st.warning("Nenhuma notícia recente foi encontrada para esse setor.")
        else:
            st.subheader("Sentimento Predominante")
            
            if reputation == "Neutro":
                color = "#4A90E2"
            elif reputation == "Positivo" or reputation == "Muito positivo":
                color = "#2FBF42"
            else:
                color = "#CB3131"

            st.markdown(f"<h2 style='color: {color};'>{reputation}</h2>", unsafe_allow_html=True)

            st.subheader("Notícias analisadas")
            st.dataframe(data)

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
                FROM cnpj.estabelecimentos e
                JOIN cnpj.cnaes c 
                    ON e.cnae_fiscal_principal = c.codigo
                JOIN cnpj.municipios m
                    ON e.municipio = m.codigo
                WHERE c.codigo IN ({codigos});
                '''
        citys = database.read_from_database(query)
        lista_citys = citys['municipio'].tolist()
        coordinates = []
        
        for city in lista_citys:
            lat, lon = utils.get_coordinates(city)
            if lat and lon:
                coordinates.append({'city': city, 'latitude': float(lat), 'longitude': float(lon)}) 

        # Criar um DataFrame com as coordenadas
        df = pd.DataFrame(coordinates)
        
        st.header("Mapa de Localizações das Empresas do Setor")
        st.markdown("""O mapa fornece uma visualização da distribuição geográfica das empresas em um setor específico, <br> destacando sua localização em todo o território brasileiro.""", unsafe_allow_html=True)

        if not df.empty:
            st.map(df, latitude='latitude', longitude='longitude')

    with tab_5:
        query = f'''
                SELECT es.cnpj_basico, cnpj_ordem, cnpj_dv,
                razao_social AS Empresa, cnaes.descricao AS "CNAE", data_inicio_atividade AS "Data de abertura", 
                municipios.descricao AS Cidade, uf AS Estado, porte,situacao_cadastral
                FROM cnpj.estabelecimentos AS es
                INNER JOIN cnpj.municipios ON municipio = municipios.codigo
                INNER JOIN cnpj.empresas ON es.cnpj_basico = empresas.cnpj_basico
                INNER JOIN cnpj.cnaes ON cnae_fiscal_principal = cnaes.codigo
                WHERE cnaes.codigo IN ({codigos});
                '''
        leads = carregar_leads(query)
        st.header("Leads do setor")
        text = """Lista atualizada de empresas-chave para ampliar sua rede."""
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

        st.dataframe(filtered_leads, use_container_width=True, hide_index=True)
else:
    st.write("Não foi possível carregar os dados.")