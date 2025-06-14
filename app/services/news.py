import unicodedata
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import feedparser
from urllib.parse import quote_plus
from newspaper import Article
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import pandas as pd
import spacy
from spacy.lang.pt.stop_words import STOP_WORDS
import torch
from collections import Counter
import pandas as pd
from services import database
import datetime as dt

nlp = spacy.load("pt_core_news_sm")
def get_source_url(google_news_url):
    options = Options()
    options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get(google_news_url)

    try:
        time.sleep(5)
        href = driver.current_url
    except Exception as e:
        print("Erro:", e)
        href = None
    finally:
        driver.quit()

    return href

def get_google_news_urls(query, max_results=5):
    palavra_chave = quote_plus(query)
    url = f'https://news.google.com/rss/search?q={palavra_chave}&hl=pt-BR&gl=BR&ceid=BR:pt'
    feed = feedparser.parse(url)
    return [get_source_url(entry.link) for entry in feed.entries[:max_results]]

def predict_sentiment(texts):
    if not texts:
        return []
    
    MODEL_NAME = "tabularisai/multilingual-sentiment-analysis"
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

    inputs = tokenizer(texts, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
    sentiment_map = {0: "Muito Negativo", 1: "Negativo", 2: "Neutro", 3: "Positivo", 4: "Muito Positivo"}
    return [sentiment_map[p] for p in torch.argmax(probabilities, dim=-1).tolist()]

def process_text(text):
    # Normalização: lowercase + remoção de acentos
    text = text.lower()
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")

    extra_stopwords = {"dos", "para", "de", "sua", "que", "da", "em", "o", "a", "as", "os", "no", "na"}
    stop_words = STOP_WORDS.union(extra_stopwords)

    doc = nlp(text)
    tokens = []
    for token in doc:
        lemma = token.lemma_.lower()
        if (
            token.is_alpha and
            lemma not in stop_words and
            token.pos_ not in {'PRON', 'ADP', 'DET', 'PART'}
        ):
            tokens.append(lemma)

    return ' '.join(tokens)

def process_article(url, user_agent):
    try:
        article = Article(url, language='pt', browser_user_agent=user_agent)
        article.download()
        article.parse()
    except Exception as e:
        print(f"[ERRO] Não foi possível acessar {url}: {e}")
        return None

    doc = nlp(article.text)
    phrases = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 20]

    sentiments = predict_sentiment(phrases)
    sentiment_counts = Counter(sentiments)
    top_sentiment = sentiment_counts.most_common(1)[0][0]

    return article.title, top_sentiment, article.text


def sentiment_analysis(sector):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

    news_titles = []
    valid_urls = []
    news_sentiments = []
    news = []
    processed_texts = []

    # Verifica a última inserção
    query_max = '''
        SELECT MAX(insercao) as last_update
        from noticias
        WHERE setor = %s
    '''
    last_update_df = database.read_from_database(query_max, params=(sector,))
    
    last_update = None
    if last_update_df is not None and not last_update_df.empty:
        raw_last_update = last_update_df.iloc[0]['last_update']
        if pd.notna(raw_last_update):
            last_update = pd.to_datetime(raw_last_update)

    now = dt.datetime.now()

    if last_update is None or (now - last_update) >= dt.timedelta(hours=72):
        search = f"Setor {sector}"
        urls = get_google_news_urls(search)

        for url in urls:
            check_query = "SELECT titulo, sentimento, texto, texto_processado from noticias WHERE url = %s LIMIT 1"
            result = database.read_from_database(check_query, params=(url,))
            
            if not result.empty:
                row = result.iloc[0]
                news_titles.append(row['titulo'])
                valid_urls.append(url)
                news_sentiments.append(row['sentimento'])
                news.append(row['texto'])
                processed_texts.append(row['texto_processado'])
            else:
                result = process_article(url, user_agent)
                if result:
                    title, sentiment, text = result
                    processed_text = process_text(text)

                    row_df = pd.DataFrame([{
                        'titulo': title,
                        'texto': text,
                        'texto_processado': processed_text,
                        'url': url,
                        'setor': sector,
                        'sentimento': sentiment,
                        'insercao': now
                    }])

                    database.write_to_database('noticias', row_df, exists='append')

                    news_titles.append(title)
                    valid_urls.append(url)
                    news_sentiments.append(sentiment)
                    news.append(text)
                    processed_texts.append(processed_text)
    else:
        existing_news_query = """
            SELECT titulo, sentimento, texto, url, texto_processado
            from noticias
            WHERE setor = %s
        """
        existing_news = database.read_from_database(existing_news_query, params=(sector,))
        
        for _, row in existing_news.iterrows():
            news_titles.append(row['titulo'])
            valid_urls.append(row['url'])
            news_sentiments.append(row['sentimento'])
            news.append(row['texto'])
            processed_texts.append(row['texto_processado'])

    data = pd.DataFrame({
        'Notícia': news_titles,
        'URL': valid_urls,
        'Sentimento': news_sentiments
    })

    word_cloud_text = " ".join(processed_texts)

    if news_sentiments:
        all_sentiment_counts = Counter(news_sentiments)
        reputation = all_sentiment_counts.most_common(1)[0][0]
    else:
        reputation = "Não foi possível definir"

    return reputation, data, word_cloud_text