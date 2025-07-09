<h1 align="center"> Trabalho de Conclusão de Curso </h1>

# Pré-requisitos
<ul>
<li>Python 3 --> https://www.python.org/downloads/</li>
<li>Git --> https://git-scm.com/downloads</li>
<li>Preferencialmente, crie um diretório para armazenar o ambiente, como no exemplo:</li>
</ul>

TCC Cássia/<br>
├── git clone https://github.com/cassiamariane/tcc<br>

Ao final da configuração o ambiente deverá seguir o seguinte exemplo:

TCC Cássia/
│
├── tcc/
│   └── ambiente de backend clonado através do git<br>

# Instalação
<ul>
<li>No diretório "TCC Cássia/", clique com o botão direito e selecione a opção "Open Git Bash here"</li>
<li>git clone https://github.com/cassiamariane/tcc</li>
<li><code>cd tcc</code></li>
<li><code>pip install virtualenv</code></li>
<li><code>virtualenv venv</code></li>
<li><code>. venv/Scripts/activate</code></li>
<li><code>pip install -r requirements.txt</code></li>
</ul>

# Aquisição dos dados
<ul> 
<li>Instale os arquivos CNPJ em <a href="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/" target="_blank">Portal de Dados Abertos - CNPJ</a></li> 
<li>Salve os arquivos obedecendo à seguinte arquitetura dentro da pasta do projeto "tcc":</li> 
├── tcc/<br>
│   └── app/<br>
│       └── models/<br>
│           └── Dados2025/<br>
│               └── CNPJ/<br>
│                   ├── Cnaes/<br>
│                   ├── Naturezas_juridicas/<br>
│                   ├── Municipios/<br>
│                   ├── Paises/<br>
│                   ├── Estabelecimentos/<br>
│                   └── Empresas/<br>
</ul>

# Banco de dados
<ul> 
<li>No Workbench (ou ferramenta de banco relacional de sua preferência), execute o script de criação do banco de dados.</li> <li>Ative o ambiente virtual: <code>. venv/Scripts/activate</code></li> 
<li>Execute o comando para criação da base: <code>python -m app.models.create_database</code></li> 
</ul>

# Execução
<ul>
<li><code>. venv/Scripts/activate</code></li>
<li><code>cd app</code></li>
<li><code>streamlit run main.py</code></li>
</ul>
