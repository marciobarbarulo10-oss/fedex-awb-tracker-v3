# 📦 FedEx AWB Tracker — Rastreamento em Massa via API Oficial

Ferramenta Python para rastreamento automatizado de múltiplos AWBs FedEx com classificação inteligente de status, dashboard HTML interativo e relatório mensal analítico.

Desenvolvida para operações de **importação farmacêutica**, com foco em visibilidade de carga em trânsito internacional para o Brasil.

---

## 🚀 O que faz

- Lê uma lista de AWBs de um arquivo `.xlsx` (com pedido e produto)
- Consulta a **API oficial da FedEx** em paralelo (multi-thread)
- Classifica automaticamente cada remessa em **5 categorias de status**
- Gera um **dashboard HTML interativo** atualizado a cada hora
- Abre painel de detalhe completo por AWB ao pesquisar — linha do tempo de eventos estilo FedEx
- Detecta e alerta remessas **em atraso** (alfândega ou hub internacional) em **dias úteis brasileiros**
- Calcula **ETA estimado** com base no histórico de entregas por região de origem
- Mantém **histórico de 3 meses** e detecta mudanças de status do dia
- Gera **relatório mensal** navegável por mês com sparkline de volume e análise por região

---

## 📊 Categorias de Status

| Categoria | Descrição |
|---|---|
| 🏷 `LABEL CREATED` | Etiqueta criada, aguardando coleta |
| ✈ `COMING TO BRAZIL` | Em trânsito internacional |
| 🔍 `CUSTOMS INSPECTION` | Retido na Receita Federal / Alfândega |
| 🚚 `NATIONAL TRANSIT` | Liberado — em trânsito nacional |
| 📦 `OUT FOR DELIVERY` | Saiu para entrega |
| ✅ `DELIVERED` | Entregue ao destinatário |

> ⚠️ Remessas em atraso (+5 dias úteis em Alfândega ou +3 dias úteis em Memphis) são destacadas em vermelho no dashboard.

---

## 🖥 Dashboard HTML

O tracker gera automaticamente um dashboard interativo acessível pelo navegador em `http://localhost:8888`, incluindo:

- **KPIs por categoria** com barras de acento coloridas e filtro por clique
- **Mapa de calor** na coluna de dias — verde → amarelo → laranja → vermelho conforme o tempo parado
- **Linha do tempo por AWB** no hover de cada linha da tabela
- **Ordenação clicável** em todas as colunas
- **Coluna ETA** com estimativa de dias úteis restantes por região
- **Painel de detalhe completo** ao buscar um AWB ou pedido — todos os eventos agrupados por data com hora e localização, igual ao site da FedEx
- **Countdown regressivo** para a próxima atualização automática
- **Mudanças de status do dia** com transições destacadas
- **Relatório Mensal** com sparkline de volume histórico, navegação por mês e KPIs de lead time por região (mínimo, máximo, média — clicáveis para ver o AWB correspondente)

---

## 📅 Dias Úteis Brasileiros

O lead time e os alertas de atraso são calculados em **dias úteis**, excluindo finais de semana e os seguintes feriados:

**Fixos:** Ano Novo, Tiradentes, Dia do Trabalho, Independência, N. Sra. Aparecida, Finados, Proclamação da República, Consciência Negra, Natal

**Móveis (calculados por ano):** Segunda e Terça de Carnaval, Sexta-feira Santa, Páscoa, Corpus Christi

---

## 🗂 Estrutura do Projeto

```
fedex-awb-tracker-v3/
├── tracker.py               # Script principal
├── requirements.txt         # Dependências
├── awbs.xlsx                # Sua lista de AWBs (não incluído — crie o seu)
├── .gitignore
└── README.md
```

### Arquivos gerados automaticamente

| Arquivo | Descrição |
|---|---|
| `ultimo_status_gerado.xlsx` | Relatório Excel mais recente |
| `ultimo_status_gerado.html` | Dashboard interativo |
| `historico_status.xlsx` | Histórico dos últimos 3 meses |
| `tracking.log` | Log detalhado de cada execução |

---

## ⚙️ Configuração

### 1. Clone o repositório

```bash
git clone https://github.com/marciobarbarulo10-oss/fedex-awb-tracker-v3.git
cd fedex-awb-tracker-v3
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

### 3. Configure suas credenciais FedEx

Edite o arquivo `tracker.py` e substitua as credenciais na classe `Config`:

```python
client_id:     str = "SEU_CLIENT_ID_AQUI"
client_secret: str = "SEU_CLIENT_SECRET_AQUI"
```

> Obtenha suas credenciais gratuitamente em: [developer.fedex.com](https://developer.fedex.com)

### 4. Crie o arquivo `awbs.xlsx`

O arquivo deve ter as seguintes colunas:

| AWB | Pedido | PRODUTO |
|---|---|---|
| 123456789012 | 75001 | MEDICAMENTO A |
| 987654321098 | 75002 | MEDICAMENTO B |

A coluna `PRODUTO` é opcional mas recomendada — aparece no painel de detalhe e no relatório mensal.

---

## ▶️ Uso

```bash
python tracker.py
```

O script vai:

1. Ler os AWBs do `awbs.xlsx`
2. Autenticar na API FedEx (token OAuth)
3. Consultar todos os AWBs em paralelo (barra de progresso)
4. Salvar o relatório Excel e o dashboard HTML
5. Atualizar o histórico de 3 meses
6. Subir servidor local em `http://localhost:8888`
7. Repetir automaticamente a cada 1 hora

---

## 🛠 Tecnologias

- **Python 3.8+**
- `requests` — chamadas à API FedEx
- `pandas` — manipulação de dados
- `openpyxl` — geração do Excel formatado
- `tqdm` — barra de progresso
- `concurrent.futures` — consultas paralelas
- Dashboard em **HTML + CSS + JavaScript** puro — sem dependências de frontend

---

## 💡 Contexto

Projeto desenvolvido durante o curso de **Engenharia de Software** aplicando conhecimentos práticos adquiridos na gestão de processos de importação farmacêutica.

Resolve um problema real: visibilidade centralizada de múltiplas remessas internacionais, com análise de lead time, alertas de atraso e histórico mensal — sem depender do site manual da FedEx.

---

## 📄 Licença

MIT License — sinta-se livre para usar e adaptar.
