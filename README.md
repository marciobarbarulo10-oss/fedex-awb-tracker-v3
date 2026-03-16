# 📦 FedEx AWB Tracker — Rastreamento em Massa via API Oficial

Ferramenta Python para rastreamento automatizado de múltiplos AWBs FedEx com classificação inteligente de status e geração de relatório Excel colorido.

Desenvolvida para operações de **importação farmacêutica**, com foco em visibilidade de carga em trânsito internacional para o Brasil.

---

## 🚀 O que faz

- Lê uma lista de AWBs de um arquivo `.xlsx`
- Consulta a **API oficial da FedEx** em paralelo (multi-thread)
- Classifica automaticamente cada remessa em **5 categorias de status**
- Gera um **relatório Excel formatado** com abas por categoria, resumo e gráfico
- Detecta e alerta remessas **em atraso** (alfândega ou hub internacional)
- Mantém **histórico** de todas as consultas e detecta **mudanças de status**

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

> ⚠️ Remessas em atraso (+5 dias em Alfândega ou +3 dias em Memphis) são destacadas em **vermelho** no relatório.

---

## 🗂 Estrutura do Projeto

```
fedex-tracker/
├── tracker.py          # Script principal
├── requirements.txt    # Dependências
├── awbs.xlsx           # Sua lista de AWBs (não incluído — crie o seu)
├── .gitignore
└── README.md
```

---

## ⚙️ Configuração

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/fedex-tracker.git
cd fedex-tracker
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

O arquivo deve ter ao menos uma coluna chamada `AWB`. A coluna `Pedido` é opcional.

| AWB | Pedido |
|---|---|
| 123456789012 | PED-001 |
| 987654321098 | PED-002 |

---

## ▶️ Uso

```bash
python tracker.py
```

O script vai:
1. Ler os AWBs do `awbs.xlsx`
2. Autenticar na API FedEx
3. Consultar todos os AWBs (com barra de progresso)
4. Salvar o relatório em `ultimo_status_gerado.xlsx`
5. Atualizar o histórico em `historico_status.xlsx`

---

## 📁 Arquivos Gerados

| Arquivo | Descrição |
|---|---|
| `ultimo_status_gerado.xlsx` | Relatório mais recente com abas por categoria |
| `historico_status.xlsx` | Histórico de todas as consultas |
| `tracking.log` | Log detalhado de cada execução |
| `snapshot_YYYYMMDD.xlsx` | Backups automáticos diários |

---

## 🛠 Tecnologias

- **Python 3.8+**
- `requests` — chamadas à API FedEx
- `pandas` — manipulação de dados
- `openpyxl` — geração do Excel formatado
- `tqdm` — barra de progresso
- `concurrent.futures` — consultas paralelas

---

## 💡 Contexto

Projeto desenvolvido durante o curso de **Engenharia de Software** aplicando conhecimentos práticos adquiridos na gestão de processos de importação farmacêutica.

Resolve um problema real: visibilidade centralizada de múltiplas remessas internacionais sem depender do site manual da FedEx.

---

## 📄 Licença

MIT License — sinta-se livre para usar e adaptar.
