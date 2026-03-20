# FedEx AWB Tracker v3

Rastreamento automático de remessas internacionais via API oficial FedEx — dashboard web, relatórios Excel, alertas de atraso e instalação com um clique.

Desenvolvido para operações de importação com múltiplos AWBs simultâneos.

---

## O que faz

- Lê AWBs de um arquivo `.xlsx` e consulta a API FedEx em paralelo
- Classifica cada remessa em 5 categorias de status com lógica própria
- Gera dashboard web interativo com histórico completo de eventos por AWB
- Detecta mudanças de status entre consultas e destaca alertas
- Calcula dias úteis brasileiros (feriados nacionais fixos e móveis)
- Mantém histórico de 3 meses e gera relatório mensal por região de origem
- Sobe servidor local automaticamente — acesse pelo navegador em `http://localhost:8888`
- Loop automático com intervalo configurável (1h, 2h, 4h ou 8h)

---

## Categorias de status

| Categoria | Descrição |
|-----------|-----------|
| 🏷 `LABEL CREATED` | Etiqueta criada, aguardando coleta |
| ✈ `COMING TO BRAZIL` | Em trânsito internacional |
| 🔍 `CUSTOMS INSPECTION` | Retido na alfândega / Receita Federal |
| 🚚 `NATIONAL TRANSIT` | Liberado — em trânsito nacional |
| 📦 `OUT FOR DELIVERY` | Saiu para entrega |
| ✅ `DELIVERED` | Entregue ao destinatário |

> Remessas em atraso (+5 dias úteis em alfândega ou +3 dias em Memphis) são destacadas automaticamente em vermelho.

---

## Dashboard

- KPIs clicáveis por categoria com filtro instantâneo
- Modal de detalhes por AWB: todos os eventos agrupados por data com hora e localização, no estilo do site da FedEx
- Coluna de dias com heatmap: verde → amarelo → laranja → vermelho conforme o tempo parado
- ETA estimado por região de origem baseado no histórico
- Painel de mudanças com filtro por janela de tempo (1h / 6h / 12h / 24h)
- Exportar mudanças como CSV
- Relatório mensal com sparkline de volume e análise de lead time por região
- Relatório por período com resumo de movimentações (gerado via `/gerar-relatorio`)
- Countdown regressivo para a próxima atualização automática

---

## Instalação

### Windows — um clique

1. Baixe e extraia os arquivos em uma pasta (ex: `C:\FedExTracker`)
2. Clique com o botão direito em `instalar.bat` → **Executar como administrador**
3. O instalador configura Python e dependências automaticamente
4. Um atalho "FedEx Tracker" será criado na Área de Trabalho

### Manual

```bash
pip install requests pandas openpyxl tqdm python-dotenv
python fedex_api_oficial.py
```

---

## Configuração

Na primeira execução, o navegador abre automaticamente a tela de configuração em `http://localhost:8888/config`.

Insira seu **Client ID** e **Client Secret** da FedEx API. O sistema valida as credenciais antes de salvar — sem precisar editar nenhum arquivo.

> Obtenha credenciais gratuitas em [developer.fedex.com](https://developer.fedex.com) → My Projects → Track API

Para alterar depois: acesse `http://localhost:8888/config` com o sistema rodando.

---

## Arquivo de AWBs

Crie `awbs.xlsx` na pasta do projeto com as colunas:

| AWB | Pedido | PRODUTO |
|-----|--------|---------|
| 770123456789 | 75001 | NOME DO PRODUTO |

`Pedido` e `PRODUTO` são opcionais mas aparecem no dashboard e nos relatórios.

---

## Estrutura do projeto

```
fedex-awb-tracker-v3/
├── fedex_api_oficial.py   # Script principal
├── instalar.bat           # Instalador Windows
├── requirements.txt       # Dependências
├── awbs.xlsx              # Sua lista de AWBs (não incluído)
├── .gitignore
└── README.md
```

Arquivos gerados automaticamente:

| Arquivo | Descrição |
|---------|-----------|
| `ultimo_status_gerado.xlsx` | Relatório Excel com abas por categoria |
| `ultimo_status_gerado.html` | Dashboard interativo |
| `historico_status.xlsx` | Histórico dos últimos 3 meses |
| `config.json` | Credenciais e preferências (não subir no Git) |
| `tracking.log` | Log de cada execução com rotação automática |

---

## Stack

Python · FedEx Track API · pandas · openpyxl · tqdm · HTML/JS puro

---

## Licença

MIT
