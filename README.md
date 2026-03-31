# 📦 FedEx AWB Tracker — Medicsupply Edition

Sistema de rastreamento automático de remessas internacionais via API oficial FedEx, com dashboard web interativo, relatórios Excel, alertas de atraso e identidade visual personalizada.

Desenvolvido sob medida para operações de importação de medicamentos com múltiplos AWBs simultâneos.

---

## ✨ O que o sistema faz

- Lê AWBs de um arquivo `.xlsx` e consulta a API FedEx em paralelo
- Classifica cada remessa em 6 estágios de status com lógica própria
- Gera dashboard web interativo com histórico completo de eventos por AWB
- Detecta mudanças de status entre consultas e destaca alertas visuais
- Calcula dias úteis brasileiros (feriados nacionais fixos e móveis)
- Mantém histórico de 3 meses e gera relatório mensal por região de origem
- Sobe servidor web local automaticamente — acesse em `http://localhost:8888`
- Loop automático com intervalo configurável (1h, 2h, 4h ou 8h)
- Interface visual no padrão da marca (tema claro personalizado)

---

## 🗂 Categorias de status

| Categoria | Descrição |
|-----------|-----------|
| 🏷 `LABEL CREATED` | Etiqueta criada, aguardando coleta |
| ✈ `COMING TO BRAZIL` | Em trânsito internacional |
| 🔍 `CUSTOMS INSPECTION` | Retido na alfândega / Receita Federal |
| 🚚 `NATIONAL TRANSIT` | Liberado — em trânsito nacional |
| 📦 `OUT FOR DELIVERY` | Saiu para entrega |
| ✅ `DELIVERED` | Entregue ao destinatário |

> Remessas com +5 dias úteis em alfândega ou +3 dias em Memphis são destacadas automaticamente em vermelho.

---

## 🖥 Dashboard

- KPIs clicáveis por categoria com filtro instantâneo
- Modal de detalhes por AWB com todos os eventos agrupados por data, hora e localização
- Heatmap de dias no status: verde → amarelo → laranja → vermelho
- ETA estimado por região de origem baseado no histórico
- Painel de mudanças com filtro por janela de tempo (1h / 6h / 12h / 24h)
- Exportar histórico de mudanças como CSV
- Relatório mensal com sparkline de volume e análise de lead time por região
- Relatório por período personalizado
- Countdown regressivo para a próxima atualização automática
- Tema visual personalizado (pink `#E91E8C` + grafite `#2D3A4A`)

---

## 🚀 Instalação

### Windows — um clique

1. Baixe e extraia os arquivos em uma pasta (ex: `C:\FedExTracker`)
2. Clique com o botão direito em `instalar.bat` → **Executar como administrador**
3. O instalador configura Python e dependências automaticamente
4. Um atalho será criado na Área de Trabalho

### Manual

```bash
pip install requests pandas openpyxl tqdm python-dotenv
python fedex_api_oficial.py
```

---

## ⚙️ Configuração

Na primeira execução, o navegador abre automaticamente em `http://localhost:8888/config`.

Insira seu **Client ID** e **Client Secret** da FedEx API. O sistema valida as credenciais antes de salvar.

> Obtenha credenciais gratuitas em [developer.fedex.com](https://developer.fedex.com) → My Projects → Track API

Para alterar depois: acesse `http://localhost:8888/config` com o sistema rodando.

---

## 📁 Estrutura do projeto

```
fedex-awb-tracker/
├── fedex_api_oficial.py            # Script principal
├── aplicar_tema_medicsupply.py     # Patch de tema visual
├── instalar.bat                    # Instalador Windows
├── requirements.txt                # Dependências
├── awbs.xlsx                       # Sua lista de AWBs (não incluído)
├── .gitignore
└── README.md
```

| Arquivo gerado | Descrição |
|----------------|-----------|
| `ultimo_status_gerado.xlsx` | Relatório Excel com abas por categoria |
| `ultimo_status_gerado.html` | Dashboard interativo |
| `historico_status.xlsx` | Histórico dos últimos 3 meses |
| `config.json` | Credenciais e preferências (não subir no Git) |
| `tracking.log` | Log de cada execução com rotação automática |

---

## 🛠 Stack

Python · FedEx Track API · pandas · openpyxl · tqdm · HTML/JS puro · servidor HTTP nativo

---

## 📋 Changelog

### v4.0 — Março 2026
- Tema visual personalizado aplicado (Medicsupply Edition)
- Script `aplicar_tema_medicsupply.py` para aplicação do tema sem edição manual
- Dashboard migrado para modo claro com paleta de marca
- Header e rodapé com identidade visual completa
- Hover, badges e filtros ativos no padrão da marca (pink `#E91E8C` + grafite `#2D3A4A`)

### v3.0
- Relatório executivo semanal em HTML
- Relatório por período personalizado via interface web
- Servidor web embutido com rota `/gerar-relatorio`
- Módulo de configuração via browser (sem edição de arquivos)
- Painel de mudanças de status com filtro por janela de tempo
- Histórico mensal com sparkline de volume por região

### v2.0
- Dashboard HTML interativo com modal de eventos por AWB
- Heatmap de dias no status
- ETA estimado por região de origem
- Detecção de mudanças entre consultas
- Cálculo de dias úteis brasileiros com feriados móveis

---

## 📄 Licença

MIT
