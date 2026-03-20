

# Trading Infrastructure (Legacy Version. Shortened)

Database-driven architecture for a crypto statistical arbitrage system, including pair selection, signal generation, execution, and risk monitoring.

This repository represents an early production-oriented trading system, where all components communicate through a centralized MySQL database and operate as independent processes.

---


## Trading Performance (Live Results)

This system was tested on live trading using Bybit infrastructure.

Key metrics (Jun–Nov 2025):

- Total trades: 704
- Win rate: 64.6%
- Profit factor: 1.22
- Balance return: 35%
- Max drawdown: -4.36%

Full report:

https://drive.google.com/file/d/120tinWJZwjI5GAySZXhz6B4y4T025ObQ/view?usp=sharing

reports/trading_performance.png

## Overview

The system is designed as a multi-stage trading pipeline:

1. Market data processing and statistical analysis  
2. Cointegration-based pair discovery  
3. Signal-driven trade execution  
4. Risk monitoring and alerting  

All modules are connected through MySQL, enabling decoupled execution and modular scaling.

---

## Architecture

SELECTION:
SQL-DB-Coint-upd-6.py  <-- fetch raw data, extract signals, update parameters in db. 
SQL-DB-Stat-upd-5.py  <-- update trade result, graduite pairs level, sanity check.

EXECUTION:
daily_guard_2.py  <-- RM engine.
Level_2_CFT_bot_07-12-2025.py  <-- executor, monitor, result writing to DB.
TG_messenger_2.py  <-- alret and upd messager.

---

## Visualization

### System Flow Diagram (Miro)

https://miro.com/app/board/uXjVIL2Bep0=/?share_link_id=286279014811

---

### Performance Dashboard (Power BI)

*temporary pdf version
https://drive.google.com/file/d/1xx7cCt0ES1gkZ8MYIYSVnwtESrrr6eVz/view?usp=sharing

[Power BI dashboard will be here here]

---

## Technologies Used

- Python — core implementation  
- MySQL — central data storage and communication layer  
- CCXT — exchange data access  
- Statsmodels / NumPy / Pandas — statistical analysis  
- Telegram API — monitoring and alerts  

---

## System Flow

1. Market data is collected and processed  
2. Statistical metrics (ADF, etc.) are calculated  
3. Cointegrated pairs are identified  
4. Results and signals are written to MySQL  
5. Execution bot reads signals and places trades  
6. Trade results are written back to database  
7. Risk guard monitors daily performance  
8. Alerts are sent via Telegram  

---

## Selection Layer

Responsible for generating trading candidates and maintaining statistical state.

### SQL-DB-Stat-upd-5.py

- Calculates:
  - stationarity metrics (ADF)
  - trading statistics
  - pair grading (level assignment)
- Performs:
  - sanity checks
  - data cleanup
- Writes results to MySQL

Core statistical processing engine.

---

### SQL-DB-Coint-upd-6.py

- Fetches market data via CCXT  
- Runs cointegration tests  
- Updates database with:
  - validated pairs  
  - statistical relationships  

Pair discovery and validation module.

---

## Execution Layer

Handles trading, monitoring, and communication.

### Level_2_CFT_bot_07-12-2025.py

- Main execution engine  
- Reads signals from MySQL  
- Opens / manages / closes trades  
- Writes trade results back to DB  

Features:
- Multi-account architecture (per bot)
- Level-based pair allocation (e.g. Level 2)
- Supports:
  - demo trading  
  - real trading (config switch)

Core trading engine.

---

### daily_guard_2.py

- Risk management module  
- Monitors:
  - daily PnL  
  - abnormal conditions  
- Enforces:
  - daily loss limits  
  - safety constraints  

System-level risk control.

---

### TG_messenger_2.py

- Telegram notification service  
- Sends:
  - errors  
  - system updates  
  - trading events  

Observability layer.

---

## Design Principles

### Database-Centric Architecture

All components interact via MySQL:

- signals  
- statistics  
- execution state  
- trade results  

Benefits:

- decoupled services  
- easy monitoring  
- scalable architecture  

---

### Multi-Level / Multi-Bot System

- Each bot operates on a separate account  
- Each pair is assigned a level  
- Bots trade only their assigned level  

Enables:

- capital segmentation  
- strategy isolation  
- controlled scaling  

---


## Limitations

This repository is not standalone.

Missing components:

- database schema  
- historical data  
- configuration files  
- credentials  

This repo is intended to demonstrate:

- system design  
- architecture decisions  
- trading workflow  

---

## Evolution

This system later evolved into a more modular architecture:

- Selection Layer  
- Working Layer  
- Execution Layer  

With improved:

- scheduling  
- fault tolerance  
- scalability  

---

## Disclaimer

This project is for research and educational purposes only.  
It does not constitute financial advice.