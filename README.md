# BankNifty 09:20 AM Short Strangle — Backtest

This script backtests a simple options-selling strategy on BankNifty: short a CE and a PE at 09:20 AM every Week-1 trading day, exit by 15:20 or when a 50% stop-loss fires. Everything runs on 1-minute OHLCV data from a CSV file.

---

## What the strategy does

- **Entry:** At 09:20 AM on every calendar day 1–7 of the month (Week-1), short 1 lot each of the CE and PE whose premium is closest to ₹50.
- **Lot size:** 15 units per leg (1 lot).
- **Exit:** At 15:20 close — unless a 50% SL fires earlier (i.e., the option price goes 1.5× the entry price on the High column between 09:21 and 15:19).
- **Expiry:** First Wednesday of the month that falls in Week-1 and has data.
- **No compounding.** Fixed 1 lot every day regardless of capital.

---

## Requirements

```bash
pip install pandas numpy matplotlib openpyxl
```

Python 3.8+ should work fine.

---

## How to run

```bash
python banknifty_backtestt.py <path_to_data.csv>
```

If you don't pass a CSV path, the script will pick the largest `.csv` file it finds in the same folder automatically.

**Example:**
```bash
python banknifty_backtestt.py Options_data_2023.csv
```

---

## Input data format

The CSV should have 1-minute OHLCV data with at least these columns (column names are case-insensitive):

| Column   | Description                                    |
|----------|------------------------------------------------|
| Ticker   | Option symbol, e.g. `BANKNIFTY44000CE`         |
| Date     | Trade date (DD-MM-YYYY or similar)             |
| Time     | Bar time (HH:MM)                               |
| Open     | Open price                                     |
| High     | High price (used for SL check)                 |
| Low      | Low price                                      |
| Close    | Close price (used for entry/exit)              |

The underlying index rows should also be present — the script separates them by checking which tickers don't parse as options.

---

## Output

After the run, you'll get:

- **`banknifty_strangle_backtest.xlsx`** — the main report with three sheets:
  - *Guide* — explains the strategy and methodology
  - *TradeSheet* — every trade, entry/exit price, P&L, SL hit flag, etc.
  - *Statistics* — CAGR, max drawdown, win/loss breakdown, monthly P&L table, and embedded charts
- **`equity_curve.png`** — NAV chart over the backtest period
- **`drawdown.png`** — drawdown from peak equity

A quick summary also prints to the terminal when the run finishes.

---

## Key implementation details

A few things worth knowing if you're reading the code:

- **CAGR** is calculated using the actual calendar date range (`(last_date - first_date).days / 365.25`), not trading-day count. Week-1 only gives ~55–60 trading days per year, so dividing by 252 would massively overstate CAGR.
- **Available Capital** starts at ₹1,00,000 on Day 1 (not zero).
- **SL scan** excludes the 15:20 bar — that bar is always treated as the normal exit so there's no double-counting.
- **Monthly P&L** is chained: each month's starting NAV is the prior month's ending NAV, so the percentages reconcile properly to the overall return.
- The whole backtest is **fully vectorised** — no row-level Python loops, so it stays fast even on large datasets.

---

## Project files

```
short strangle/
├── banknifty_backtest.py          # main backtest script
├── Options_data_2023.csv           # input data (1-min OHLCV)
├── banknifty_strangle_backtest.xlsx # output report (generated on run)
├── equity_curve.png                # equity chart (generated on run)
├── drawdown.png                    # drawdown chart (generated on run)
```
