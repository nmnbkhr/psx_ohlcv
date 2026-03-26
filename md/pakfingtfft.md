# 🧭 PakFinData Execution Playbook
**A Quantitative Guide to Signal Synthesis**

---

### STEP 1: Find the Macro Bias (Daily FFT)
*Question: Are we structurally overbought or oversold?*

Look at the **Macro Cycles** page. Identify the Dominant Cycle length and compare the Current Price to the smooth **IFFT Signal Line**.

* 📉 **Oversold (Bull Bias):** Price is significantly **BELOW** the IFFT line, but the cycle is bottoming out. 
* 📈 **Overbought (Bear Bias):** Price is significantly **ABOVE** the IFFT line, and the cycle is peaking. Mean-reversion is due.
* ➖ **White Noise:** Power spectrum is flat. Cycles are dead. Trade pure momentum instead.

---

### STEP 2: Read the Microstructure (Intraday VPIN)
*Question: Who is aggressively controlling the tape right now?*

Switch to the **Microstructure** page. Check the **VPIN Gauge** (Order Flow Toxicity).

* 🟢 **VPIN < 0.4 (Low Toxicity):** Retail noise. Balanced buying and selling. Safe environment.
* 🟡 **VPIN 0.4 - 0.7 (Elevated):** Imbalance is building. Institutions are quietly accumulating or distributing.
* 🔴 **VPIN > 0.7 (High Toxicity):** Toxic order flow. Algorithms are aggressively sweeping the book in one direction. 

---

### STEP 3: The Game Theory Execution
*Question: How do I route my order for the best fill?*

Combine Step 1 and Step 2 into the **Expected Value ($EV$) Matrix**:

| Macro Bias (FFT) | Micro Toxicity (VPIN) | $EV$ | Execution Strategy |
| :--- | :--- | :--- | :--- |
| **BULLISH** (Below IFFT) | 🟢 **LOW** (Safe) | **+** | **MAKER:** Post a Limit Buy Order on the Bid. Earn the spread. |
| **BULLISH** (Below IFFT) | 🔴 **HIGH** (Buy Volume) | **-** | **TAKER:** Cross the spread! Market Buy immediately before price spikes. |
| **BEARISH** (Above IFFT) | 🟢 **LOW** (Safe) | **+** | **MAKER:** Post a Limit Sell Order on the Ask. Let buyers come to you. |
| **BEARISH** (Above IFFT) | 🔴 **HIGH** (Sell Volume) | **-** | **TAKER:** Cross the spread! Market Sell immediately before the floor drops. |
| **NEUTRAL** | 🔴 **HIGH** (Mixed) | **-** | **AVOID:** Toxic chop. Widen quotes or step away from the terminal. |

---
*💡 Pro Tip: Never act as a Market Maker (post limit orders) when VPIN is in the Red Zone. The probability of adverse selection (getting run over by a larger player) is mathematically guaranteed.*