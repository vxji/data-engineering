# Demo script: 10 minutes

Run all cells in `labs/capstone/Qafila_Capstone.ipynb` before you start, so outputs are ready if the network fails. Keep the recorded backup open in another tab.

| Time | Speaker | Do | Say |
|---|---|---|---|
| 0:00 | Yahya | Slide 1 | "Finance needs a revenue number it can trust. Operations needs to see missed deliveries. Both must work with personal data locked away." |
| 1:00 | Yahya | Slide 2 | "Three sources became a governed platform: database, API and events, through staging and marts, to views and Iceberg." |
| 2:00 | Yahya | Run the one-command cell | "One command: 35 assets and 58 checks in about 30 seconds." |
| 3:00 | Yahya | Slide 3, then the re-run proof | "Schedulers retry. Here we run the same load twice, and a hash of every row is identical. Two days later, only 1,434 payments arrive, and 30 are refunds that replace old rows." |
| 5:00 | Abdullah | Slide 4, then the block proof | "The provider sends a status nobody agreed. The contract blocks, and the marts keep yesterday's numbers instead of a wrong one." |
| 6:30 | Abdullah | Quarantine cell | "16 payments had non-positive amounts. In the baseline, 14 delivered orders showed a negative payment. Now they sit here with a reason and an owner." |
| 7:30 | Abdullah | Slide 5, then the governance cell | "National IDs and IBANs never enter the warehouse. They're in a separate vault, and the warehouse holds only a keyed token. The scanner checks values on every run." |
| 8:45 | Yahya | Slide 6, operations chart | "The result: 6.5 million SAR of product revenue, and 42% of Riyadh deliveries miss the SLA, against about 28% elsewhere." |
| 9:30 | Yahya | Slide 7 | "Three limits, and what we'd do next." |

**If a live cell fails:** say what you expected, then show the saved output or the recording. Don't debug in front of the panel.
