# Results and insights

Back to the [README](../README.md).

## Results 2023 (run of 16.11.2026, pedestrian streets excluded)

| metric | value |
|---|---|
| total compensation | 4,034,540,500 ILS |
| compensated businesses | 7,503 (26 businesses had no street code) |
| compensated business-days | 655,176 |
| average compensation per business | 537,723.64 ILS |
| cost per area unit | 783.07 ILS |
| share of business-days at the 10,000 cap | 34.8% |
| cap savings vs. uncapped | 17,180,234,000 ILS |
| streets closed on at least one day | 93 |
| pedestrian-street events excluded | 9 |

Top streets (`tlv.gold.comp_by_street_2023`):

| street | businesses | closure days | compensation (ILS) | share |
|---|---|---|---|---|
| Ibn Gvirol | 592 | 365 | 1,211,609,000 | 30.0% |
| Allenby | 518 | 331 | 946,746,000 | 23.5% |
| Herzl | 288 | 142 | 304,042,600 | 7.5% |
| Sde Yitzhak | 201 | 176 | 242,200,800 | 6.0% |
| Eilat | 154 | 179 | 219,447,000 | 5.4% |

## Insights

1. More than half of the 2023 compensation (53.5%) comes from two light-rail streets, Ibn Gvirol and Allenby: 1,110 businesses closed almost the whole year. The ten largest streets hold 85% of the total.
2. Long closures drive the bill: 78 closure events touch 2023, and 11 of them (over 90 days each, light rail and infrastructure works) account for 78% of all closure days.
3. The rate is only half the lever. Cutting it from 100 to 50 ILS per area unit lowers the bill by 30% (to 2.84 billion), not 50%, because many businesses are already at the 10,000 ILS daily cap; at 20 ILS the bill is 1.65 billion. Any policy change has to move the cap and the rate together.
4. 24.2.2023 shows a one-day spike of 71 closed streets: the Tel Aviv Marathon (44 one-day closures of type "ארוע מרתון" in the source).
5. Single licenses can carry very large areas (the central bus station holds 11 licenses with 361,365 area units in total), so the 10,000 ILS daily cap is what keeps the model bounded: without it the 2023 bill would be 21.2 billion ILS.
6. Public bodies are compensated like any business: the municipality itself (owner id 500250006) holds 123 licenses that receive 83,095,800 ILS (2.1%), and a keyword match on owner names (municipality, ministries, the State, the Electric Company, hospitals, universities) finds 496 licenses receiving 164,617,500 ILS, 4.1% of the total. Kept in the model: the rules say nothing about public bodies and the data has no reliable flag for them. This is a policy decision to raise, not one to make in the pipeline.
7. Data checks run on 16.11.2026 (`sql/data_checks.sql`): the total recomputed directly from Silver in one SQL query equals the Gold total to the shekel (4,034,540,500); all 37,370 business ids reach Silver, none rejected; no duplicates at any grain; area is plausible (median 74, 90th percentile 624, one business with area 0, the ten largest are a farm, the Ministry of Defence, Ichilov hospital, the Electric Company and the central bus station, 41% of businesses are at or above the 100-unit cap threshold); 983 business ids (2.6%) share owner and address with another id, all with different areas, so they are distinct licenses in one complex, not duplicates.
8. Before excluding permanent pedestrian streets (assumption 11) the total was 6.2 billion ILS; five events from June 2020 added 2.2 billion.
