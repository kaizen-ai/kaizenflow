# Template for a paper review

## Meta
- Year - Title
- Paper authors:
- Link to the paper (ideally on gdrive)
- Review author / date
- Score in [0, 5], where:
    - 5/5: Must-read
    - 4/5: Some interesting ideas we can reuse
    - 3/5: Pretty much what one would have done as first experiment
    - 2/5: ...
    - 1/5: Horrible: same bullet-proof logic as in a politician speech
- Summary:
    - At most 5-10 bullet points explaining what the paper tries to accomplish
    - Describe the data used, setup, model formulation, ...
    - Good references
- Praises:
    - At most 5 bullet points
    - Focus on what is different, interesting, and not on the obvious
- Critiques:
    - At most 5 bullet points
    - Explain what is not solid in the analysis, suggestions on how to improve
- Next steps:
    - What next steps should we take, if any, e.g.,
        - Read the bibliography
        - Try experiments

## To cut and paste

```
## Year - Title
- Paper authors:
- [Link]()
- Review author / date:
- Score:
- Summary:
- Critique:
- Next steps:
```

# News for commodity prediction

## 2015 - The role of news in commodity markets
- Paper authors: Borovkova
- [Link](https://drive.google.com/file/d/1p3Z6W5DPBrDyTGBK__uLE2gNkQDO6VTM/view?usp=sharing)
- Review author / date: GP, 2019-11-22
- Score: 4/5
- Summary:
    - Dataset: prepackaged Thomson-Reuters sentiment (TRNA)
    - Studies the effect of sentiment on commodities through event studies
    - Forecast prices and volatility
- Praises:
    - Decent statistics about the data set
    - States that one needs to understand if the sentiment is attached to demand
      and supply
        - Not sure if TR actually does that
    - Confirms our point about "momentum-related news" (i.e., news about the fact
      that the price is going up)
    - Confirms periodicity we are aware of
    - Interesting local level model to extract the hidden sentiment
        - Very similar to what we thought to do (including the idea of using
          Kalman smoother)
- Critiques:
    - Nothing really
- Next steps:
    - Understand if TR considers sentiment distinguishing supply or demand
        - We should do this (not sure how PR does that)
    - Remove carefully momentum-related news
    - Remove or count carefully repeated news (maybe use a measure of similarity
      between articles)
    - How to deliver "event study" models to customers? Should we "unroll the
      model" for them providing a stream of predictions?

# Social sentiment

## 2018 - Twitter, Investor Sentiment and Capital Markets, what do we know?
- Paper authors:
- Review author: GP, 2019-08-21
- Link:
- Score: 3 / 5
- Summary:
    - Good survey of the literature about social sentiment used for finance
    - Most authors report predictivity of social sentiment for:
    - Different metrics (returns, risk, trading volume)
    - Assets (US stocks, exchange rates, commodities)
    - Events (IPO, earnings)
- Next steps:
    - Read all the bibliography and reproduce some of the results
