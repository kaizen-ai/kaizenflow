# Supported Model References

<!-- toc -->

- [Models implementation matrix](#models-implementation-matrix)
  * [Implementations stages](#implementations-stages)
  * [Currently avaliable models](#currently-avaliable-models)

<!-- tocstop -->

# Models implementation matrix

## Implementations stages

See the
[checklist](/docs/deploying/all.model_deployment.how_to_guide.md#implementation-stages-checklist).

## Currently avaliable models

| Stage / Model                                               | C1b | C3a | C5b | C8a | C8b[^1] | C11a |
| ----------------------------------------------------------- | --- | --- | --- | --- | ------- | ---- |
| `SystemTestCase` unit tests                                 | +   | +   | +   | -   | +       | +    |
| TiledBacktest unit tests                                    | +   | +   | +   | +   | +       | +    |
| System reconciliation unit tests                            | +   | +   | +   | -   | +       | -    |
| Manual paper trading run from dev server                    | +   | +   | +   | -   | +       | +    |
| Scheduled AirFlow paper trading DAG & system reconciliation | +   | +   | +   | -   | +       | +    |
| PnL observer AirFlow DAG                                    | +   | +   | +   | -   | +       | -    |
| Multiday system reconciliation AirFlow DAG[^2]              | +   | +   | +   | -   | -       | -    |

[^1]: The model is currently disabled, see CmTask5789.
[^2]: The DAG currently disabled for all models, see CmTask5349.
