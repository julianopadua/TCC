# `src/ml/core.py`

## Função

Núcleo compartilhado do pipeline de ML:

- **`MemoryMonitor`:** acompanhamento de uso de RAM durante cargas grandes.
- **`TemporalSplitter`:** partição treino/teste respeitando eixo temporal (por ano ou regra configurável no uso pelo `train_runner`).

## Consumidores principais

`train_runner.py` e trainers em `src/models/`.
