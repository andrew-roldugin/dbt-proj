# Data Vault 2.0 dbt-проект для закупок

- Используются raw → stage → vault (hubs, sats, links) модели
- Генерация ключей и хэш-диффов через automate_dv
- Все бизнес-ключи, payload и ссылки формируются через stg слой

## Слои
- models/stage/ — подготовка данных для автоматизации Data Vault
- models/raw_vault/hubs/ — бизнес-ключи
- models/raw_vault/sats/ — атрибуты
- models/raw_vault/links/ — связи

## Документация
Документация по моделям — в models/docs/schema.yml
