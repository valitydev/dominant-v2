# План внедрения eqWAlizer в DMT

Документ описывает поэтапный rollout eqWAlizer с минимальным шумом в diff и максимальной
пользой от статической типизации. Dialyzer остаётся глобальным safety net; eqWAlizer —
точечным type checker'ом на выбранных модулях.

## Цель

Получить реальный signal от типизации (ошибки в domain-логике, некорректные return paths,
несовпадение типов на границах модулей) без типизации всего проекта «для галочки».

## Принципы

1. **Opt-in, не opt-out** — по умолчанию eqWAlizer выключен (`enable_all = false`).
2. **Specs только там, где eqWAlizer проверяет** — не типизируем весь проект ради CI.
3. **Один `dynamic_cast` на trust boundary** — woody/config/thrift reflection, не на каждую функцию.
4. **Reflection-модули — за dialyzer + CT**, не за eqWAlizer.
5. **Каждая фаза — отдельный mergeable PR** с зелёным CI.

## Текущее состояние ветки `add_specs`

| Что есть | Оценка |
|----------|--------|
| ELP в Docker, Makefile, CI workflow | Хорошо |
| `eqwalizer_support` в deps | Хорошо, но нужен pin на sha, не `main` |
| Specs в ~20 модулях (~800 строк) | Шум, мало пользы |
| `eqwalize-all` в CI | Заставляет типизировать всё |
| Нет `.elp.toml` | Default = все non-test модули |
| Фикс `object_type() :: atom() \| binary()` | Оставить — runtime bugfix |

## Разделение ответственности

```
Dialyzer     → весь проект, global analysis, specs не обязательны
EqWAlizer    → opted-in модули, local analysis, specs обязательны в проверяемом модуле
CT / eunit   → integration и regression
```

### Tier 1 — eqWAlizer opt-in (Phase 1)

- `dmt_object_id` (~63 строк)
- `dmt_object` (~134 строк)
- `dmt_author` (~57 строк)
- `dmt_author_database` (~187 строк)

### Tier 2 — eqWAlizer opt-in позже (Phase 2–3)

- `dmt_repository` (~1102 строк) — ядро бизнес-логики
- `dmt_database` (~1317 строк) — CRUD/search, без graph-функций

### Tier ∞ — `-eqwalizer(ignore).` (dialyzer + CT)

| Модуль | Причина |
|--------|---------|
| `dmt_domain` | Thrift schema reflection |
| `dmt_object_reference` | Дубликат reflection |
| `dmt_domain_pt` | Parse transform |
| `dmt_json` | Generic JSON↔term bridge |
| `dmt_mapper` (string↔thrift) | Return `dynamic()` неизбежен |
| `dmt_sup` | Opaque application env |
| `dmt_db_migration` | CLI / URI parsing |
| `dmt_author_handler`, `dmt_repository_handler`, `dmt_repository_client_handler` | Woody boundary |
| `dmt_kafka_publisher` | Brod config opaque |
| `dmt_api_woody_utils` | Config opaque |

---

## Фаза 0: Инфраструктура и откат шума

**PR:** `chore/eqwalizer-infra`  
**Оценка:** 0.5–1 день

### Задачи

1. Добавить `.elp.toml`:

   ```toml
   [eqwalizer]
   enable_all = false
   max_tasks = 4
   ```

2. Pin `eqwalizer_support` на конкретный sha/tag в `rebar.config`, не `{branch, "main"}`.

3. Синхронизировать версии: `.env` OTP 28.5 ↔ `.tool-versions` erlang 28.5.

4. Добавить `eqwalizer.modules` — список модулей для CI (version controlled).

5. Переписать Makefile:

   ```makefile
   EQW_MODULES ?= $(shell grep -v '^\#' eqwalizer.modules | tr '\n' ' ')

   eqwalizer:
   	$(REBAR) compile
   	@for m in $(EQW_MODULES); do \
   	  ERL_LIBS=$(CURDIR)/_build/default/lib elp eqwalize $$m || exit 1; \
   	done
   ```

6. Обновить `.github/workflows/static-analysis.yml`:
   - проверять модули из `eqwalizer.modules`, не `eqwalize-all`;
   - использовать exit code `elp eqwalize` вместо `grep "^[0-9]+ ERRORS"`;
   - добавить `make wc-dialyze` в тот же workflow или в `erlang-checks`.

7. Откатить из ветки `add_specs`:
   - specs в `dmt_sup`, `dmt_app`, handlers, `dmt_domain*`, `dmt_object_reference`, `dmt_json`, `dmt_db_migration`, `dmt_api_woody_utils`;
   - `eqwalizer:dynamic_cast` в reflection-коде;
   - размытые типы (`atom() => term()`, `commit_error() | dynamic()`).

8. Оставить без отката:
   - фикс `object_type() :: atom() | binary()` + guard в `just_object/6`;
   - runtime guards в `dmt_repository_handler` (`to_ref/1`, `filter_domain_objects/1`);
   - partner migration — отдельным PR, если ещё не в master.

### Критерий готовности

- `make wc-eqwalizer` с пустым `eqwalizer.modules` — no-op;
- dialyzer зелёный;
- CT зелёный.

---

## Фаза 1: Tier 1 — быстрая победа

**PR:** `feat/eqwalizer-tier1-object-author`  
**Оценка:** 1–2 дня

### Модули

| Модуль | `-typing([eqwalizer]).` | Комментарий |
|--------|--------------------------|-------------|
| `dmt_object_id` | да | Pattern match по type tag |
| `dmt_object` | да | Domain types, insert/update/remove |
| `dmt_author` | да | Thin facade |
| `dmt_author_database` | да | CRUD + SQL |

### Задачи

1. Добавить `-typing([eqwalizer]).` в каждый модуль.
2. Написать минимальные specs на exported функции (и private, если вызываются из public API).
3. Shared types — только здесь (`-export_type` в `dmt_object`, `dmt_author`).
4. Добавить 4 модуля в `eqwalizer.modules`.
5. CI: eqwalizer job проверяет только эти модули.

### Не делать

- Не трогать `dmt_object_reference`.
- `dmt_object_type` — `-eqwalizer(ignore).` если попадёт в scope.

### Критерий готовности

- `elp eqwalize dmt_object` — 0 errors;
- diff specs < 200 строк (vs ~800 в текущей ветке).

---

## Фаза 2: Tier 2a — dmt_repository

**PR:** `feat/eqwalizer-tier2-repository`  
**Оценка:** 3–5 дней

### Слайсы (порядок включения)

| # | Область | Ценность | Сложность |
|---|---------|----------|-----------|
| 2a | `get_object*`, `get_snapshot`, `get_version` | Высокая | Средняя |
| 2b | `search_objects*`, `filter_search_results` | Высокая | Низкая |
| 2c | `commit*`, `validate_*`, `commit_operation*` | Критическая | Высокая |
| 2d | `get_related_graph*`, `search_related_graph*` | Средняя | Высокая |

### Стратегия

Specs на весь модуль, но `-eqwalizer({nowarn_function, ...})` на функции ещё не готовых слайсов.
Постепенно снимать `nowarn` по мере типизации.

### Типы — конкретные, без escape hatches

```erlang
%% Хорошо
-type commit_error() ::
    {operation_error, {conflict, term()} | {invalid, term()}}
    | version_not_found
    | author_not_found
    | migration_in_progress
    | {object_update_too_old, {object_ref(), version()}}
    | {conflict, binary()}.

%% Плохо
-type commit_error() :: ... | eqwalizer:dynamic().
```

### Исправить регрессии из текущей ветки

| Место | Проблема | Fix |
|-------|----------|-----|
| `to_domain/1` | Silent skip объектов | Crash `{bad_domain_object, Obj}` или warning + metric |
| `maybe_from_string/1` | Crash на non-integer | Явный `{error, bad_token}` или soft default |
| `marshall_to_object_info/1` | Crash если `created_by = undefined` | Fail-fast в `add_created_by_to_objects` |

### Критерий готовности

- Слайсы 2a + 2b в CI;
- `commit/3` типизирован (2c) — можно отдельным sub-PR;
- `dmt_integration_tests_SUITE` зелёный.

---

## Фаза 3: Tier 2b — dmt_database (CRUD, без graph)

**PR:** `feat/eqwalizer-tier2-database`  
**Оценка:** 3–5 дней

### Включить

- `get_*` / `insert_*` / `update_*` / `search_objects` / `check_*`
- Exported types: `worker/0`, `object_map/0`, `entity_type/0`, `version/0`

### Исключить (`-eqwalizer({nowarn_function, ...})`)

- `get_related_graph*` / `filter_edges_by_nodes`
- `parse_graph_edges_result`

### Исправить до типизации

| Bug | Fix |
|-----|-----|
| `get_version/2` — `case_clause` если `CreatedBy` не binary | Catch-all → `{error, malformed_row}` |
| `filter_nodes_by_type/2` — atom vs binary | Normalize both sides |

---

## Фаза 4: Boundary modules (опционально, low ROI)

**PR:** `chore/eqwalizer-handlers-boundary`  
**Оценка:** 1 день

Handlers не типизировать полностью:

```erlang
-module(dmt_repository_handler).
-eqwalizer(ignore).

-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, woody:result()} | no_return().
handle_function(F, Args, Ctx, Opts) ->
    do_handle_function(F, eqwalizer:dynamic_cast(Args), Ctx, Opts).
```

Runtime guards (`to_ref/1`, `filter_domain_objects/1`) оставить — они для production safety,
не для eqWAlizer.

---

## CI/CD: финальная схема

```yaml
# .github/workflows/static-analysis.yml
jobs:
  dialyzer:
    run: make wc-dialyze

  eqwalizer:
    run: make wc-eqwalizer   # читает eqwalizer.modules
```

### Файл `eqwalizer.modules`

```
# Phase 1
dmt_object_id
dmt_object
dmt_author
dmt_author_database

# Phase 2a (uncomment when ready)
# dmt_repository
```

### Gate policy

- PR, затрагивающий модуль из списка → eqwalizer обязан быть зелёным.
- PR, добавляющий модуль в список → review specs + green eqwalizer.
- PR в ignore-модули → только dialyzer.

---

## Метрики прогресса

| Метрика | Сейчас (ветка) | Цель Phase 1 | Цель Phase 2 |
|---------|----------------|--------------|--------------|
| Модулей под eqWAlizer | ~20 | 4 | 5–6 |
| Строк specs | ~800 | ~150 | ~400 |
| `dynamic_cast` | ~56 | <5 | <15 |
| `-eqwalizer(ignore)` | 0 | ~8 | ~10 |
| CI time (eqwalizer) | all modules | ~4 modules, <30s | ~5 modules, <60s |

Tracking:

```bash
elp eqwalize-stats
```

---

## Roadmap PR

```
PR0  chore/eqwalizer-infra          ← .elp.toml, Makefile, CI, revert noise, pin deps
PR1  fix/object-type-binary        ← bugfix (можно до PR0)
PR2  feat/eqwalizer-tier1          ← 4 модуля
PR3  fix/repository-regressions    ← to_domain, get_version, filter_nodes
PR4  feat/eqwalizer-tier2a-read    ← dmt_repository read path
PR5  feat/eqwalizer-tier2a-commit  ← dmt_repository commit path
PR6  feat/eqwalizer-tier2b-db      ← dmt_database CRUD
PR7  migration/partner             ← если ещё не в master, отдельно
```

---

## Definition of Done

- [ ] `.elp.toml` с `enable_all = false`
- [ ] `eqwalizer.modules` — единственный source of truth для CI
- [ ] Dialyzer в CI на всём проекте
- [ ] EqWAlizer в CI только на opted-in модулях
- [ ] Tier 1 (4 модуля) — 0 errors
- [ ] `dmt_repository` read + search — 0 errors
- [ ] `dmt_repository:commit/3` — 0 errors
- [ ] Reflection-модули явно помечены `-eqwalizer(ignore).`
- [ ] Нет `eqwalizer:dynamic()` в union types «для зелёного CI»
- [ ] `eqwalizer_support` pinned на sha

---

## Справка: escape hatches eqWAlizer

| Механизм | Когда использовать |
|----------|-------------------|
| `-typing([eqwalizer]).` | Opt-in модуль |
| `-eqwalizer(ignore).` | Opt-out модуля целиком |
| `-eqwalizer({nowarn_function, F/N}).` | Функция ещё не готова / тест с намеренно broken types |
| `eqwalizer:dynamic_cast/1` | Осознанная trust boundary (woody args, application env) |
| `eqwalizer:fix_me/1` | Tech debt — надо починить позже |
| `eqwalizer:dynamic(T)` | Return type bridge (лучше минимизировать) |

Документация: [ELP eqwalizer config](https://whatsapp.github.io/erlang-language-platform/docs/get-started/configure-project/elp-toml/), [eqWAlizer FAQ](https://github.com/WhatsApp/eqwalizer/blob/main/FAQ.md).
