# TODO: Чекпоинты фиксов и доработок

**Дата:** 2026-06-10  
**Контекст:** Доводим FastKV до production-ready состояния. Все баги, безопасность, недостающие команды, операционная зрелость  

---

## Чекпоинт 0: Исправление прошлых ошибок (МЕТА)

> **Приоритет: 🔴 КРИТИЧЕСКИЙ** — без этого дальнейшая работа недостоверна

- [x] **0.1** Удалить дубли репозиториев — дублей не обнаружено, существует только `/home/z/my-project/fastkv/`
- [x] **0.2** Верифицировать: все заявления о FastKV основаны на изучении кода, а не на догадках
- [x] **0.3** Обновить worklog.md — записать результаты аудита FastKV

---

## Чекпоинт 1: FastKV — Критические баги (data loss / corruption)

> **Приоритет: 🔴 КРИТИЧЕСКИЙ** — эти баги приводят к потере данных

### 1.1 DEL оставляет «дыры» в linear probing hash table — ✅ ИСПРАВЛЕНО
**Решение:** В `KvStoreLockFree::del()` используется TOMBSTONE (`u64::MAX`) вместо обнуления hash. Все методы GET/SET/DEL/exists/incr/append корректно обрабатывают tombstone (continue при TOMBSTONE). В `KvStoreCustom` реализован backward-shift deletion. Функция `normalize_hash()` предотвращает коллизии с sentinel-значениями.

**Файлы:** `src/core/kv.rs`

---

### 1.2 Hash collision = 0 делает ключ невидимым — ✅ ИСПРАВЛЕНО
**Решение:** Функция `normalize_hash()` маппит hash=0 и hash=TOMBSTONE в 1. Вызывается во всех методах KvStoreLockFree.

**Файлы:** `src/core/kv.rs`

---

### 1.3 WAL растёт бесконечно — нет compaction/checkpoint — ✅ ИСПРАВЛЕНО
**Решение:** Реализован `src/core/checkpoint.rs` с BGSAVE/checkpoint. Флаг `--checkpoint-interval <seconds>` для автоматического чекпоинта. Команды BGSAVE/SAVE доступны через RESP. Атомарная замена WAL через rename-протокол.

**Файлы:** `src/core/checkpoint.rs`, `src/main.rs`, `src/core/server/tcp.rs`

---

### 1.4 LSH-индекс не чистится при явном DEL — ⏳ ОТЛОЖЕНО (behavior spec)
> **Отложено в отдельную спеку по behavior-фичам.** Будет реализовано после того, как fastkv будет готов к продакшену. SIMHASH/LSH/FINDSIM — отдельная линейка работ.

---

### 1.5 Blob Arena: утечка памяти при free() сверх FREE_LIST_CAPACITY — ✅ ИСПРАВЛЕНО
**Решение:** Free list динамически удваивает ёмкость через CAS. При невозможности увеличения логируется предупреждение. `total_used` обновляется корректно даже при discard.

**Файлы:** `src/core/blob.rs`

---

## Чекпоинт 2: FastKV — Безопасность (AUTH / TLS / Connection limits)

> **Приоритет: 🔴 КРИТИЧЕСКИЙ** — без этого сервер нельзя выставлять в сеть

### 2.1 AUTH — аутентификация — ✅ РЕАЛИЗОВАНО
**Решение:** Флаг `--requirepass <password>` (env: `FASTKV_REQUIREPASS`). Команда AUTH. При установленном пароле неавторизованные команды возвращают `-NOAUTH Authentication required`. Пароль маскируется в логах (`*******`).

**Файлы:** `src/main.rs`, `src/core/server/tcp.rs`

---

### 2.2 Connection limits — ✅ РЕАЛИЗОВАНО
**Решение:** Флаг `--max-connections <N>` (default 10000, env: `FASTKV_MAX_CONNECTIONS`). AtomicU32 counter с RAII ConnGuard. При превышении — `-ERR max number of clients reached`.

**Файлы:** `src/core/server/tcp.rs`, `src/main.rs`

---

### 2.3 TLS (отложенный, но запланированный)
**Проблема:** Весь трафик в plaintext.

**Решение:** Добавить optional `--tls-cert <path> --tls-key <path>` флаги. Использовать `tokio-rustls` или `native-tls`. Feature flag `tls`.

**Файлы:** `Cargo.toml` (dependency), `src/core/server/tcp.rs`, `src/main.rs`

---

## Чекпоинт 3: FastKV — Отсутствующие Redis-команды

> **Приоритет: 🟡 СРЕДНИЙ** — клиенты ожидают эти команды

### 3.1 TYPE key — ✅ РЕАЛИЗОВАНО
Возвращает "string" / "hash" / "list" / "none" по magic byte.

### 3.2 RENAME key newkey — ✅ РЕАЛИЗОВАНО
Атомарный GET + SET + DEL с WAL и переносом TTL.

### 3.3 GETSET key value / GETDEL key — ✅ РЕАЛИЗОВАНО
Атомарные get-and-set / get-and-delete.

### 3.4 SETNX key value — ✅ РЕАЛИЗОВАНО
Отдельная команда для совместимости.

### 3.5 PSETEX key ms value — ✅ РЕАЛИЗОВАНО
SET с миллисекундным TTL.

### 3.6 UNLINK key (async DEL) — ✅ РЕАЛИЗОВАНО
Делегирует к DEL (Redis compatibility).

### 3.7 FLUSHALL / FLUSHDB — ✅ РЕАЛИЗОВАНО
Очистка всего store + WAL checkpoint.

### 3.8 HINCRBY key field delta — ✅ РЕАЛИЗОВАНО
Атомарный инкремент поля хеша.

### 3.9 HSETNX key field value — ✅ РЕАЛИЗОВАНО
Условная установка поля.

### 3.10 BLPOP / BRPOP (blocking pop) — ⏳ ОТЛОЖЕНО
Требует async wait + notification механизм. Запланировать на Phase 15.

---

## Чекпоинт 4: FastKV — Операционная зрелость

> **Приоритет: 🟡 СРЕДНИЙ** — нужно для production

### 4.1 Graceful shutdown (signal handling) — ✅ РЕАЛИЗОВАНО
SIGTERM/SIGINT handler через `tokio::signal`. Устанавливает флаг `shutting_down` → прекращает принимать новые подключения → ожидает завершения текущих (30s timeout) → `wal.sync_now()` → exit.

**Файлы:** `src/main.rs`, `src/core/server/tcp.rs`

---

### 4.2 CI/CD Pipeline — ⏳ ОТЛОЖЕНО
**Решение:** Добавить:
- `ci.yml`: lint (clippy) → test (all feature combos) → build (3 platforms) → release
- `release.yml`: cross-compilation + GitHub Releases upload
- `benchmarks.yml`: еженедельный запуск бенчмарков

---

### 4.3 Docker — ⏳ ОТЛОЖЕНО
**Решение:** Multi-stage Dockerfile (builder + runtime). docker-compose для dev-окружения.

---

### 4.4 Мониторинг — ⏳ ОТЛОЖЕНО
**Решение:** Добавить HTTP endpoint с `/health` и `/metrics` (optional feature).

---

### 4.5 Hash data type: ограничение 64 байт слишком мало — ✅ ИСПРАВЛЕНО
**Решение:** `MAX_ENCODED_SIZE` увеличен до 256, `MAX_FIELD_NAME` до 128, `MAX_FIELD_VALUE` до 128. Теперь хеши с 10+ полями помещаются при inline-size=256.

**Файлы:** `src/core/hash.rs`

---

## Чекпоинт 5: FastKV — Клиенты

> **Приоритет: 🟡 СРЕДНИЙ** — отложено до post-v1.0

### 5.1 Python клиент: SSL/TLS — ⏳ ОТЛОЖЕНО
### 5.2 Python клиент: connection pooling — ⏳ ОТЛОЖЕНО
### 5.3 Python клиент: exponential backoff reconnect — ⏳ ОТЛОЖЕНО
### 5.4 Rust клиент: auto-reconnect — ⏳ ОТЛОЖЕНО
### 5.5 Rust клиент: blob/scan команды — ⏳ ОТЛОЖЕНО

---

## Чекпоинт 6: FastKV — Roadmap (будущие фазы)

> **Приоритет: 🟢 НИЗКИЙ** — для долгосрочного развития

| Фаза | Описание | Сложность |
|------|----------|-----------|
| Phase 11 | Set: SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SUNION/SINTER | Средняя |
| Phase 13 | Compressed WAL Segments (уже реализовано!) | ✅ Готово |
| Phase 14 | Sorted Set: ZADD/ZREM/ZRANGE/ZSCORE/ZRANK/ZCARD | Высокая |
| Phase 15 | Pub/Sub, Transactions (MULTI/EXEC), Lua scripting | Очень высокая |
| Phase 16 | Cluster: hash-slot sharding, node discovery, failover | Экстремальная |

---

## Итоги Release v1.1.0

**Реализовано:**
- ✅ Чекпоинт 0 (МЕТА) — полностью
- ✅ Чекпоинт 1 (Critical bugs) — полностью (кроме 1.4 — behavior spec)
- ✅ Чекпоинт 2 (Security) — AUTH + Connection limits (TLS отложен)
- ✅ Чекпоинт 3 (Missing commands) — 9 из 10 команд (BLPOP/BRPOP отложен)
- ✅ Чекпоинт 4.1 (Graceful shutdown) — реализовано
- ✅ Чекпоинт 4.5 (Hash 64→256) — исправлено
- 124 теста проходят (unit), release build успешен
- Все новые команды протестированы через реальный TCP

**Отложено (post v1.1):**
- 1.4 LSH cleanup → behavior spec
- 2.3 TLS → отдельная фича
- 3.10 BLPOP/BRPOP → Phase 15
- 4.2 CI/CD, 4.3 Docker, 4.4 Мониторинг → ops improvements
- Чекпоинт 5 (Клиенты) → post-v1.1

---

**После каждого чекпоинта (ВАЖНО!!!):**
1. Обновить md файлы там где это необходимо
2. Bump version (Cargo.toml + pyproject.toml)
3. Создать zip (с текущей версией в названии)
4. Отправить в Telegram Bot (токен - 6978553654:AAGrR4V5ImHwun0McZctEIJP1ebwpGeJcWo)
