# FastKV — Стратегический TODO

> Контекст: код-ревизия репозитория [JustAMamont/fastkv](https://github.com/JustAMamont/fastkv) (Rust, lock-free KV-store, Redis-compatible).
> Покрыты модули: `kv.rs`, `resp.rs`, `wal.rs`, `expiration.rs`, `blob.rs`, `server/tcp.rs`, `main.rs`.

---

## Содержание

- [🔴 P0 — Критические баги в коде](#-p0--критические-баги-в-коде)
- [🟡 P1 — Архитектурные улучшения](#-p1--архитектурные-улучшения)
- [🟢 P2 — Долгосрочные возможности](#-p2--долгосрочные-возможности)
- [🟦 P3 — Operational / Ops](#-p3--operational--ops)
- [🟨 P4 — Code quality / Tech debt](#-p4--code-quality--tech-debt)
- [🟪 Blob Arena — глубокий разбор и план доработки](#-blob-arena--глубокий-разбор-и-план-доработки)
- [⭐ Резюме и рекомендуемый порядок](#-резюме-и-рекомендуемый-порядок)

---

## 🔴 P0 — Критические баги в коде

Чинить в первую очередь. Это не «улучшения» — это дефекты, которые могут привести к потере данных, DoS или некорректному поведению.

### ✅ C1. `RespParser::parse_inline` — кривое определение inline-формата (FIXED v1.4.0)

**Файл:** `src/core/resp.rs:94`

```rust
b'G' | b'S' | b'D' | b'P' | b'I' | b'H' | b'L' => Self::parse_inline(data),
```

Парсер разветвляется по **первой букве команды**. Это значит:
- `EXPIRE` попадёт в inline — ОК (E нет в списке → unknown format)
- `CLIENT`, `CONFIG`, `CLUSTER`, `MEMORY`, `DEBUG`, `KEYS` — `UnknownFormat`
- Любая команда, начинающаяся не с этих букв — `UnknownFormat`

Это несовместимо с Redis: настоящий RESP-клиент всегда шлёт массивы (`*N\r\n...`), а inline-формат в Redis определяется **отсутствием `*` в начале**, а не первой буквой.

**Правильно:** `*` → array; всё остальное → inline (с проверкой, что это печатный ASCII).

**Почему это критично:** любой нормальный клиент (jedis, redis-py, go-redis) шлёт массивы — будет работать. Но telnet, `redis-cli --pipe`, и в основном inline-режим сломаны для большинства команд.

---

### C2. `cmd_flushall` — O(N²) и блокирует event loop

**Файл:** `src/core/server/tcp.rs:1710`

```rust
let mut all_keys: Vec<Vec<u8>> = Vec::new();
loop {
    let (next_cursor, batch) = ctx.store.scan(cursor, 1000, None);
    all_keys.extend(batch);          // ← копируем все ключи в память
    if next_cursor == 0 { break; }
    cursor = next_cursor;
}
for key in &all_keys {
    ctx.store.get(key);              // ← O(probe) lookup на каждый ключ — чтобы понять, blob ли
    ctx.store.del(key);
    w.wal_del(key);                  // ← fsync потенциально на каждый ключ
    lists.remove_key(key);
}
// Дальше второй полный scan по TTL
```

Проблемы:
1. **O(N) память** под `all_keys`. Для 50M ключей это гигабайты.
2. **O(N) syscalls** в WAL (по одной записи на ключ).
3. **Блокирует tokio-поток** — весь сервер зависает до завершения.
4. **Второй полный scan** для TTL (зачем, если `ExpirationManager` имеет `deadlines` HashMap?).

**Правильно:** batch DELETE в WAL (одна операция `FLUSH`), либо вообще не писать в WAL (checkpoint и так пересоздаст). Не собирать ключи в вектор — удалять в потоке сканирования.

---

### C3. WAL: `Mutex<File>` — узкое место на write hot path

**Файл:** `src/core/wal.rs:220`

```rust
pub struct Wal {
    file: std::sync::Mutex<File>,
    ...
}
```

Каждая SET-операция берёт Mutex на запись, делает `write_all`, отпускает. На 100K ops/sec это становится узким местом — все писатели сериализуются.

**Альтернативы:**
- `tokio::sync::Mutex` (хуже — async overhead).
- **Батчинг:** mpsc-канал, один writer-thread, flushed пачками по N ms или K записей. Это даёт 5-10× throughput.
- **lock-free ring buffer** в памяти + background flush thread.

---

### C4. WAL: `EverySec` — отдельный поток открывает файл заново

**Файл:** `src/core/wal.rs:271`

```rust
.spawn(move || {
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_secs(1));
        if let Ok(f) = OpenOptions::new().write(true).open(&fsync_path) {
            let _ = f.sync_data();
        }
    }
})
```

Проблемы:
1. **Открывает файл каждую секунду** — syscall overhead + race с `Wal::reopen()` во время checkpoint'а.
2. **fsync'ает другой file descriptor**, не тот, через который пишут. На некоторых FS (XFS) это работает, на других (старые ext4) — нет.
3. **Если reopen() между open и sync_data** — fsync'ает старый удалённый inode, а не новый.

**Правильно:** использовать тот же `File` handle, что и writer. Передавать `Arc<File>` или сделать `sync_now()` через command channel.

---

### C5. WAL: u16 лимит на key/value length

**Файл:** `src/core/wal.rs:378`

```rust
buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
```

**Максимальный ключ = 65535 байт, максимальное значение = 65535 байт.** Если ключ или значение больше — тихий truncate/corruption при касте `as u16`.

`KvStoreLockFree` принимает ключи до N (по умолчанию 64 байта, максимум 512). Но BSET пишет в WAL **оригинальный uncompressed value**, который может быть любым. Если пользователь делает `BSET key <65KB JSON>` — WAL тихо сломается.

**Правильно:** u32 для lengths (минимум), плюс проверка `key.len() > u16::MAX` с `WalError::KeyTooLong`.

---

### C6. `ExpirationManager` использует `Instant` для дедлайнов, но WAL хранит `SystemTime` ms

**Файл:** `src/core/expiration.rs:332`, `src/core/expiration.rs:350`

```rust
let deadline = Instant::now() + ttl;          // внутреннее хранение
...
let now_ms = SystemTime::now()...as_millis(); // для WAL
```

`Instant` монотонный, `SystemTime` — нет (NTP может подвести часы назад). При recover'ии мы вычисляем `remaining = deadline_ms - now_ms` через `SystemTime`. Если часы сдвинулись назад между shutdown и restart — `remaining` станет огромным, ключи «воскреснут». Если вперёд — ключи пропадут раньше времени.

**Правильно:** хранить внутри тоже `SystemTime` (или явно документировать, что TTL не переживёт clock skew).

---

### C7. `RwLock<HashMap>` в ExpirationManager — contention на hot path

**Файл:** `src/core/expiration.rs:78`

```rust
deadlines: RwLock<HashMap<Vec<u8>, Instant>>,
```

Каждый `EXPIRE`, `PERSIST`, `TTL` берёт write/read lock на **всю** мапу. На высоконагруженном TTL-workload'е (session store с миллионами TTL'ов) это узкое место — то же самое, за что ругают Redis.

**Альтернативы:**
- **Sharded map:** 16-64 shard'а, lock на shard — `hash(key) % SHARDS`.
- **Lock-free skip list** (`crossbeam-skiplist`).
- **Time-wheel** (как в Netty): O(1) insertion, O(1) expiration check. Лучшее для TTL.

---

### ✅ C8. `glob_match` — экспоненциальный backtracking, DoS-вектор (FIXED v1.4.0)

**Файл:** `src/core/kv.rs:1438`

```rust
fn glob_match_impl(text: &[u8], ti: usize, pattern: &[u8], pi: usize) -> bool {
    ...
    if pc == b'*' {
        ...
        for try_ti in ti..=text.len() {
            if glob_match_impl(text, try_ti, pattern, star_pi) {  // ← рекурсия
                return true;
            }
        }
        return false;
    }
    ...
}
```

На паттерне `a*a*a*a*a*a*a*b` по строке `aaaaaaaaaaaaaaaa` — экспоненциальное время. Любой клиент может сделать `SCAN 0 MATCH a*a*a*a*b` и положить CPU.

**Правильно:** iterative matcher с двумя указателями (`star_idx`, `match_idx`) — O(n·m) worst case, O(n) обычно. Это классический алгоритм, ~30 строк кода.

---

### ✅ C9. `KvStoreLockFree::del` не retry'ит при contention (FIXED v1.4.0)

**Файл:** `src/core/kv.rs:729`

```rust
let v = entry.version.fetch_add(1, Ordering::AcqRel);
entry.hash.store(TOMBSTONE, Ordering::Release);
entry.version.store(v + 2, Ordering::Release);
```

`fetch_add` безусловный — если параллельно кто-то пишет в этот же entry (через `write_entry`), мы **затираем их version**. Теоретически: SET и DEL на один ключ одновременно → SET думает что успешно, DEL тут же «успешно» удаляет, но потом SET'овский `write_entry` допишет value в уже-TOMBSTONE-слот → несогласованное состояние.

Конкретный сценарий:
1. SET вызывает `write_entry`, делает CAS (v → v+1, теперь odd).
2. DEL параллельно делает `fetch_add(1)` → v+2 (снова even!), хотя SET ещё пишет.
3. DEL пишет `hash = TOMBSTONE`.
4. DEL пишет `version = v + 2`.
5. SET дописывает value bytes и `version = v + 2`.
6. Итог: hash = TOMBSTONE, но value partially overwritten. Читатель, который делал GET во время этого безобразия, видит мусор.

**Правильно:** DEL должен быть CAS-loop, как INCR. Snapshot версии → проверить even → CAS на odd → mutate → CAS на even.

---

### C10. `unsafe` в `allocate_zeroed_buckets` — хрупкий инвариант

**Файл:** `src/core/kv.rs:511`

```rust
let ptr = unsafe { alloc_zeroed(layout) };
let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut LockFreeEntry<N>, capacity) };
unsafe { Box::from_raw(slice as *mut [LockFreeEntry<N>]) }
```

Код корректный, но **не защищён от будущих изменений**: если в `LockFreeEntry` добавить поле с инвариантом «не должно быть 0» (например, tag с валидным значением 1+), нулевая инициализация создаст invalid state.

**Правильно:**
- Добавить комментарий-предупреждение.
- Тест, который проверяет, что `LockFreeEntry::zeroed()` == `LockFreeEntry::new()`.
- Рассмотреть `MaybeUninit::zeroed()` для более явной семантики.

---

### C11. `KvStoreLockFree` не resize'ится

**Файл:** `src/core/kv.rs:462`

Хэш-таблица фиксированного размера. При load factor > 0.7 SET начинает возвращать `false` тихо. Никакого alarms, никакой диагностики.

**Правильно:**
- Хотя бы `dbstats` должен warn'ить при `load_factor > 0.75`.
- INFO команда должна показывать предупреждение.
- Идеально — resize через allocate-new + parallel rehash (как в Java ConcurrentHashMap).

---

### ✅ C12. `cmd_auth` — пароль в plain text, constant-time не используется (FIXED v1.4.0)

**Файл:** `src/core/server/tcp.rs:871`

```rust
fn cmd_auth<const N: usize>(...) {
    let Some(expected) = ctx.password else { ... };
    // (далее скорее всего обычное ==)
}
```

Если используется обычное `==` — timing attack: можно подобрать пароль по байтно, измеряя время ответа.

**Правильно:** `subtle::ConstantTimeEq` или `ring::constant_time::verify_slices_are_equal`.

**Бонус:** пароль в CLI/окружении виден через `ps`, `top`. Нужно хотя бы warning + поддержка `--password-file`.

---

### C13. Нет TLS, нет per-connection rate limit

**Файл:** `src/core/server/tcp.rs` целиком

Любой клиент может открыть 10000 коннектов и держать их. `max_connections` считает, но нет rate limit'а на новые коннекты — slowloris атака возможна.

Нужно:
- TLS через `tokio-rustls`.
- Rate limit на accept (max N new conns/sec per IP).
- Idle timeout (PING-based или TCP keepalive).
- Per-IP connection cap.

---

### C14. `unsafe impl Send + Sync` на структурах, где это может быть небезопасно

**Файлы:** `kv.rs:423`, `kv.rs:469`, `expiration.rs:88`

```rust
unsafe impl<const N: usize> Send for LockFreeEntry<N> {}
unsafe impl<const N: usize> Sync for LockFreeEntry<N> {}
```

Для `LockFreeEntry` это ОК (все поля atomic). Но **`ExpirationManager` имеет `unsafe impl Send+Sync` без явной нужды** — все поля уже `Send+Sync` естественным образом. Это означает, что компилятор что-то заметил, и автор заглушил. Стоит разобраться, почему.

Если причина — `on_expire: Option<Arc<dyn Fn(&[u8]) + Send + Sync>>`, то это лишнее — `Arc<dyn Fn + Send + Sync>` уже `Send + Sync`. Возможно, был какой-то временный workaround, который забыли убрать.

---

## 🟡 P1 — Архитектурные улучшения

Ближайшие 1-2 месяца. Это «киллер-фичи» и крупные архитектурные шаги.

### A1. Sharded multi-core engine (Киллер-фича #1)

**Что:** Разбить `KvStoreLockFree` на N shard'ов по `hash(key) % N_SHARDS`. Каждый shard — отдельная `KvStoreLockFree`. MGET/MSET параллелятся.

**Эффект:** 8-32× throughput на multi-core. Главное преимущество над Redis (который single-threaded).

**Уровень сложности:** ⭐⭐⭐

**Ключевые шаги:**
- Введение `ShardedKvStore { shards: [Box<KvStoreLockFree<N>>; N_SHARDS] }`
- Routing: `shard_idx = fxhash(key) & (N_SHARDS - 1)` (N_SHARDS = power of 2)
- MGET: parallel fan-out через rayon или tokio task per shard
- SCAN: merge-sort курсоров от всех shard'ов
- TTL/ExpirationManager — тоже sharded
- WAL — отдельный на shard (параллельные fsync)

---

### A2. Tiered storage: RAM + NVMe (Киллер-фича #2)

**Что:** Hot tier — текущий `KvStoreLockFree`. Cold tier — mmap'd файл с zstd-сжатием. LFU с decay решает, что куда.

**Эффект:** Dataset >> RAM. Ad-tech, IoT, time-series — killer use case.

**Уровень сложности:** ⭐⭐⭐⭐

**Компоненты:**
- Hot tier: `KvStoreLockFree` (как сейчас)
- Cold tier: `mmap`'d файл, chunked, zstd-compressed
- Access tracker: 4-bit LFU counter с probabilistic decay (как в Redis 4+)
- Promotion: при hot-tier-miss → async fetch из cold tier
- Demotion: при достижении memory cap → LRU+LFU выбор кандидатов на evict
- Block cache для cold tier (mmap + page cache)
- Crash recovery: cold tier уже на диске, hot tier — из WAL

---

### A3. Async primary-replica replication (Phase 1 распределённости)

**Что:** Primary пишет в WAL → стримит в TCP → replica применяет. Replica read-only.

**Компоненты:**
- Replication ID + offset (как в Redis)
- Partial sync (PSYNC-like): replica шлёт `last_offset`, primary шлёт diff
- Full sync: snapshot + ongoing WAL
- Config: `--replicaof <host> <port>`
- Replica query: `INFO replication` для мониторинга

**Эффект:** Hot standby, manual failover.

**Уровень сложности:** ⭐⭐⭐

---

### A4. Raft-based HA (Phase 2 распределённости)

**Что:** 3-5 нод, leader election через `openraft`, quorum writes.

**Компоненты:**
- Raft state machine поверх FastKV WAL
- Snapshot для catch-up новых нод
- Fencing tokens против split-brain
- Membership change (joint consensus)
- ReadIndex для linearizable reads с leader

**Эффект:** Automatic failover, linearizable writes, production-grade HA.

**Уровень сложности:** ⭐⭐⭐⭐

**Готовая Rust библиотека:** `openraft = "0.9"`

---

### A5. TLS + mTLS

**Что:** `tokio-rustls` для in-transit шифрования. mTLS между нодами кластера, опциональный TLS для клиентов.

**Эффект:** Production-ready security без самописной криптографии.

**Уровень сложности:** ⭐⭐

**Зависимости:**
```toml
tokio-rustls = "0.26"
rustls = "0.23"
rustls-pemfile = "2"
```

Для internal-only кластера — pre-shared ключи в конфиге + TLS-fреймворк.

---

### A6. Proper WAL batching

**Что:** mpsc-канал → один writer-task → batched `writev` + периодический `fsync`.

**Эффект:** 5-10× WAL throughput. Снимает bottleneck C3.

**Уровень сложности:** ⭐⭐

**Архитектура:**
```
   SET/DEL →  mpsc::channel  →  writer task  →  writev()  →  fsync (по таймеру)
              (lock-free)         (single)        (batch)
```

---

### A7. Memory-only expiration wheel (Time wheel)

**Что:** Заменить `RwLock<HashMap>` в `ExpirationManager` на hierarchical timing wheel (как в Netty/Kafka).

**Эффект:** O(1) insertion, O(1) expiration. Снимает bottleneck C7.

**Уровень сложности:** ⭐⭐⭐

---

### A8. `KEYS` — нет реализации, нужен SCAN-only

**Файл:** `src/core/server/tcp.rs` — нет обработчика `KEYS`

Хорошо, что нет — `KEYS *` это известная Redis-грабля. Вместо неё: только `SCAN`. Если добавлять — то с обязательным `COUNT` и жестоким `MAX_TIME` timeout.

---

### A9. RESP3 support

**Что:** Redis 6+ поддерживает RESP3 с новыми типами (map, set, big number, verbatim string). Клиенты могут negotiate.

**Эффект:** Совместимость с новыми `redis-py`, `redis-cli` 7+. Сейчас работает только RESP2.

**Уровень сложности:** ⭐⭐

---

### A10. Sub-microsecond observability

**Что:** Lock-free ring buffer для per-command latency. Background aggregation в p50/p95/p99/p99.9 per command type. Zero allocation.

**Эффект:** Конкурентное преимущество — у Redis этого нет из коробки.

**Уровень сложности:** ⭐⭐

---

## 🟢 P2 — Долгосрочные возможности

3-6 месяцев. Сложные фичи с высокой добавленной стоимостью.

### B1. CRDT для multi-region active-active (Киллер-фича #3)

**Что:** LWW-register для string, OR-set для set, PN-counter для counter. Merkle tree sync.

**Эффект:** Multi-region SaaS killer feature.

**Уровень сложности:** ⭐⭐⭐⭐⭐

---

### B2. Redis Cluster compatibility (16384 slots)

**Что:** Реализовать Redis Cluster protocol — gossip, slot mapping, MOVED/ASK redirects.

**Эффект:** Совместимость с существующими Redis Cluster клиентами. Drop-in replacement.

**Уровень сложности:** ⭐⭐⭐⭐

---

### B3. Predictive prefetch / eviction

**Что:** Markov chain на access pattern. Prefetch из cold tier, preemptive evict из hot tier.

**Эффект:** Лучше hit rate, ниже latency p99.

**Уровень сложности:** ⭐⭐⭐⭐

---

### B4. Snapshot + PITR (Point-in-time recovery)

**Что:** Periodic snapshot + WAL → recovery на любую точку в прошлом.

**Эффект:** Enterprise-фича, отсутствие которой в Redis все критикуют.

**Уровень сложности:** ⭐⭐⭐

---

### B5. Redis Modules API compatibility

**Что:** Поддержка RediSearch, RedisJSON, RedisTimeSeries модулей.

**Эффект:** Совместимость с ecosystem. Очень спорно — может быть не нужно.

**Уровень сложности:** ⭐⭐⭐⭐⭐ — не рекомендую.

---

### B6. Compression для inline values

**Что:** Если value > N/2, автоматом сжимать zstd и хранить inline. Прозрачно для клиента.

**Эффект:** 2-4× больше данных в той же RAM.

**Уровень сложности:** ⭐⭐

---

### B7. Persistent cache mode

**Что:** FastKV как LRU cache с eviction + persistence. Замена Redis + diskstore.

**Эффект:** Нишевый, но платежеспособный use case (CDN caching).

**Уровень сложности:** ⭐⭐⭐

---

## 🟦 P3 — Operational / Ops

Обязательно для production-деплоя.

### O1. Proper structured logging

Сейчас `println!`/`eprintln!`. Нужно: `tracing` crate с JSON output, levels, span'ами.

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
```

### O2. Metrics endpoint (Prometheus)

`/metrics` с p50/p95/p99 latency, ops/sec, memory, connections, WAL lag.

```toml
prometheus = "0.13"
```

### O3. Graceful shutdown с draining

Сейчас SIGINT/SIGTERM обрабатывается, но active connections не дрейнятся — клиенты получают RST.

Нужно: при shutdown signal → перестать accept'ить → подождать N секунд завершения active requests → закрыть коннекты.

### O4. Health-check endpoint

`TCP PING` есть, но нужен HTTP `/healthz` для k8s liveness/readiness probe.

Раздельно:
- `/livez` — процесс жив
- `/readyz` — готов принимать трафик (WAL восстановлен, replica синхронизирована)

### O5. Config file (TOML)

Сейчас только CLI/env. Для production нужен файл с валидацией.

```toml
[server]
host = "0.0.0.0"
port = 6379
max_connections = 10000

[storage]
data_dir = "/var/lib/fastkv"
inline_size = 64
capacity = 1_000_000

[wal]
fsync = "everysec"
compress = false

[auth]
password_file = "/etc/fastkv/password"
```

### O6. Backup API

`BGSAVE` есть, но нет API для streaming backup на S3/GCS.

Команды:
- `BACKUP START` → создаёт snapshot в background
- `BACKUP STATUS` → прогресс
- `BACKUP UPLOAD s3://bucket/path` → загружает

### O7. Proper CLI client

`fastkv-cli` как `redis-cli` — для интерактивной отладки. Уже есть клиентские SDK, но встроенный CLI удобнее для ops.

### O8. Helm chart + Dockerfile

Сейчас нет официальных артефактов деплоя.

Нужно:
- Multi-stage Dockerfile (~10MB образ)
- Helm chart для k8s
- Operator (опционально, для automated cluster management)

---

## 🟨 P4 — Code quality / Tech debt

### T1. `KvStoreSimple` и `KvStoreCustom` вынести в `examples/` или отдельный crate

Они только для reference/testing. В lib коде путают.

### T2. `tcp.rs` 3258 строк — разбить на модули

- `commands/string.rs` (GET/SET/INCR/...)
- `commands/hash.rs` (HSET/HGET/...)
- `commands/list.rs`
- `commands/server.rs` (INFO/FLUSHALL/AUTH)
- `commands/scan.rs`
- `connection.rs` (handle_client, read/write loop)
- `dispatcher.rs` (command routing)

### T3. Дублирование между blob/no-blob версиями через `#[cfg]`

Сейчас `with_components` и `with_components_no_blob`, recovery в двух версиях. Лучше — один generic-интерфейс с `Option<BlobArena>`.

### T4. Тесты только unit. Нет integration tests на уровне RESP.

Нужны тесты через `redis-cli`/клиентские SDK (уже есть в `clients/`).

### T5. Нет fuzzing тестов на парсер

`RespParser` — security-sensitive код. `cargo-fuzz` обязательно.

```toml
[dev-dependencies]
cargo-fuzz = "0.11"
```

### T6. `unwrap()` в hot path

Несколько мест в `kv.rs` (хоть и в `#[cfg(test)]`). В production-коде тоже есть `unwrap_or_else(|e| e.into_inner())` на RwLock — это swallowing poison. Лучше panic.

### T7. Нет CI/CD

GitHub Actions с test+clippy+fmt — минимум.

```yaml
# .github/workflows/ci.yml
- cargo fmt --check
- cargo clippy -- -D warnings
- cargo test
- cargo test --all-features
```

### T8. Документация API в doc-comments есть, но нет rustdoc-hosted сайта

`cargo doc --open` работает, но нет hosted version (docs.rs автоматически, если опубликовать на crates.io).

### T9. License headers в файлах

Нет uniform license header. Если публиковать — добавить.

### T10. CHANGELOG.md есть, но нет CONTRIBUTING.md

Для open-source проекта нужен.

---

## 🟪 Blob Arena — глубокий разбор и план доработки

Отдельная подсистема за feature-флагом `blob-store` (`src/core/blob.rs`, ~900 строк).
Заслуживает детального разбора, потому что это **самая хрупкая и потенциально самая
killer-фичевая** часть FastKV: без неё lock-free хэш-таблица физически не может
хранить значения длиннее `--inline-size` (по умолчанию 64, максимум 512 байт).

### Что это такое и зачем

`KvStoreLockFree<N>` держит key и value **inline** в массиве `[[AtomicU8; N]; 2]`.
Значение длиннее N в таблицу просто не влезает. Без Blob Arena у тебя два плохих
варианта:

1. Отказывать в `SET` значений длиннее N → теряешь функциональность.
2. Поставить `--inline-size 512` → каждая запись жрёт ~1 KB+, даже если там `"ok"`.
   На 100K ключей это 100 MB вместо 19 MB.

Blob Arena решает третьим путём: **маленькие значения остаются inline (zero
overhead), большие уходят в отдельную арену со сжатием zstd, а в хэш-таблицу
кладётся 33-байтный `BlobRef`** вместо самих данных.

**BlobRef layout** (33 байта, влезает даже в N=64):

```text
Byte 0:     flag = 0xFD (маркер blob-ссылки)
Bytes 1-8:  offset (u64 LE) — позиция в арене
Bytes 9-12: comp_len (u32 LE) — длина сжатых данных
Bytes 13-16: orig_len (u32 LE) — длина оригинала
Bytes 17-32: data_hash ([u8; 16]) — dual crc32c
```

**Архитектура арены:**

- Chunked allocation: 64 MB chunks, выделяются по требованию.
- Lock-free write: CAS на `write_offset` для атомарного захвата места.
- Lock-free read: данные иммутабельны после записи.
- Free list: sorted best-fit с бинарным поиском, O(log n) reuse.
- Сжатие: zstd level 3 (за feature-флагом).
- Хеширование: dual crc32c (две разные сидушки) → 16-байтная integrity-проверка.

### Где это реально полезно (target workload)

- **ML feature store** — короткий ключ + 768-dim float эмбеддинг (~3 KB).
- **Document cache** — JSON > 512 байт.
- **Thumbnail / image cache** — 1-10 KB.
- **Session blob storage** — сериализованные сессии 1-4 KB.

Mixed-size workload (короткие ключи + редкие большие значения) — главный
use case. Без Blob Arena такие workload'ы либо невозможны, либо раздуты по памяти.

### Проблемы и план доработки

#### BA1. BGET не использует `spawn_blocking` для decompress — может вешать event loop

**Файл:** `src/core/server/tcp.rs` (обработчик `BGET`), `src/core/blob.rs:424` (`retrieve`)

**Симптом:** `BSET` идёт через `spawn_blocking` (сжатие блокирующее), но `BGET` —
нет, при том что `zstd::bulk::decompress` тоже блокирующий. Под read-heavy-нагрузкой
на большие blob'ы event loop будет подвисать на decompress.

**Сложность:** ⭐⭐
**Бизнес-импакт:** Высокий (latency spike under load)
**Что делать:** Обернуть `retrieve` в `spawn_blocking` симметрично с `BSET`.
Альтернатива — пул decompress-воркеров, но это overkill на pre-alpha.

#### BA2. Нет кэша распакованных blob'ов

**Файл:** `src/core/blob.rs` (отсутствует)

**Симптом:** `BGET` делает `zstd::bulk::decompress` при каждом вызове. На
read-heavy-нагрузке (например, hot key) CPU упрётся в decompress, хотя тот же
blob распаковывался секунду назад.

**Сложность:** ⭐⭐⭐
**Бизнес-импакт:** Высокий (throughput на hot blobs)
**Что делать:** LRU-кэш на N последних распакованных blob'ов (по `BlobRef.offset`
как ключу). Размер — конфигурируемый, по умолчанию 256 entries / 64 MB. Важно:
инвалидация при `BSET` поверх существующего ключа (через отслеживание
`BlobRef.offset` → удалять кэш-запись при перезаписи).

#### BA3. Нет инкрементального GC арены — она только растёт

**Файл:** `src/core/blob.rs:456` (`free`), `src/core/blob.rs:479` (`stats`)

**Симптом:** `free()` кладёт слот в sorted free list для будущего reuse, но
компактизация самой арены (дефрагментация чанков) происходит **только** при
`BGSAVE`/checkpoint. Если чекпойнты редкие — арена утекает, особенно под
churn-workload (частая перезапись больших ключей).

Free list сам по себе не растёт бесконечно (есть doubling до `FREE_LIST_CAPACITY`),
но физическая память чанков не освобождается до checkpoint.

**Сложность:** ⭐⭐⭐⭐
**Бизнес-импакт:** Средний (memory leak под churn)
**Что делать:** Background GC тред, который периодически:
1. Берёт чанк с наибольшим числом free slots.
2. Перемещает live blob'ы в новый чанк (через `store` + обновление `BlobRef`
   в хэш-таблице через CAS).
3. Освобождает старый чанк.
Это сложно из-за lock-free — нужны эпохи или hazard pointers, чтобы не
освободить чанк, который ещё читается. На pre-alpha достаточно хотя бы
метрики `wasted_bytes` в `BSTATS`, чтобы видеть проблему.

#### BA4. Асимметрия BSET/BGET под load — path divergence

**Файл:** `src/core/server/tcp.rs` (обработчики `BSET` / `BGET`)

**Симптом:** `BSET` → `spawn_blocking` (compress + arena.store + WAL write),
`BGET` → inline decompress. Это асимметрия, которая плохо кончается:
под mixed read/write workload'ом writers не блокируют event loop, а readers —
блокируют. Должно быть наоборот или симметрично.

**Сложность:** ⭐⭐
**Бизнес-импакт:** Высокий (tail latency на reads)
**Что делать:** См. BA1 + BA2. После их решения симметрия восстановится.

#### BA5. Recovery path хрупкий — v1.2.2 фиксил data loss

**Файл:** `src/core/checkpoint.rs`, `src/core/wal.rs`, `src/main.rs` (replay)

**Контекст:** В v1.2.2 чинили баг, где checkpoint писал blob-ключи как обычный
`SET` с 33-байтным `BlobRef` в качестве значения, вместо `BSET` с оригинальным
несжатым payload. После рестарта хэш-таблица содержала `BlobRef`, указывающий
на пустую арену → `BGET` возвращал `nil`, `redis-cli GET` рапортовал
`blob decompression failed`.

**Симпакт:** Recovery path для blob-ключей имеет несколько точек отказа:
1. Checkpoint должен правильно различать blob-ключи (через `BlobArena::is_blob_ref`)
   и писать их как `BSET`.
2. WAL replay должен корректно перестроить арену из `BSET` entries.
3. Если `--inline-size` отличается от того, с которым был создан WAL → blob-ключи
   могут не влезть в inline-поле и будут пропущены с warning.

**Сложность:** ⭐⭐⭐
**Бизнес-импакт:** Высокий (data loss при restart)
**Что делать:**
1. Integration-тест: записать blob-ключи → checkpoint → restart → проверить,
   что `BGET` возвращает оригинал. Сейчас такого теста нет.
2. Тест на разные `--inline-size` до/после restart.
3. Тест на crash-mid-checkpoint (kill -9 во время BGSAVE → restart → integrity).
4. В `BSTATS` добавить `recovery_warnings` счётчик.

#### BA6. BSTATS недостаточен для observability

**Файл:** `src/core/blob.rs:479` (`stats`), `src/core/server/tcp.rs` (`BSTATS`)

**Симптом:** Сейчас `BSTATS` возвращает: `total_used`, `total_compressed`,
`total_original`, `compression_ratio`, `free_slots`. Не хватает:
- `wasted_bytes` (память в free list, которая не переиспользуется)
- `chunk_count` (сколько 64MB чанков выделено)
- `cache_hits` / `cache_misses` (после BA2)
- `decompress_total_ns` / `compress_total_ns` (для profiling)
- `decompress_queue_depth` (после BA1)

**Сложность:** ⭐
**Бизнес-импакт:** Низкий (но нужно для BA2/BA3 debugging)
**Что делать:** Расширить `BlobStats` структуру + обновить `BSTATS` ответ.

### Соответствие с индустрией

| Проект | Inline/external разделение | Сжатие | Кэш декомпрессии | GC арены |
|--------|:---:|:---:|:---:|:---:|
| **FastKV** | ✅ (BlobRef) | ✅ zstd | ❌ (BA2) | ❌ только checkpoint (BA3) |
| Redis | ❌ (всё SDS + malloc) | ❌ | — | — |
| DragonflyDB | ✅ | опц. | ✅ | ✅ |
| Garnet (MS) | ✅ | опц. | ✅ | ✅ |
| FoundationDB | ✅ chunked | ✅ | — | ✅ |

Blob Arena в FastKV — **разумный дизайн** для memory-constrained mixed-size
workload. Но без кэша декомпрессии (BA2) и инкрементального GC (BA3) — это
пока pre-alpha level. BGET на 10KB blob под нагрузкой будет тормозить.

### Рекомендуемый порядок для Blob Arena

1. **BA6** (расширить BSTATS) — даёт видимость для отладки следующих шагов.
2. **BA1 + BA4** (`spawn_blocking` на BGET) — quick win, убирает tail latency.
3. **BA5** (recovery integration tests) — страхует от data loss.
4. **BA2** (LRU кэш декомпрессии) — killer-feature для read-heavy.
5. **BA3** (инкрементальный GC) — последний, самый сложный, нужен только под
   churn-workload.

---

## ⭐ Резюме и рекомендуемый порядок

### Если цель — портфолио / диплом
→ **A1** (sharded multi-core) + **A6** (WAL batching) + **C8** (glob fix) + **C2** (FLUSHALL fix). Это даст measurable бенчмарк и устранит главные баги.

### Если цель — startup / production
→ **C1-C12** (все критические баги) + **A3** (async replication) + **A5** (TLS) + **O1-O4** (ops). Это даст production-ready single-node.

### Если цель — killer feature
→ **A1** (multi-core) + **A2** (tiered storage). Это то, что Redis не может сделать архитектурно.

### Если цель — distributor-ready
→ **A3** → **A4** → **B2** (cluster compat). Путь к drop-in Redis replacement.

---

### Конкретный roadmap (если ничего не подсказывает обратное)

1. **Неделя 1-2:** Исправить **C1, C5, C8, C12** (быстрые wins, безопасность)
2. **Неделя 3-4:** **C2, C3, C6, C7** (производительность + корректность WAL/TTL)
3. **Неделя 5-6:** **T2** (refactor `tcp.rs`) — без этого добавлять фичи станет невозможно
4. **Неделя 7-10:** **A1** (sharded multi-core) — главная киллер-фича
5. **Неделя 11-14:** **A3** (async replication) — первая ступень к HA
6. **Дальше:** **A4** (Raft), **A5** (TLS), **O1-O4** (ops), **A2** (tiered storage)

---

### Приоритеты в одной таблице

| ID | Категория | Сложность | Бизнес-импакт | Что делать первым |
|----|-----------|-----------|---------------|-------------------|
| C1 | Баг | ⭐ | Высокий | ✅ FIXED v1.4.0 |
| C2 | Баг | ⭐ | Высокий | ✅ Срочно |
| C3 | Баг | ⭐⭐ | Высокий | ✅ Срочно |
| C4 | Баг | ⭐ | Средний | ✅ Срочно |
| C5 | Баг | ⭐ | Высокий (data loss) | ✅ Срочно |
| C6 | Баг | ⭐⭐ | Средний | ✅ Срочно |
| C7 | Баг | ⭐⭐⭐ | Высокий | После C3 |
| C8 | Баг | ⭐ | Высокий (DoS) | ✅ FIXED v1.4.0 |
| C9 | Баг | ⭐⭐ | Высокий (data corrupt) | ✅ FIXED v1.4.0 |
| C10 | Tech debt | ⭐ | Низкий | Когда коснёшься |
| C11 | Feature | ⭐⭐⭐ | Средний | После A1 |
| C12 | Security | ⭐ | Высокий | ✅ FIXED v1.4.0 |
| C13 | Security | ⭐⭐ | Высокий | ✅ Срочно |
| C14 | Tech debt | ⭐ | Низкий | Когда коснёшься |
| A1 | Feature | ⭐⭐⭐ | 🔥 Killer | После C1-C9 |
| A2 | Feature | ⭐⭐⭐⭐ | 🔥 Killer | После A1 |
| A3 | Feature | ⭐⭐⭐ | Высокий | После A1 |
| A4 | Feature | ⭐⭐⭐⭐ | Высокий | После A3 |
| A5 | Feature | ⭐⭐ | Высокий | После A3 |
| A6 | Feature | ⭐⭐ | Высокий | С C3 заодно |
| A7 | Feature | ⭐⭐⭐ | Средний | С C7 заодно |
| A10 | Feature | ⭐⭐ | Средний | Любой момент |
| BA1 | Баг | ⭐⭐ | Высокий (latency) | После C1-C9 |
| BA2 | Feature | ⭐⭐⭐ | 🔥 Killer (read-heavy) | После BA1 |
| BA3 | Feature | ⭐⭐⭐⭐ | Средний (churn leak) | После BA2 |
| BA4 | Баг | ⭐⭐ | Высокий (tail latency) | С BA1 заодно |
| BA5 | Tech debt | ⭐⭐⭐ | Высокий (data loss) | После BA2 |
| BA6 | Ops | ⭐ | Низкий (observability) | ✅ Срочно (даёт видимость) |

---

*Документ сгенерирован по результатам code review. Каждый пункт можно разобрать отдельно с примерами патчей.*
