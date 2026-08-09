# Lesson 16 — Kafka Streams: транзакции, Processor API, Interactive Queries

Урок 15 разобрал DSL: `filter`, `mapValues`, `groupByKey().count()`, окна,
соединения. Эта поверхность декларативна и покрывает большую часть работы, но
три вещи остаются за кадром.

Первое — **корректность при сбоях**. Streams-приложение читает записи,
обновляет локальное состояние, пишет результат и коммитит оффсеты — четыре
отдельных эффекта, которые должны либо произойти все, либо ни один. Урок 10
вводил транзакции на уровне Producer; здесь они превращаются в один флаг
конфигурации, но с последствиями, которые стоит понимать.

Второе — **слой под DSL**. `StreamsBuilder.build()` компилируется в `Topology`
из узлов-обработчиков. Эту `Topology` можно собрать руками, и есть операции —
таймеры, условная отправка потомкам, прямой доступ к хранилищу, — которые DSL
выразить не может.

Третье — **чтение состояния обратно**. Пока результаты покидали приложение
только через выходной топик. Interactive Queries позволяют сервису читать свои
собственные state store напрямую, без базы данных в середине.

## Часть 1 — Транзакции в Kafka Streams

### Что именно становится атомарным

Вспомним машинерию уровня Producer из урока 10. **Идемпотентный producer**
(включён по умолчанию: `acks=all`, `retries>0`,
`max.in.flight.requests.per.connection<=5`) устраняет дубликаты от повторов
через producer ID и последовательные номера по партициям и сохраняет порядок
при этих повторах. **Транзакции** строятся сверху: producer пишет записи *и*
оффсеты консьюмера внутри транзакции, потом коммитит или отменяет весь набор,
а координирует это `TransactionCoordinator`.

Streams-задача на каждый батч записей делает три записи:

1. выходные записи в нижестоящие топики,
2. changelog-записи для каждого обновлённого state store,
3. оффсеты консьюмера по прочитанным входным партициям.

С включёнными транзакциями все три попадают в одну транзакцию. Либо весь батч
виден и оффсеты продвинулись, либо ничего из этого нет и батч переобработается
после перезапуска. Вот что здесь значит «exactly-once»: не то, что запись
физически доставлена один раз, а то, что её *эффекты* применены один раз.

```
              +---------------------------+
  input  ---> |  kafka streams (one txn)  | ---> output topic
              |  process + update state   | ---> __consumer_offsets
              +---------------------------+ ---> changelog topic
                   all committed, or all aborted
```

### Как включить

Один параметр:

```java
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

Это всё, что меняется на стороне приложения. Streams сам выставит
`transactional.id` нижележащему producer, включит идемпотентность и переведёт
внутренний консьюмер в `isolation.level=read_committed` для чтения
repartition- и changelog-топиков.

> **Only v2 exists.** `exactly_once` (v1) и `exactly_once_beta` объявлены
> устаревшими в 3.0 и **удалены в Kafka 4.0**. Материалы, предлагающие выбор
> между v1 и v2, написаны до 4.x. Требуется брокер 2.5+.

### v1 против v2 — и почему разница вылезает в latency

Различие важно потому, что распространённый слайд делает из него неверный
вывод.

- **v1** использовал **по одному producer на Task**. При N задачах на потоке —
  N транзакционных producer'ов, N транзакций на коммит.
- **v2** использует **по одному producer на `StreamThread`**. Поток открывает
  одну транзакцию, покрывающую все его задачи, и коммитит один раз. Меньше
  producer'ов, меньше транзакционных маркеров, меньше нагрузки на координатор —
  ровно поэтому v1 и выкинули.

> **Correction.** Старые слайды утверждают «4 subtopology = 4 транзакции = 400 мс
> задержки». *Счёт* — это рассуждение из v1, и он больше не верен: под v2 поток
> коммитит один раз, а не по разу на задачу.
>
> Вывод про *задержку* выживает, но по другой причине. Запись, пересекающая
> границу subtopology, пишется в repartition-топик, а следующая subtopology
> читает его с `read_committed` — то есть не видит запись до коммита выше по
> потоку. Каждый переход поэтому ждёт до одного commit interval. Четыре
> subtopology ≈ 4 × `commit.interval.ms` добавленной сквозной задержки.
> Формулировка: «задержка накапливается на каждой границе subtopology», а не
> «N транзакций».

### Периодичность коммитов

`commit.interval.ms` по умолчанию **30000 мс**, и **100 мс**, когда выставлена
гарантия `exactly_once_v2` — Streams снижает его сам, потому что под
транзакциями commit interval *и есть* нижняя граница задержки.

Пропускная способность почти не страдает: транзакции батчатся, а маркеры
маленькие. Платишь задержкой. Настраивать осознанно:

- ниже `commit.interval.ms` → меньше задержка, больше транзакционных маркеров
  и трафика к координатору;
- выше → эффективнее, но каждый нижестоящий читатель с `read_committed` ждёт
  дольше.

### Нижестоящие консьюмеры

У обычного консьюмера `isolation.level=read_uncommitted`, и он видит записи из
отменённых транзакций. Консьюмеры, которым этого нельзя, ставят
`read_committed` — с двумя следствиями, которые стоит проговорить студентам:

- **незавершённая** транзакция блокирует читателя: он не может уйти дальше
  last stable offset, поэтому зависший producer подвешивает консьюмера;
- брошенная транзакция отменяется координатором по таймауту, после чего
  читатель едет дальше.

> **Timeout figure.** Слайды обычно называют 60 с. Это дефолт
> `transaction.timeout.ms` на уровне брокера/producer. Streams переопределяет
> его вниз для своего producer — проверь фактическое значение на используемом
> кластере, а не цитируй общий дефолт.

### Хранилища состояния и восстановление после падения

Локальные хранилища **не** транзакционны вместе с остальным батчем. При падении
в середине транзакции вывод и оффсеты откатываются, но локальная директория
RocksDB уже может содержать незакоммиченные обновления. Streams решает это тем,
что при рестарте выбрасывает локальное хранилище и **переигрывает
changelog-топик** с последнего закоммиченного оффсета. Корректно, но для
больших хранилищ восстановление долгое.

KIP-892 («Transactional Semantics for StateStores») закрывает именно это: пишет
обновления хранилища в транзакционный буфер, чтобы локальное хранилище можно
было откатить, а не пересобирать. Статус проверь по версии брокера/клиента,
прежде чем описывать это как «в будущем».

### Требования на стороне кластера

Транзакции требуют не только конфига приложения:

- `__transaction_state` создаётся с
  `transaction.state.log.replication.factor` (по умолчанию 3) и
  `transaction.state.log.min.isr` (по умолчанию 2). На **однонодовом брокере
  оба должны быть 1**, иначе инициализация транзакций виснет и падает.
- Внутренние топики Streams — changelog и repartition — создаются с
  `StreamsConfig.REPLICATION_FACTOR_CONFIG`, по умолчанию `-1` (дефолт брокера,
  обычно 3). На одной ноде **ставь 1**. Тот же класс отказа, что в уроках
  14–15, теперь в третьем варианте.
- При включённых ACL принципалу нужны новые права: `Write` и `Describe` на
  ресурс `TransactionalID`, соответствующий `transactional.id`, который Streams
  выводит из `application.id`, плюс `IdempotentWrite` на `Cluster`. Поскольку
  transactional ID выводится из `application.id`, стабильный `application.id`
  позволяет покрыть приложение одной префиксной ACL — а рандомный
  (`"app-" + UUID.randomUUID()`) оставляет каждый запуск без прав.

## Часть 2 — Processor API

### Топология руками

`StreamsBuilder` — это builder *для* `Topology`. Processor API его пропускает и
строит граф напрямую, именуя каждый узел и явно указывая родителей:

```java
Topology topology = new Topology();
topology
    .addSource("source", keyDeserializer, valueDeserializer, "src-topic")
    .addProcessor("upper", CaseProcessor::new, "source")
    .addProcessor("logger", LogProcessor::new, "upper")
    .addSink("sink", "out-topic", keySerializer, valueSerializer, "upper");
```

Последний аргумент каждого вызова — список **имён родительских узлов**. Здесь
`upper` расходится на двух потомков, `logger` и `sink` — форма DAG из урока 15,
выписанная явно, а не выведенная.

Четыре вида узлов: `addSource`, `addProcessor`, `addSink` и `addStateStore`
(привязывается к именованным процессорам). `topology.describe()` печатает
получившийся граф и, что важно, разбиение на **subtopology** — тот же вывод,
что даёт DSL, и именно так считаются переходы транзакций из части 1.

### Интерфейс Processor

```java
static class CaseProcessor implements Processor<String, String, String, String> {
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        context.forward(new Record<>(record.key(),
                                     record.value().toUpperCase(),
                                     record.timestamp()));
    }
}
```

Параметры типа — `<KIn, VIn, KOut, VOut>`. Три метода жизненного цикла: `init`
(вызывается один раз на задачу — сохранить контекст и получить хранилища),
`process` (на каждую запись), `close` (освободить ресурсы; хранилища Streams
закрывает сам).

`ProcessorContext` — ручка ко всему вокруг записи:

- `forward(Record)` — всем потомкам; `forward(Record, String childName)` —
  одному названному потомку. В DSL эквивалента избирательной отправки нет, и
  это одна из причин спускаться на этот уровень.
- `getStateStore(name)` — хранилище, объявленное через `addStateStore` или
  подключённое из DSL.
- `schedule(...)` — таймеры, ниже.
- `recordMetadata()` — `Optional<RecordMetadata>` с топиком, партицией и
  оффсетом обрабатываемой записи. Пусто, если запись пришла из пунктуатора, а
  не из входного топика.
- `taskId()`, `applicationId()`, `commit()` (*запрос* на коммит, а не
  немедленный коммит).

Процессор, который ничего не форвардит, — терминальный узел; логгер выше именно
такой.

### FixedKeyProcessor

`FixedKeyProcessor<KIn, VIn, VOut>` — вариант «только значение»: получает
`FixedKeyRecord` и умеет только `record.withValue(...)`. Раз ключ доказуемо не
меняется, Streams знает, что репартиционирование ниже по потоку не нужно.
Предпочитай его всегда, когда ключ не трогается — та же логика, что `mapValues`
вместо `map` в уроке 15.

> **Removed API.** `Transformer` / `ValueTransformer` с
> `transform()` / `transformValues()` объявлены устаревшими в 3.3 (KIP-820) и
> **удалены в 4.0**. Любой пример с ними не скомпилируется. Замена —
> `Processor` / `FixedKeyProcessor` из
> `org.apache.kafka.streams.processor.api`, через `process()` /
> `processValues()`.

### Punctuation — код по таймеру

Обычно процессор срабатывает только когда пришла запись. `schedule` добавляет
периодический вызов:

```java
Cancellable c = context.schedule(
        Duration.ofSeconds(10),
        PunctuationType.WALL_CLOCK_TIME,
        timestamp -> { /* runs every 10 seconds */ });
```

> **Naming.** Интерфейс называется **`Punctuator`**, функциональный, с методом
> `void punctuate(long timestamp)`. Слайды, где написано `Punctuate.punctuate()`,
> путают имя типа.

Два типа часов, и выбор между ними — весь смысл темы:

- **`STREAM_TIME`** — движется по таймстампам записей, проходящих через задачу.
  Продвигается только когда записи приходят. **Если вход замолчал, пунктуатор
  не сработает никогда.** Та же ловушка, что `suppress(untilWindowCloses)` в
  уроке 15.
- **`WALL_CLOCK_TIME`** — движется по системным часам, срабатывает независимо
  от трафика. Недетерминирован при переобработке, но это правильный выбор для
  «выдавать отчёт раз в 10 секунд» или «вычищать протухшие записи».

Оба — best-effort: вызов происходит внутри poll-цикла, поэтому долгий
`process()` его задержит. `schedule` возвращает `Cancellable` — отменяй, если
расписание пер-ключевое и ключ отработан, иначе расписания накапливаются.

Канонический сценарий: stateful-процессор обновляет хранилище на каждой записи,
а wall-clock пунктуатор раз в N секунд обходит хранилище и форвардит пачку
результатов. Запись на каждое событие, чтение периодически — форма, которую DSL
не выражает.

### Смешивание с DSL

Processor API не обязан заменять DSL. Два оператора DSL встраивают его в
середину цепочки:

```java
stream.process(MyProcessor::new, "my-store");        // может менять ключ
stream.processValues(MyFixedKeyProcessor::new, "my-store");  // ключ сохраняется
```

Хвостовые аргументы называют state store, которые подключаются к процессору.
Это и есть нормальное продакшн-использование Processor API: DSL для обвязки,
узел-процессор там, где DSL кончился.

> **Correction to a slide takeaway.** «Processor API сейчас нет смысла
> использовать» — перегиб. DSL по умолчанию, но Processor API остаётся
> единственным способом: планировать punctuation, форвардить избирательно
> названным потомкам, читать *и* писать хранилище произвольной логикой, делать
> dead-letter маршрутизацию, смотреть метаданные записи при обработке. Устарел
> *`Transformer`*, а не Processor API.

## Часть 3 — Interactive Queries

### Проблема

Streams-приложение, считающее события по ключам, держит ответ в локальном
хранилище, но единственный выход до сих пор — выходной топик. Обычная
архитектура вешает на этот топик второй сервис, который пишет в базу, а базу
уже читает REST-слой:

```
Kafka -> Streams -> output topic -> sink service -> database <- REST API
```

Три движущиеся части ради того, чтобы держать текущий счётчик доступным.
Interactive Queries схлопывают их: Streams-приложение выставляет своё же
хранилище, а REST-слой читает его внутри процесса.

```
Kafka -> Streams (state store) <- REST API
```

Хранилище материализуется обычным способом — от топологии ничего особенного не
требуется:

```java
builder.stream("events", Consumed.with(stringSerde, stringSerde))
       .groupByKey()
       .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-store")
                          .withKeySerde(stringSerde)
                          .withValueSerde(longSerde));
```

Именованное хранилище — значит запрашиваемое хранилище.

### Чтение локального хранилища

```java
ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("count-store",
                                             QueryableStoreTypes.keyValueStore()));
Long value = store.get(key);
```

Тип **только для чтения** по замыслу: записи должны идти через топологию,
иначе changelog и хранилище разъедутся.

Два режима отказа, которые лучше обработать, чем обнаружить на демо:

- вызов `store(...)` до того, как клиент дошёл до `RUNNING`, бросает
  `StreamsNotStartedException` / `InvalidStateStoreException`. Либо retry, либо
  `StateListener` и открывать эндпоинт только в RUNNING. Во время ребаланса
  хранилище снова становится недоступным.
- `store.get(key)` возвращает `null` для ключа, который ещё не встречался. Это
  не ошибка, это 404 — и он не должен доехать до вызывающего в виде
  `NullPointerException`.

### Одно хранилище на task — распределённая часть

Хранилище разбито точно так же, как входной топик: **один экземпляр
хранилища на task**, а задачи размазаны по экземплярам приложения. Экземпляр
держит только те ключи, чьи партиции ему сейчас назначены. Спросить
экземпляр A про ключ, которым владеет B, — получить локальный `null`,
что неверно, а не просто пусто.

Значит, каждый экземпляр должен уметь (а) понять, кто владеет ключом, и
(б) до него дотянуться.

**(а) Владение.** Каждый экземпляр объявляет, где его можно найти:

```java
props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + port);
```

Streams разносит эту строку через протокол consumer group, так что каждый
экземпляр знает полную карту членства. Дальше:

```java
KeyQueryMetadata metadata =
        kafkaStreams.queryMetadataForKey("count-store", key, stringSerde.serializer());
HostInfo active = metadata.activeHost();
```

Streams хеширует ключ заданным сериализатором, определяет партицию и
возвращает хост-владелец — плюс `standbyHosts()` для standby-реплик.

> **Deprecated method.** `allMetadataForKey` возвращал только `StreamsMetadata`
> и заменён на `queryMetadataForKey` в 2.5 (KIP-535), который возвращает
> `KeyQueryMetadata` с разделением active/standby. Слайды с `allMetadataForKey`
> — это до 2.5.

Когда назначение ещё не известно — во время ребаланса — результат равен
`KeyQueryMetadata.NOT_AVAILABLE`, чей хост — сентинел
`HostInfo("unavailable", -1)`. Проверяй на него; не разыменовывай его как
настоящий адрес.

**(б) Как дотянуться.** **Kafka Streams не даёт никакого RPC.** Он сообщает
`host:port` и на этом всё. Транспорт — твой: HTTP, gRPC, что уже есть в
сервисе. Логика маршрутизации всегда одной формы:

```java
HostInfo host = metadata.activeHost();
if (isSelf(host)) {
    return localStore.get(key);      // отвечаем локально
}
return httpGet(host, key);           // переадресуем владельцу
```

`isSelf` сравнивает с собственным значением `APPLICATION_SERVER_CONFIG`. Когда
несколько экземпляров живут на одной машине, достаточно сравнить порт;
между машинами надо сравнивать и хост.

### Поведение при ребалансе

Запусти второй экземпляр — партиции перераспределятся: ключи, бывшие
локальными, становятся удалёнными, и запросы начинают пересылаться. Погаси
его — и после ребаланса партиции вернутся, но хранилище сначала надо
**восстановить из changelog-топика**, и только потом оно отвечает. Окно между
назначением и восстановлением — это ровно тот момент, когда вылетает
`InvalidStateStoreException`. `num.standby.replicas` его сокращает, держа тёплые
копии на других экземплярах; `standbyHosts()` — то, что позволяет ответить
(возможно, чуть устаревшими данными) с одного из них.

### IQv2

KIP-796 (3.2) добавил второй интерфейс запросов,
`KafkaStreams.query(StateQueryRequest)`, расширяемый на типы запросов сверх
поиска по ключу и range scan. Классический API выше не устарел и остаётся
простым для изучения; про IQv2 достаточно знать, что он есть.

## Ключевые тезисы

- **`processing.guarantee=exactly_once_v2`** делает выходные записи,
  changelog-записи и коммит оффсетов одной атомарной единицей. В 4.x
  существует только v2.
- v2 использует **один producer на StreamThread**, а не на task. «N subtopology
  = N транзакций» — рассуждение из v1; накапливается на самом деле
  **задержка** — по одному commit interval на границу subtopology, из-за
  `read_committed` на repartition-топиках.
- `commit.interval.ms` падает до **100 мс** под EOS. Это нижняя граница
  задержки; пропускная способность почти не страдает.
- Локальные хранилища не транзакционны — при падении они **пересобираются из
  changelog**.
- Транзакции на однонодовом брокере требуют
  `transaction.state.log.replication.factor=1`, `min.isr=1` и
  `StreamsConfig.REPLICATION_FACTOR_CONFIG=1`; с ACL — `Write`/`Describe` на
  `TransactionalID` и `IdempotentWrite` на `Cluster`.
- **Processor API**: `addSource` / `addProcessor` / `addSink` /
  `addStateStore`, связывание по имени родителя. `Processor` (ключ может
  меняться) и `FixedKeyProcessor` (ключ сохраняется, репартиция не нужна).
- **`Punctuator`** через `context.schedule(Duration, PunctuationType, ...)`.
  `STREAM_TIME` двигается только входящими записями; `WALL_CLOCK_TIME`
  срабатывает всегда. Пер-ключевые расписания отменяй через `Cancellable`.
- `process()` / `processValues()` встраивают процессор в DSL-цепочку —
  нормальная продакшн-форма. `Transformer` удалён, Processor API — нет.
- **Interactive Queries** читают именованное хранилище внутри процесса,
  убирая sink-сервис и базу. Хранилища пер-task, поэтому
  `APPLICATION_SERVER_CONFIG` + `queryMetadataForKey` маршрутизируют ключ к
  владеющему экземпляру — а **RPC пишешь сам**.
- Обрабатывай `InvalidStateStoreException` (старт или ребаланс), `null`
  (неизвестный ключ) и сентинел-хост `"unavailable"`.

## Ссылки

- Kafka Streams Developer Guide — Processor API, Interactive Queries,
  processing guarantees.
- KIP-447 — producer-per-thread для exactly-once (`exactly_once_v2`).
- KIP-535 — `queryMetadataForKey`, запросы к standby-репликам.
- KIP-796 — Interactive Query v2.
- KIP-820 — удаление `Transformer`/`ValueTransformer`.
- KIP-892 — транзакционная семантика для state store.
- `org.apache.kafka.streams.processor.api` — `Processor`, `FixedKeyProcessor`,
  `ProcessorContext`, `Record`.
- `org.apache.kafka.streams.processor` — `Punctuator`, `PunctuationType`,
  `Cancellable`.
- `org.apache.kafka.streams.state` — `QueryableStoreTypes`, `HostInfo`,
  `ReadOnlyKeyValueStore`.
