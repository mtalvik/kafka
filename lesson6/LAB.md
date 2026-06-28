# Lesson 6 — Kafka Security Lab (KRaft mode)

Hands-on practice: SSL/TLS encryption, authentication via mTLS / SASL/PLAIN / SASL/SCRAM, and ACL authorization.
Modernized from the OTUS Apache Kafka course (lesson 6) — adapted for **Apache Kafka 4.x with KRaft** (no ZooKeeper).

## Prerequisites

- macOS or Linux with Java 17+
- Apache Kafka 4.x installed in `~/kafka` (see step 2.0)
- All commands run from the `lesson6/` directory

> This lab uses a native install (not Docker), because Docker/Colima may be blocked by corporate endpoint security tools.

## Lab outline

1. **PKI setup** — CA, broker keystore, client keystore, truststores
2. **SSL without mTLS** — encrypted channel, no client cert required
3. **SSL with mTLS** — client must present a cert
4. **SASL/PLAIN over SSL** — username/password auth
5. **SASL/SCRAM over SSL** — modern password auth (no external store)
6. **ACL** — per-user permissions via StandardAuthorizer

---

## Big picture — what connects all the Parts

The core idea: **encryption** and **authentication** are two independent knobs. We always turn SSL on for encryption. What changes between parts is **how the client proves its identity**.

### Matrix of Kafka's 4 security protocols

| | Transport: PLAINTEXT | Transport: SSL/TLS |
|---|---|---|
| **no SASL** | `PLAINTEXT` — nothing (Kafka default) | `SSL` — encrypted, optional mTLS → **Parts 2, 3** |
| **+ SASL** | `SASL_PLAINTEXT` — password in clear ⚠️ | `SASL_SSL` — encrypted + SASL → **Parts 4, 5** |

We deliberately skip `PLAINTEXT` and `SASL_PLAINTEXT` — never used in production.

### What changes in each Part

| Part | Listener | Auth method | Client identity | Where credentials live |
|---|---|---|---|---|
| 2 | `SSL://9093` with `ssl.client.auth=none` | none (anonymous) | — | — |
| 3 | `SSL://9093` with `ssl.client.auth=required` | mTLS (client cert) | `User:CN=client,OU=Clients,...` (DN) | `client.keystore.jks` |
| 4 | + `SASL_SSL://9094`, `PLAIN` | username + password | `User:client` | inline JAAS in `server.properties` |
| 5 | + `SASL_SSL://9094`, `SCRAM-SHA-512` | challenge-response | `User:client` | inside Kafka (`__cluster_metadata`) |
| 6 | (any of the above) + `StandardAuthorizer` | (same) | (same) | + ACL rules in metadata |

### What's common across all Parts

- **SSL is always on** — every part runs over an encrypted channel
- **PKI from Part 1** (CA + `*.jks`) is reused in every part
- **Principal `User:<X>`** — Kafka always knows "who is speaking", and Part 6 builds ACL on top of that

### How the config grows (incrementally)

- **Part 2 → Part 3**: change `ssl.client.auth=none` → `required` (one line)
- **Part 3 → Part 4**: add a second listener (SASL_SSL) + a JAAS block listing users
- **Part 4 → Part 5**: change the mechanism `PLAIN` → `SCRAM-SHA-512`, stop hard-coding users in `server.properties`
- **Part 5 → Part 6**: add `authorizer.class.name=StandardAuthorizer` + write ACL rules

---

## Part 1 — PKI setup

Goal: build a complete set of PKI artifacts to reuse across all later parts.

At the end of this section you will have:

| File | What it is | Used by |
|---|---|---|
| `ca-key` + `ca-cert` | our own Certificate Authority | stays local |
| `server.keystore.jks` | broker identity (private key + signed cert) | the broker |
| `server.truststore.jks` | CAs the broker trusts | the broker |
| `client.keystore.jks` | client identity (for mTLS) | the client |
| `client.truststore.jks` | CAs the client trusts | the client |

**How PKI works here:**

```
        ┌───────────┐
        │    CA     │  ← we create our own
        │ ca-cert   │
        │ ca-key    │
        └─────┬─────┘
              │ signs
       ┌──────┴──────┐
       ▼             ▼
  ┌─────────┐   ┌─────────┐
  │ broker  │   │ client  │  ← each has its own keystore
  │ keystore│   │ keystore│
  └─────────┘   └─────────┘

           ↕ exchange certs during TLS handshake ↕

  ┌──────────────┐   ┌──────────────┐
  │ server.      │   │ client.      │  ← both trust the same CA
  │ truststore   │   │ truststore   │     → they trust each other
  │ (CARoot)     │   │ (CARoot)     │
  └──────────────┘   └──────────────┘
```

Password for everything is `password` (this is a lab, not production).

### 1.0 — Cleanup (if running again)

```bash
rm -f server.keystore.jks server.truststore.jks \
      client.keystore.jks client.truststore.jks \
      ca-cert ca-key ca-cert.srl \
      cert-file cert-signed \
      cert-client-file cert-client-signed
```

### 1.1 — Create our own CA

A CA is a key pair (private key + public certificate). The private key is used to **sign** other certificates. The public cert goes into every truststore so everyone knows "this is who we trust".

```bash
openssl req -new -x509 \
  -keyout ca-key -out ca-cert \
  -days 365 \
  -subj "/CN=Lesson6-CA/O=Lesson6/C=EE"
```

You'll be asked for a **PEM pass phrase** — type `password` (twice).

What appeared:
- `ca-key` — CA's private key (encrypted with the passphrase)
- `ca-cert` — CA's public self-signed certificate, valid for 365 days

Verify:
```bash
openssl x509 -in ca-cert -noout -subject -issuer -dates
```

Subject and issuer should be the same (that's what "self-signed" means).

### 1.2 — Broker keystore

Generate the broker's private key + a self-signed cert (the CA will sign it in the next step).

```bash
keytool -genkeypair \
  -keyalg RSA -keysize 2048 \
  -keystore server.keystore.jks \
  -storetype pkcs12 \
  -storepass password -keypass password \
  -alias localhost \
  -validity 365 \
  -dname "CN=localhost,OU=Kafka,O=Lesson6,L=Tallinn,ST=Harjumaa,C=EE"
```

Key options:
- `-storetype pkcs12` — modern format (JKS is older)
- `-alias localhost` — name of the entry inside the keystore; **must match the broker's hostname** (matters for the TLS SAN check)
- `-dname` — Distinguished Name; CN must be the hostname

Verify:
```bash
keytool -list -keystore server.keystore.jks -storepass password
```

Expect one entry: `localhost, PrivateKeyEntry`.

### 1.3 — Sign the broker cert with the CA

Step 1.2 created a self-signed cert. Now we generate a CSR (Certificate Signing Request), have the CA sign it, then import the signed cert back.

```bash
# 1.3a — export the CSR
keytool -keystore server.keystore.jks -alias localhost \
  -certreq -file cert-file -storepass password

# 1.3b — CA signs the CSR
openssl x509 -req \
  -CA ca-cert -CAkey ca-key \
  -in cert-file -out cert-signed \
  -days 365 -CAcreateserial \
  -passin pass:password

# 1.3c — import CA into the broker keystore (MUST come before 1.3d)
keytool -keystore server.keystore.jks -alias CARoot \
  -importcert -file ca-cert -storepass password -noprompt

# 1.3d — import the signed cert (replaces the self-signed one)
keytool -keystore server.keystore.jks -alias localhost \
  -importcert -file cert-signed -storepass password -noprompt
```

> Order matters: **1.3c must come before 1.3d**. keytool requires the CA cert to be already present in the keystore before importing a cert signed by that CA — otherwise it can't verify the chain.

Verify:
```bash
keytool -list -keystore server.keystore.jks -storepass password
```

Now there are **two** entries:
- `CARoot, trustedCertEntry`
- `localhost, PrivateKeyEntry` (with a chain length of 2)

To see the chain in detail:
```bash
keytool -list -v -keystore server.keystore.jks -storepass password -alias localhost
```

You'll see `Certificate chain length: 2` — meaning `localhost` was signed by `CARoot`. That's what we want.

### 1.4 — Truststores (broker + client)

A truststore says "I trust this CA, and anyone signed by it".

```bash
# broker truststore
keytool -keystore server.truststore.jks -alias CARoot \
  -importcert -file ca-cert -storepass password -noprompt

# client truststore
keytool -keystore client.truststore.jks -alias CARoot \
  -importcert -file ca-cert -storepass password -noprompt
```

Verify:
```bash
keytool -list -keystore server.truststore.jks -storepass password
keytool -list -keystore client.truststore.jks -storepass password
```

Each should have one entry: `CARoot, trustedCertEntry`.

### 1.5 — Client keystore (for mTLS in Part 3)

This step is only needed for **SSL with mTLS** (Part 3). If you only plan to use SASL for auth, you don't need a client keystore. But we generate it now to avoid coming back later.

```bash
# 1.5a — generate the client key
keytool -genkeypair \
  -keyalg RSA -keysize 2048 \
  -keystore client.keystore.jks \
  -storetype pkcs12 \
  -storepass password -keypass password \
  -alias client \
  -validity 365 \
  -dname "CN=client,OU=Clients,O=Lesson6,L=Tallinn,ST=Harjumaa,C=EE"

# 1.5b — export the CSR
keytool -keystore client.keystore.jks -alias client \
  -certreq -file cert-client-file -storepass password

# 1.5c — CA signs the CSR
openssl x509 -req \
  -CA ca-cert -CAkey ca-key \
  -in cert-client-file -out cert-client-signed \
  -days 365 -CAcreateserial \
  -passin pass:password

# 1.5d — import CA into the client keystore
keytool -keystore client.keystore.jks -alias CARoot \
  -importcert -file ca-cert -storepass password -noprompt

# 1.5e — import the signed client cert
keytool -keystore client.keystore.jks -alias client \
  -importcert -file cert-client-signed -storepass password -noprompt
```

Verify:
```bash
keytool -list -keystore client.keystore.jks -storepass password
```

Two entries: `CARoot` + `client` (PrivateKeyEntry, chain length 2).

### 1.6 — Final check

```bash
ls -la *.jks ca-* cert-*
```

PKI is ready.

---

## Part 2 — SSL without mTLS

**Goal:** turn on channel encryption. The client verifies the broker's cert via its truststore but **doesn't present its own cert**. Base scenario: data is protected from eavesdropping.

### 2.0 — Install Apache Kafka (one-time)

Download the latest stable version (4.3.0, released May 2026) to `~/kafka`:

```bash
cd ~
curl -L -o kafka.tgz https://dlcdn.apache.org/kafka/4.3.0/kafka_2.13-4.3.0.tgz
tar -xzf kafka.tgz
mv kafka_2.13-4.3.0 kafka
rm kafka.tgz
```

Verify:
```bash
~/kafka/bin/kafka-topics.sh --version
```
Should print `4.3.0`.

> Kafka 4.x is **KRaft-only** — ZooKeeper was removed in 4.0 (March 2025). All scripts under `~/kafka/bin/` work with the new stack.

### 2.1 — server-ssl.properties

We use **relative paths** — the config stays portable and commits cleanly to git. Discipline: always run Kafka commands **from the `lesson6/` directory**.

File `server-ssl.properties`:

```properties
# ---- KRaft (combined mode: broker + controller in the same process) ----
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
controller.listener.names=CONTROLLER

# ---- Listeners ----
listeners=SSL://localhost:9093,CONTROLLER://localhost:9095
advertised.listeners=SSL://localhost:9093
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL

# ---- SSL (paths relative to lesson6/) ----
ssl.keystore.location=server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=server.truststore.jks
ssl.truststore.password=password
ssl.client.auth=none
ssl.endpoint.identification.algorithm=

# ---- Storage (kafka-data/ inside lesson6/, gitignored) ----
log.dirs=kafka-data

# ---- Single-node defaults ----
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
```

**How this maps to OTUS slide 23:**

| Slide | This lab | What changed |
|---|---|---|
| `listeners=SSL://:9093` | `listeners=SSL://localhost:9093,CONTROLLER://localhost:9095` | + KRaft controller listener |
| (not in slide) | `process.roles=broker,controller` | KRaft combined mode |
| (not in slide) | `node.id=1`, `controller.quorum.voters=1@localhost:9095` | KRaft config |
| `ssl.client.auth=requested` | `ssl.client.auth=none` | Part 2 has no mTLS; Part 3 will set `required` |
| `/opt/kafka/private/...` | `server.keystore.jks` | relative path |
| everything else | ✅ same as slide | unchanged |

### 2.2 — Format KRaft storage (one-time per cluster)

KRaft requires a unique cluster ID and formatted storage before the first start.

**Switch to `lesson6/` first** — all the next commands run from there:

```bash
cd lesson6
```

```bash
# data directory (relative path, same as in properties)
mkdir -p kafka-data

# cluster ID
CLUSTER_ID=$(~/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster ID: $CLUSTER_ID"

# format
~/kafka/bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c server-ssl.properties
```

Should print `Formatting metadata directory ... with metadata.version 4.3-IV1`.

> If you want a clean restart later — `rm -rf kafka-data` and re-format.

### 2.3 — Start the broker

From the same `lesson6/` folder:

```bash
~/kafka/bin/kafka-server-start.sh server-ssl.properties
```

Wait for `Kafka Server started`. If it crashes, read the stacktrace — usually paths are wrong (check you're in the right folder).

To stop: Ctrl+C.

**Alternative (like the OTUS slides, in the background):**
```bash
~/kafka/bin/kafka-server-start.sh -daemon server-ssl.properties
tail -f ~/kafka/logs/server.log
~/kafka/bin/kafka-server-stop.sh
```

For learning, foreground is better — errors are visible immediately.

### 2.4 — client-ssl.properties

In a second terminal (also inside `lesson6/`) create `client-ssl.properties`:

```properties
security.protocol=SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
```

### 2.5 — Sanity check

Verify the SSL listener is up:

```bash
openssl s_client -connect localhost:9093 -showcerts < /dev/null 2>&1 | head -30
```

You should see a certificate chain with `CN=localhost`, issuer `Lesson6-CA`. The line `Verify return code: 19 (self signed certificate in certificate chain)` is normal — our CA is self-signed.

### 2.6 — Topic + produce + consume over SSL

```bash
# create the topic
~/kafka/bin/kafka-topics.sh --create \
  --topic test \
  --bootstrap-server localhost:9093 \
  --command-config client-ssl.properties

# list topics
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9093 \
  --command-config client-ssl.properties
```

Producer (type messages, Ctrl+D to exit):
```bash
~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9093 \
  --topic test \
  --producer.config client-ssl.properties
```

Consumer (in a third terminal):
```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9093 \
  --topic test \
  --from-beginning \
  --consumer.config client-ssl.properties
```

You'll see your messages. The channel is TLS-encrypted.

### 2.7 — Try to break it (for understanding)

**A) Connect via PLAINTEXT (default)** — should hang and fail:
```bash
~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9093
```
A client without `--command-config` uses `security.protocol=PLAINTEXT` by default. The broker on 9093 is waiting for a TLS handshake and won't respond to plaintext bytes → timeout, or a Java OutOfMemoryError as the client tries to parse the TLS handshake response as Kafka protocol bytes.

**B) Connect via SSL but WITHOUT a truststore** — handshake failure:
```bash
echo 'security.protocol=SSL' > /tmp/ssl-no-trust.properties
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9093 \
  --command-config /tmp/ssl-no-trust.properties
```
Error like `SSLHandshakeException: PKIX path building failed` — the client received the broker's cert but can't verify its chain because the default truststore (Java's `cacerts`) doesn't contain our CA.

### 2.8 — Stop before Part 3

```bash
# if running in foreground — just Ctrl+C in the broker terminal
# if running as a daemon:
~/kafka/bin/kafka-server-stop.sh
```

Storage is kept in `lesson6/kafka-data/`. For a clean start before Part 3:
```bash
rm -rf kafka-data
```

---

## Part 3 — SSL with mTLS

**Goal:** the broker **requires** a client certificate. Client identity comes from the cert's DN — no passwords, only PKI.

**What changes from Part 2:** one line in the broker config + the client must now present its keystore.

### 3.1 — server-ssl-auth.properties

Create `server-ssl-auth.properties` (same as `server-ssl.properties` but with `ssl.client.auth=required` instead of `none`):

```properties
# ---- KRaft ----
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
controller.listener.names=CONTROLLER

# ---- Listeners ----
listeners=SSL://localhost:9093,CONTROLLER://localhost:9095
advertised.listeners=SSL://localhost:9093
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL

# ---- SSL (with mTLS) ----
ssl.keystore.location=server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=server.truststore.jks
ssl.truststore.password=password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

# ---- Storage ----
log.dirs=kafka-data

# ---- Single-node ----
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
```

### 3.2 — client-ssl-auth.properties

The client must now present its keystore. Create `client-ssl-auth.properties`:

```properties
security.protocol=SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
ssl.keystore.location=client.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.endpoint.identification.algorithm=
```

Difference from `client-ssl.properties`: 3 new lines about the keystore.

### 3.3 — Start the broker

Don't re-format storage — `kafka-data/` is kept from Part 2 (cluster ID, metadata, the `test` topic with all messages — everything stays). Just start with the new config:

```bash
~/kafka/bin/kafka-server-start.sh server-ssl-auth.properties
```

Wait for `Kafka Server started`.

### 3.4 — Tests

**A) With a keystore (`client-ssl-auth.properties`) — works:**
```bash
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9093 \
  --command-config client-ssl-auth.properties
```
Should show `test` (the topic from Part 2).

**B) WITHOUT a keystore (the old `client-ssl.properties`) — handshake fails:**
```bash
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9093 \
  --command-config client-ssl.properties
```
Error like `SSLHandshakeException: Received fatal alert: bad_certificate` or `certificate_required` — the broker demands a cert, the client didn't supply one.

**C) Produce/consume under mTLS:**
```bash
# producer
~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9093 \
  --topic test \
  --producer.config client-ssl-auth.properties

# consumer (another terminal)
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9093 \
  --topic test \
  --from-beginning \
  --consumer.config client-ssl-auth.properties
```

### 3.5 — Client identity in broker logs

When the client connects, the broker logs its principal. In the broker terminal look for lines like:
```
Principal = User:CN=client,OU=Clients,O=Lesson6,L=Tallinn,ST=Harjumaa,C=EE
```

By default the principal is the **entire DN** of the certificate. It's long but precise. For ACL (Part 6) it's common to configure `ssl.principal.mapping.rules` so the principal becomes just `User:client`. Remember this.

### 3.6 — Stop

Ctrl+C in the broker terminal.

> **When to use mTLS vs SASL?** mTLS is a good fit when you already have a PKI infrastructure (e.g. a corporate CA, mesh services with automatically issued certs). SASL/PLAIN or SASL/SCRAM is simpler when you need passwords (LDAP-style user/password). In real production clusters it's common to use mTLS for inter-broker traffic + SASL/SCRAM for clients.

---

## Part 4 — SASL/PLAIN over SSL

**Goal:** authenticate by **username/password**, with passwords stored in the JAAS config on the broker. The channel is still encrypted by SSL — otherwise passwords would travel in clear text.

### Key differences from the OTUS slides

| Slides | This lab (KRaft 4.x) |
|---|---|
| `kafka_server_jaas.conf` in a separate file | JAAS config inline in `server.properties` via `listener.name.<listener>.plain.sasl.jaas.config` |
| `KAFKA_OPTS="-Djava.security.auth.login.config=..."` | not needed |
| `Client { ... };` block for ZK | not needed (ZK removed) |

### Listener architecture in Part 4

Two listeners for clients (like slide 35):
- **SSL on 9093** — mTLS, used for inter-broker traffic
- **SASL_SSL on 9094** — SASL/PLAIN for regular clients
- **CONTROLLER on 9095** — KRaft protocol (internal)

### 4.1 — server-sasl-ssl.properties

```properties
# ---- KRaft ----
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
controller.listener.names=CONTROLLER

# ---- Listeners (3: SSL, SASL_SSL, CONTROLLER) ----
listeners=SSL://localhost:9093,SASL_SSL://localhost:9094,CONTROLLER://localhost:9095
advertised.listeners=SSL://localhost:9093,SASL_SSL://localhost:9094
listener.security.protocol.map=SSL:SSL,SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL

# ---- SSL (for inter-broker mTLS) ----
ssl.keystore.location=server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=server.truststore.jks
ssl.truststore.password=password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

# ---- SASL/PLAIN (for clients on the SASL_SSL listener) ----
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="admin" \
    password="admin-secret" \
    user_admin="admin-secret" \
    user_client="client-secret";

# ---- Storage ----
log.dirs=kafka-data

# ---- Single-node ----
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
```

**Breakdown of the JAAS block:**

```
PlainLoginModule required \
    username="admin" password="admin-secret" \   # broker's own identity (used if the broker itself
                                                 #   connects via SASL as a client)
    user_admin="admin-secret" \                  # valid user #1
    user_client="client-secret";                 # valid user #2
```

Each `user_<name>="<password>"` line adds a valid user that can authenticate. The standalone `username/password` at the top is the broker's own identity when acting as a SASL client. In our setup inter-broker traffic goes via SSL, so that identity isn't actually used — but the module requires it to be set.

### 4.2 — client-sasl-ssl.properties

```properties
security.protocol=SASL_SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="client" \
    password="client-secret";
```

Notice:
- `security.protocol=SASL_SSL` (not just SSL)
- `ssl.keystore.*` is **no longer needed** — identity comes from the password, not the cert
- `ssl.truststore` is still needed — we still verify the broker's cert
- `sasl.mechanism=PLAIN` + an inline JAAS config with username/password

### 4.3 — Start the broker

```bash
~/kafka/bin/kafka-server-start.sh server-sasl-ssl.properties
```

Wait for `Kafka Server started`. The logs show three listeners coming up (SSL, SASL_SSL, CONTROLLER).

### 4.4 — Tests

**A) Client with correct credentials (`client` / `client-secret`) — works:**

Bootstrap on **9094** (SASL_SSL listener):

```bash
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9094 \
  --command-config client-sasl-ssl.properties
```

**B) Wrong password — auth failure:**

```bash
sed 's/client-secret/WRONG/' client-sasl-ssl.properties > /tmp/client-wrong.properties

~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9094 \
  --command-config /tmp/client-wrong.properties
```
Expect `SaslAuthenticationException: Authentication failed: Invalid username or password`.

**C) Produce/consume (optional):**

```bash
# producer
~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --producer.config client-sasl-ssl.properties

# consumer (another terminal)
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --from-beginning \
  --consumer.config client-sasl-ssl.properties
```

### 4.5 — Identity in broker logs

The SASL principal is `User:<username>` — no DN clutter. For our user:
```
Principal = User:client
```
Much cleaner than mTLS — that's why SASL is friendlier for ACL.

### 4.6 — Stop

Ctrl+C in the broker terminal.

> ⚠️ **Limitations of SASL/PLAIN:**
> - Passwords sit in plaintext in `server.properties` (OS file permissions still apply, but it's not great).
> - Adding/removing a user = restart the broker.
> - No challenge-response — the password is effectively sent in cleartext over the connection (which is why SSL is mandatory).
>
> For production, **SCRAM** (Part 5) is better — passwords are stored in Kafka, challenge-response is used, and users can be added without a restart.

---

## Part 5 — SASL/SCRAM over SSL

**Goal:** authenticate by username/password, but with three big improvements over PLAIN:
1. **Passwords are stored inside Kafka** (in the `__cluster_metadata` topic for KRaft), not in `server.properties`
2. **Challenge-response protocol** — the password never travels over the wire as-is. The client sends a salted hash; the server verifies without ever seeing the plaintext password
3. **Users can be added or removed without restarting the broker**

Mechanism we use: `SCRAM-SHA-512` (the strong variant; `SCRAM-SHA-256` also exists).

### The bootstrap problem

SCRAM credentials live inside Kafka. To create them via `kafka-configs.sh` we need to be authenticated to Kafka. Chicken and egg.

KRaft solves this elegantly: `kafka-storage.sh format` accepts a `--add-scram` flag that embeds an initial admin user into the cluster metadata at format time. After that we use the admin user to create everyone else.

> ⚠️ This step **wipes** `kafka-data/`. The `test` topic from Parts 2–4 will be gone. That's fine for the lab — we recreate it.

### Key differences from Part 4 (SASL/PLAIN)

| Aspect | Part 4 (PLAIN) | Part 5 (SCRAM) |
|---|---|---|
| Where credentials live | inline JAAS in `server.properties` | inside Kafka (`__cluster_metadata`) |
| Wire format | password sent as-is over TLS | challenge-response, salted hash |
| Add a user | edit config + restart broker | `kafka-configs.sh` while broker runs |
| Mechanism string | `PLAIN` | `SCRAM-SHA-512` |
| Broker JAAS block | lists every user | empty (just `ScramLoginModule required;`) |

### 5.1 — server-sasl-scram.properties

```properties
# ---- KRaft ----
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
controller.listener.names=CONTROLLER

# ---- Listeners (3: SSL for inter-broker, SASL_SSL for clients, CONTROLLER for KRaft) ----
listeners=SSL://localhost:9093,SASL_SSL://localhost:9094,CONTROLLER://localhost:9095
advertised.listeners=SSL://localhost:9093,SASL_SSL://localhost:9094
listener.security.protocol.map=SSL:SSL,SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL

# ---- SSL (for inter-broker mTLS, same as Part 4) ----
ssl.keystore.location=server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=server.truststore.jks
ssl.truststore.password=password
ssl.client.auth=required
ssl.endpoint.identification.algorithm=

# ---- SASL/SCRAM-SHA-512 (no inline user list — users live in Kafka) ----
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;

# ---- Storage ----
log.dirs=kafka-data

# ---- Single-node ----
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
```

Notice how the JAAS block is just one line with no usernames — the broker will look up credentials in Kafka metadata at authentication time.

### 5.2 — Wipe storage and reformat with an admin SCRAM user

Stop the broker first (Ctrl+C in its terminal). Then:

```bash
# wipe old KRaft metadata + data
rm -rf kafka-data
mkdir -p kafka-data

# generate a fresh cluster ID
CLUSTER_ID=$(~/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster ID: $CLUSTER_ID"

# format storage AND bake in an initial admin SCRAM user
~/kafka/bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c server-sasl-scram.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=admin-secret]'
```

The `--add-scram` flag is the key difference — it embeds the `admin` user's SCRAM credentials directly into the bootstrap metadata, so we can authenticate immediately on first start.

### 5.3 — Start the broker

```bash
~/kafka/bin/kafka-server-start.sh server-sasl-scram.properties
```

Wait for `Kafka Server started`. Three listeners come up as in Part 4.

### 5.4 — admin-sasl-scram.properties (admin's client config)

In a second terminal (also in `lesson6/`):

```bash
cat > admin-sasl-scram.properties << 'EOF'
security.protocol=SASL_SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="admin" \
    password="admin-secret";
EOF
```

This is what we'll use to call `kafka-configs.sh` as the admin user.

### 5.5 — Create a `client` SCRAM user (while the broker is running!)

```bash
~/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=client-secret]' \
  --entity-type users \
  --entity-name client
```

Expected output: `Completed updating config for user client.`

No broker restart needed — the user is now active in the running cluster.

List all SCRAM users to verify:
```bash
~/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --describe \
  --entity-type users
```

Should show `admin` and `client` with their SCRAM-SHA-512 configs.

> Compare with the OTUS slides: they use `kafka-configs.sh --zookeeper localhost:2181 ...`. In Kafka 4.x with KRaft we use `--bootstrap-server` because there's no ZooKeeper.

### 5.6 — client-sasl-scram.properties (regular user's config)

```bash
cat > client-sasl-scram.properties << 'EOF'
security.protocol=SASL_SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="client" \
    password="client-secret";
EOF
```

### 5.7 — Tests

**A) Create the `test` topic and list topics as `client`:**
```bash
~/kafka/bin/kafka-topics.sh --create \
  --topic test \
  --bootstrap-server localhost:9094 \
  --command-config client-sasl-scram.properties

~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9094 \
  --command-config client-sasl-scram.properties
```

**B) Wrong password fails:**
```bash
sed 's/client-secret/WRONG/' client-sasl-scram.properties > /tmp/scram-wrong.properties

~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9094 \
  --command-config /tmp/scram-wrong.properties
```
Expect `SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512`.

**C) Add a third user *without restarting* the broker:**
```bash
~/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=bob-secret]' \
  --entity-type users \
  --entity-name bob
```

Now Bob can immediately authenticate — no broker restart, no config edit. This is the operational advantage of SCRAM over PLAIN.

**D) Produce/consume as `client` (optional):**
```bash
# producer
~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --producer.config client-sasl-scram.properties

# consumer (another terminal)
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --from-beginning \
  --consumer.config client-sasl-scram.properties
```

### 5.8 — Identity in broker logs

Like SASL/PLAIN, the SCRAM principal is `User:<username>`:
```
Principal = User:client
```
No DN, clean and short. Perfect for ACL (Part 6).

### 5.9 — Stop

Ctrl+C in the broker terminal.

> 💡 **Why SCRAM is the prod-standard choice for passwords:** all the operational benefits of LDAP-style auth (add/remove users on the fly, no broker restart, password rotation), plus the security of challenge-response (no plaintext password on the wire even briefly during the SASL exchange).

---

## Part 6 — ACL (StandardAuthorizer)

**Goal:** add **authorization** on top of authentication. Until now any authenticated user could do anything. Now we enforce per-user permissions: who can read which topic, who can write, who can manage consumer groups.

### Key concepts

- **Authorizer** — a broker plugin that checks every operation against a list of ACL rules. In Kafka 4.x KRaft mode the class is `org.apache.kafka.metadata.authorizer.StandardAuthorizer` (the old `kafka.security.authorizer.AclAuthorizer` was ZK-based and is gone).
- **ACL rule** = (Principal, Operation, Resource, Permission) — e.g. `User:client` is **allowed** `Read` on topic `test`.
- **Super users** — listed in `super.users`, bypass all ACL checks. We make `admin` a super-user.
- **Default deny** — with `allow.everyone.if.no.acl.found=false`, any operation without a matching ACL is denied.
- **Storage** — ACL rules live in the `__cluster_metadata` topic (KRaft), not in ZooKeeper znodes.

### Scenario

We'll enforce these rules:
- `admin` — super-user, can do anything (no ACL needed)
- `client` — can only **read** topic `test`
- `bob` — can only **write** to topic `test`

### 6.1 — server-sasl-scram-acl.properties

This is `server-sasl-scram.properties` from Part 5 with 3 extra lines for the authorizer. Copy and append:

```bash
cp server-sasl-scram.properties server-sasl-scram-acl.properties

cat >> server-sasl-scram-acl.properties << 'EOF'

# ---- Authorization ----
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin
allow.everyone.if.no.acl.found=false
EOF
```

**Breakdown:**
- `authorizer.class.name` — turns on the authorizer plugin
- `super.users=User:admin` — the SCRAM user `admin` bypasses ACL checks (so we can always manage the cluster)
- `allow.everyone.if.no.acl.found=false` — default-deny. Without this, any topic without ACL rules would be wide open (which is dangerous in production)

### 6.2 — Restart the broker with the new config

Stop the previous broker (Ctrl+C). Then start with the ACL-enabled config (no need to reformat — `kafka-data/` from Part 5 stays, with SCRAM users and the `test` topic):

```bash
~/kafka/bin/kafka-server-start.sh server-sasl-scram-acl.properties
```

Wait for `Kafka Server started`.

### 6.3 — See default-deny in action

In a new terminal (inside `lesson6/`), try to list topics as `client`:

```bash
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9094 \
  --command-config client-sasl-scram.properties
```

This used to work in Part 5. Now: empty result (or `TopicAuthorizationException` depending on the operation). The user is authenticated but has no permissions — default-deny is taking effect.

Admin still works (super-user):
```bash
~/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties
```
Should show `test`, `__consumer_offsets` etc.

### 6.4 — Grant `client` read access to topic `test`

For a consumer to work it needs:
- `Read` on the topic (to fetch messages)
- `Describe` on the topic (to discover partitions)
- `Read` on the consumer group (to commit offsets)

Grant all three:

```bash
# topic read + describe
~/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --add \
  --allow-principal User:client \
  --operation Read --operation Describe \
  --topic test

# consumer group read (any group)
~/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --add \
  --allow-principal User:client \
  --operation Read \
  --group '*'
```

Expected output: `Adding ACLs for resource ...` for each command.

### 6.5 — Grant `bob` write access to topic `test`

For a producer:
- `Write` on the topic
- `Describe` on the topic

```bash
~/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --add \
  --allow-principal User:bob \
  --operation Write --operation Describe \
  --topic test
```

### 6.6 — List all ACLs

```bash
~/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --list
```

Should show all 3 ACL entries: client/Read/test, client/Describe/test, client/Read/group=*, bob/Write/test, bob/Describe/test.

### 6.7 — bob-sasl-scram.properties

We need a client config for Bob:

```bash
cat > bob-sasl-scram.properties << 'EOF'
security.protocol=SASL_SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="bob" \
    password="bob-secret";
EOF
```

### 6.8 — Tests

**A) `bob` writes to topic (should work):**
```bash
echo "hello from bob" | ~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --producer.config bob-sasl-scram.properties
```

**B) `bob` tries to read (should FAIL — no Read ACL):**
```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --from-beginning \
  --max-messages 1 \
  --consumer.config bob-sasl-scram.properties
```
Expected: `TopicAuthorizationException: Not authorized to access topics: [test]`.

**C) `client` reads (should work):**
```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --from-beginning \
  --max-messages 1 \
  --consumer.config client-sasl-scram.properties
```
Should see `hello from bob`.

**D) `client` tries to write (should FAIL):**
```bash
echo "client tries to write" | ~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 \
  --topic test \
  --producer.config client-sasl-scram.properties
```
Expected: `TopicAuthorizationException: Not authorized to access topics: [test]`.

### 6.9 — Remove an ACL (for completeness)

If you want to revoke `bob`'s write access:
```bash
~/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9094 \
  --command-config admin-sasl-scram.properties \
  --remove \
  --allow-principal User:bob \
  --operation Write --operation Describe \
  --topic test
```

It will ask to confirm — type `y`.

### 6.10 — Stop

Ctrl+C in the broker terminal.

### Comparison with the OTUS slides

| Slides (ZK era) | This lab (KRaft 4.x) |
|---|---|
| `authorizer.class.name=kafka.security.authorizer.AclAuthorizer` | `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` |
| `kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181` | `kafka-acls.sh --bootstrap-server localhost:9094 --command-config admin-config.properties` |
| ACL stored in ZK znodes under `/kafka-acl/` | ACL stored in `__cluster_metadata` topic |
| Principal with mTLS = full DN | Same in KRaft — use `ssl.principal.mapping.rules` to extract CN |
| Principal with SASL = `User:<username>` | Same |

---

## Summary

You've now built up Kafka security from scratch:

1. **PKI** — own CA, broker + client keystores, truststores
2. **SSL** — encrypted channel, anonymous clients
3. **mTLS** — client cert authentication, identity from cert DN
4. **SASL/PLAIN** — username/password auth, inline JAAS, no restart-free user management
5. **SASL/SCRAM** — challenge-response auth, credentials in Kafka itself, hot-reload users
6. **ACL** — per-user authorization on top of any auth mechanism

Every layer composes: SSL is the foundation, SASL replaces or complements mTLS for auth, ACL governs what authenticated users can do. In production, the typical setup is **mTLS for inter-broker** + **SASL/SCRAM for clients** + **ACL with `StandardAuthorizer`** — exactly the stack we built.
