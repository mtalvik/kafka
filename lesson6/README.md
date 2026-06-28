# Lesson 6 — Kafka Security

Hands-on lab: TLS encryption, authentication (mTLS / SASL/PLAIN / SASL/SCRAM), ACL authorization.
Modernized from the OTUS Apache Kafka course (lesson 6) — adapted for **Apache Kafka 4.x with KRaft** (no ZooKeeper).

> 📖 **Full step-by-step guide → [`LAB.md`](./LAB.md)**

---

## What this covers

The lab walks through the four ways Kafka secures client connections, building up incrementally:

| | Transport: PLAINTEXT | Transport: SSL/TLS |
|---|---|---|
| **no SASL** | `PLAINTEXT` (skipped) | `SSL` → **Parts 2, 3** |
| **+ SASL** | `SASL_PLAINTEXT` (skipped — unsafe) | `SASL_SSL` → **Parts 4, 5** |

Plus authorization (ACL) on top in **Part 6**.

## Lab parts

1. **PKI setup** — own CA, broker + client keystores, truststores
2. **SSL без mTLS** — encrypted channel, anonymous client
3. **SSL с mTLS** — client cert authentication, identity from cert DN
4. **SASL/PLAIN over SSL** — username/password auth (inline JAAS in `server.properties`)
5. **SASL/SCRAM over SSL** — challenge-response auth, credentials stored in Kafka itself
6. **ACL with StandardAuthorizer** — per-user permissions

## Prerequisites

- macOS / Linux with Java 17+
- Apache Kafka 4.x (installation step in [`LAB.md`](./LAB.md#20--установить-apache-kafka-one-time))
- `openssl`, `keytool` (come with OpenSSL and JDK)

No Docker, no ZooKeeper, no external services — just Kafka + JDK + OpenSSL.

## Key differences from the original OTUS course

| OTUS slides (Kafka 2.x/3.x с ZK) | This lab (Kafka 4.x KRaft) |
|---|---|
| `kafka.security.authorizer.AclAuthorizer` | `org.apache.kafka.metadata.authorizer.StandardAuthorizer` |
| `kafka-configs.sh --zookeeper localhost:2181` для SCRAM | `kafka-configs.sh --bootstrap-server` |
| `bin/zookeeper-server-start.sh` + `zookeeper_jaas.conf` | not needed (no ZK) |
| Separate `kafka_server_jaas.conf` + `KAFKA_OPTS=-Djava.security.auth.login.config=...` | JAAS inline in `server.properties` via `listener.name.<listener>.<mechanism>.sasl.jaas.config` |
| ACL stored in ZK znodes | ACL stored in `__cluster_metadata` topic |

## Repository layout

```
lesson6/
├── LAB.md                       ← hands-on lab guide (start here)
├── README.md                    ← this file
├── .gitignore                   ← excludes keys, jks, runtime data
├── server-ssl.properties        ← Part 2
├── server-ssl-auth.properties   ← Part 3
├── server-sasl-ssl.properties   ← Part 4
├── client-ssl.properties        ← clients for Parts 2, 3, 4 (as appropriate)
├── client-ssl-auth.properties
├── client-sasl-ssl.properties
└── demo.txt                     ← original OTUS reference (historical)
```

Generated runtime artifacts (gitignored): `*.jks`, `ca-*`, `cert-*`, `kafka-data/`, `logs/`.

## Quick start

```bash
cd lesson6

# 1. Generate PKI (Part 1 — instructions in LAB.md §1.1–1.5)
# 2. Configure broker (e.g., server-ssl.properties — see §2.1)
# 3. Format KRaft storage (§2.2)
# 4. Start broker (§2.3)
~/kafka/bin/kafka-server-start.sh server-ssl.properties
```

Detailed walkthrough — [`LAB.md`](./LAB.md).
