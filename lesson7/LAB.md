# Lesson 7 Lab — GitOps for Kafka topics and ACLs

This lab demonstrates the modern production approach to Kafka cluster
administration: declarative configuration in Git, reconciled with the
live cluster by Terraform. The Terraform Kafka provider uses the Admin
API under the hood — the same `createTopics`, `createAcls`, `deleteTopics`
calls discussed in the lecture, just driven by HCL instead of Java.

## What you will build

- A Terraform module that manages Kafka topics, ACLs, and (optionally)
  quotas declaratively
- A single-node Kafka broker on AWS EC2 with SASL/PLAIN authentication
  (reusing the hw2 setup)
- The full hw2 log pipeline (Filebeat → Kafka → Vector → OpenSearch)
  with all topics and ACLs managed through Terraform rather than
  shell scripts

By the end you will be able to add, modify, and remove Kafka resources
by editing HCL files, running `terraform plan` to preview the change,
and `terraform apply` to execute it. You will have seen drift detection,
removal detection, and rollback.

## Prerequisites

- A working hw2 setup: three EC2 instances (`kafka`, `clients`, `elastic`)
  with the lesson 6 broker configuration, alice/bob/charlie SASL users,
  and the log pipeline.
- AWS CloudShell access with `./aws-lab.sh` and the EC2 SSH key.
- Git repository cloned to `~/REPOS/teaching/kafka/` on the local Mac
  and pushed to `github.com/mtalvik/kafka`.

## Architecture

```
  local Mac                           kafka EC2
  ─────────                           ─────────────────────────
  edit *.tf files                     ┌──────────────────────┐
       │                              │ broker :9092         │
       │ git commit / push            │   SASL/PLAIN         │
       ▼                              │   StandardAuthorizer │
  github.com/mtalvik/kafka            └──────────▲───────────┘
       │                                         │
       │ git pull (on EC2)                       │ Admin API
       ▼                                         │
  ~/kafka-repo/lesson7/gitops/        ┌──────────┴───────────┐
       │                              │ terraform-provider-  │
       │ terraform apply              │ kafka (Mongey/kafka) │
       └─────────────────────────────►└──────────────────────┘
```

Terraform runs on the `kafka` EC2 itself (where the broker is reachable
on `localhost:9092`) and stores its state file on the local disk. State
in S3 would be the production setup; local state is sufficient for the
lab.

## Configuration files explained

The lab uses two directories under `lesson7/`: `gitops/` contains the
declarative cluster state in Terraform; `infra/` contains the broker
deployment artifacts.

### `gitops/versions.tf`

Declares the minimum Terraform version and which providers this module
needs. Terraform downloads the listed providers during `terraform init`.

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.8"
    }
  }
}
```

`Mongey/kafka` is a community provider that wraps `kafka-clients` Admin
API in Terraform resources. The `~> 0.8` constraint allows 0.8.x, 0.9.x,
etc. but not 1.0.

### `gitops/provider.tf`

Configures how the Kafka provider connects to the broker. The values
come from `variables.tf` (which gets them from `terraform.tfvars`).

```hcl
provider "kafka" {
  bootstrap_servers = var.bootstrap_servers
  sasl_username     = var.admin_username
  sasl_password     = var.admin_password
  sasl_mechanism    = "plain"
  tls_enabled       = false
}
```

`tls_enabled = false` because hw2 uses SASL_PLAINTEXT (no TLS — SSL bonus
was deferred). In production this must be true.

### `gitops/variables.tf`

Declares the three required inputs. None have defaults — the user must
supply them. This is intentional: defaults in committed files leak
broker hostnames and even passwords.

```hcl
variable "bootstrap_servers" { type = list(string) }
variable "admin_username"    { type = string }
variable "admin_password"    {
  type      = string
  sensitive = true
}
```

`sensitive = true` makes Terraform mask the value in plan and apply
output.

### `gitops/topics.tf`

Four `kafka_topic` resources — these become the topics in the cluster.

| Topic           | Partitions | RF | Cleanup policy | Retention | Why                                              |
| --------------- | ---------- | -- | -------------- | --------- | ------------------------------------------------ |
| `orders`        | 3          | 1  | delete         | 7 days    | Demo topic for GitOps drills (plan/apply/destroy) |
| `payments`      | 3          | 1  | delete         | 7 days    | Demo topic, used to show "destroy" in plan        |
| `user-profiles` | 1          | 1  | compact        | n/a       | Demo of compacted topic (key-value store pattern) |
| `logs`          | 3          | 1  | delete         | 1 day     | hw2 pipeline topic, with `prevent_destroy = true` |

`logs` is marked `lifecycle.prevent_destroy = true`. This means Terraform
will refuse to delete it unless that block is removed in a separate PR.
This is the standard guard for any topic whose data loss matters.

Replication factor is 1 because this is a single-broker lab. In
production, RF=3 with `min.insync.replicas=2` is the typical minimum.

### `gitops/acls.tf`

Nine `kafka_acl` resources. Kafka ACLs are 5-tuples
(`principal × host × operation × resource × permission_type`), one
Terraform resource per tuple.

| Principal      | Resource type | Resource name | Operation | Why                                                     |
| -------------- | ------------- | ------------- | --------- | ------------------------------------------------------- |
| User:alice     | Topic         | orders        | Write     | Demo producer ACL                                       |
| User:alice     | Topic         | orders        | Describe  | Required for producer to discover topic metadata        |
| User:bob       | Topic         | orders        | Read      | Demo consumer ACL                                       |
| User:bob       | Topic         | orders        | Describe  | Required for consumer to discover topic metadata        |
| User:alice     | Topic         | logs          | Write     | hw2: Filebeat writes log events as alice                |
| User:alice     | Topic         | logs          | Describe  | Required for Filebeat producer                          |
| User:bob       | Topic         | logs          | Read      | hw2: Vector reads log events as bob                     |
| User:bob       | Topic         | logs          | Describe  | Required for Vector consumer                            |
| User:bob       | Group         | `*`           | Read      | Required for bob to commit offsets to any consumer group |

charlie is intentionally not granted any ACL — used in hw2 negative tests
to verify the deny-by-default behavior of `StandardAuthorizer`.

### `gitops/outputs.tf`

Outputs that Terraform prints after `apply`. Pure documentation; they
do not change cluster state. Useful for quickly seeing which topics
and principals are managed by this module without reading every `.tf`
file.

### `gitops/terraform.tfvars.example`

Template showing the structure of `terraform.tfvars`. Real values go
into `terraform.tfvars` (gitignored). Copy and fill in before running
Terraform:

```hcl
bootstrap_servers = ["<BROKER_HOST>:9092"]
admin_username    = "<ADMIN_USERNAME>"
admin_password    = "<ADMIN_PASSWORD>"
```

### `gitops/.gitignore`

Excludes Terraform state, provider cache, and real `.tfvars` files from
git. Without this, the state file (which contains every resource ID and
sometimes secrets) would be committed.

```
.terraform/
*.tfstate
*.tfstate.*
*.tfvars
!*.tfvars.example
```

### `infra/kafka.service`

systemd unit that runs the broker as the `ubuntu` user. Two important
environment variables:

- `KAFKA_HEAP_OPTS=-Xmx256M -Xms256M` — heap reduced from default 1G to
  fit t3.micro (1 GB total RAM).
- `KAFKA_OPTS=-Djava.security.auth.login.config=...` — points the broker
  at the JAAS configuration so SASL/PLAIN works.

`Restart=on-failure` ensures the broker restarts automatically if it
crashes (e.g. OOM).

### `infra/server.properties`

Broker configuration. Key entries:

- `process.roles=broker,controller` — KRaft single-node combined mode.
- `node.id=1` — broker / controller ID.
- `controller.quorum.bootstrap.servers=localhost:9093` — KRaft quorum
  on the loopback (single-node).
- `log.dirs=/home/ubuntu/kafka/data` — persistent log directory. The
  default `/tmp/kraft-combined-logs` was the source of the data loss
  between `stop` and `start` (Ubuntu cleans `/tmp` on boot).
- `listeners=SASL_PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093`
- `advertised.listeners=SASL_PLAINTEXT://172.31.29.117:9092` — what
  the broker tells clients to reconnect to.
- `sasl.enabled.mechanisms=PLAIN`
- `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
- `super.users=User:admin` — the admin user bypasses ACL checks.
- `allow.everyone.if.no.acl.found=false` — deny by default; explicit
  ACLs are required for non-admin users.

### `infra/kafka_server_jaas.conf.example`

Template for the JAAS file that defines SASL/PLAIN credentials. The
real file (`kafka_server_jaas.conf`) is gitignored. Copy and fill in:

```
KafkaServer {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="admin"
   password="<ADMIN_PASSWORD>"
   user_admin="<ADMIN_PASSWORD>"
   user_alice="<ALICE_PASSWORD>"
   user_bob="<BOB_PASSWORD>"
   user_charlie="<CHARLIE_PASSWORD>";
};
```

The `user_<name>` entries on the bottom define all SASL/PLAIN accounts
the broker accepts. The `username`/`password` pair on top is the
broker's own credential when it acts as a client (e.g. for inter-broker
communication or replication; not used in single-node setup but
required by the format).

### `infra/setup.sh`

Idempotent bootstrap script. On a fresh Ubuntu 24.04 EC2:

1. Installs OpenJDK 17 if missing
2. Downloads Kafka 4.3.0 to `~/kafka` if missing
3. Copies `server.properties` and `kafka_server_jaas.conf.example` into
   place (the user must replace JAAS placeholders before running)
4. Formats KRaft storage in `~/kafka/data` if not formatted
5. Installs the systemd unit and enables/starts it
6. Runs a smoke test against the broker

Running this on a working broker is safe — every step skips if its
output already exists.

## Step 1: Verify the broker is running

The broker must be running with SASL_PLAINTEXT on port 9092 and an
`admin` user with full ACLs. If the broker is stopped (e.g. after
`./aws-lab.sh stop`), start the instances first:

```bash
cd ~/otus-kafka
./aws-lab.sh start
./aws-lab.sh ssh kafka
```

On the kafka instance:

```bash
sudo systemctl status kafka
```

Expected output: `active (running)`. If the broker is not running,
`sudo systemctl start kafka`. If `kafka.service` does not exist on
this instance, run `infra/setup.sh` for first-time setup.

Confirm authentication works:

```bash
cd ~/kafka
bin/kafka-broker-api-versions.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties | head -1
```

Expected: `172.31.29.117:9092 (id: 1 rack: null isFenced: false) -> (`.

## Step 2: Clone the repo on the kafka EC2 and install Terraform

```bash
git clone https://github.com/mtalvik/kafka.git ~/kafka-repo
sudo snap install terraform --classic
terraform version
```

## Step 3: Provide broker connection details

`variables.tf` declares three required inputs with no defaults:
`bootstrap_servers`, `admin_username`, `admin_password`. These must be
supplied via `terraform.tfvars` (gitignored) or environment variables.

Copy the template and fill in real values:

```bash
cd ~/kafka-repo/lesson7/gitops
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars
```

For this lab the values are:

```hcl
bootstrap_servers = ["172.31.29.117:9092"]
admin_username    = "admin"
admin_password    = "<value from ~/kafka/config/kafka_server_jaas.conf>"
```

The admin password is the value of `password=` in the `KafkaServer`
block of the JAAS file. View it with:

```bash
grep '^   password=' ~/kafka/config/kafka_server_jaas.conf
```

## Step 4: Initialize Terraform

```bash
terraform init
```

This downloads the `Mongey/kafka` provider into `.terraform/`. Both
`.terraform/` and `terraform.tfvars` are gitignored — they stay on this
EC2 instance.

## Step 5: Plan and apply

```bash
terraform plan
```

The output lists every resource Terraform will create, modify, or delete.
On first run it should show `Plan: 13 to add, 0 to change, 0 to destroy`:
four topics (`orders`, `payments`, `user-profiles`, `logs`) and nine
ACLs (alice/bob × Read|Write|Describe on `orders` and `logs`, plus
bob Read on any consumer group).

```bash
terraform apply
```

Confirm with `yes`. Each resource takes about a second; full apply
completes in 5–15 seconds.

## Step 6: Verify against the live cluster

```bash
cd ~/kafka

bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --list

bin/kafka-acls.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --list
```

The topics and ACLs created by Terraform should appear in the output.

## Step 7: Demonstrate drift detection

Create a topic outside of Terraform — simulating someone bypassing the
GitOps workflow:

```bash
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --create --topic rogue-topic --partitions 1 --replication-factor 1
```

Return to the gitops directory and run plan:

```bash
cd ~/kafka-repo/lesson7/gitops
terraform plan
```

Expected: `No changes`. The `Mongey/kafka` provider only manages
resources that exist in its state — `rogue-topic` is invisible to it.

This is a deliberate design choice. Terraform's "I only manage what I
created" model means multiple teams can each run their own Terraform
against the same cluster without stepping on each other. The trade-off
is that orphan resources are not automatically detected; full-inventory
detection requires either Strimzi (which owns the entire cluster
namespace) or a separate audit job that diffs `kafka-topics --list`
against the expected set.

Clean up the rogue topic before continuing:

```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --delete --topic rogue-topic
```

## Step 8: Demonstrate removal detection

Remove a topic from `topics.tf` to simulate a planned deprecation:

```bash
cd ~/kafka-repo/lesson7/gitops
sed -i '/^resource "kafka_topic" "payments"/,/^}$/d' topics.tf
sed -i '/kafka_topic.payments.name,/d' outputs.tf
terraform plan
```

Expected output:

```
  # kafka_topic.payments will be destroyed
  # (because kafka_topic.payments is not in configuration)
Plan: 0 to add, 0 to change, 1 to destroy.
```

Terraform compares the configuration (what should exist) against state
(what it created last time) and computes the diff. Removing a resource
from configuration means destroying it on next apply.

Do not apply this plan — restore the file:

```bash
git checkout topics.tf outputs.tf
terraform plan
```

Expected: `No changes. Your infrastructure matches the configuration.`

## Step 9: Demonstrate rollback

Modify a topic's configuration (e.g. change `retention.ms` on `orders`),
apply, then revert:

```bash
# Forward change
sed -i 's|"retention.ms"     = "604800000"|"retention.ms"     = "86400000"|' topics.tf
terraform apply  # confirm yes

# Verify change took effect on the cluster
cd ~/kafka
bin/kafka-configs.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --entity-type topics --entity-name orders --describe \
  | grep retention.ms

# Rollback
cd ~/kafka-repo/lesson7/gitops
git checkout topics.tf
terraform apply  # confirm yes

# Verify rollback
cd ~/kafka
bin/kafka-configs.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --entity-type topics --entity-name orders --describe \
  | grep retention.ms
```

The retention reverts to the original value. This rollback is what
`AdminClient` Java code cannot provide without significant additional
work (state tracking, undo log, etc.); Terraform gets it for free
because Git holds the history.

## Step 10: Verify the hw2 pipeline still works

The `logs` topic and its ACLs are now managed by Terraform alongside
the lesson 7 demo topics. Filebeat and Vector continue to use them as
before — `terraform apply` did not change anything visible to the
applications, only the management layer.

On the elastic instance:

```bash
./aws-lab.sh ssh elastic
curl -s "http://localhost:9200/_cat/indices/applogs-*?v"
```

The `applogs-YYYY.MM.DD` index for the current date should show a
growing `docs.count`. If Filebeat was running while the cluster was
without the `logs` topic (between apply runs in earlier sessions), it
buffered events and resumed publishing as soon as the ACL was created.

## What was demonstrated

| Capability                | Achieved with Terraform                       | Achieved with shell scripts    |
| ------------------------- | --------------------------------------------- | ------------------------------ |
| Declarative state in Git  | Yes — `.tf` files are source of truth         | No — state is "in the cluster" |
| Diff before change        | Yes — `terraform plan`                        | No                             |
| Audit trail               | Yes — `git log`, `git blame`                  | Partial — shell history        |
| Repeatable across envs    | Yes — same files apply to dev/staging/prod    | No — copy-paste commands       |
| Drift detection (created) | Yes — plan shows out-of-band modifications    | No                             |
| Drift detection (orphan)  | No (Terraform model); yes with Strimzi model  | No                             |
| Rollback                  | Yes — `git revert` + `terraform apply`        | No                             |
| Protect critical topics   | Yes — `lifecycle.prevent_destroy`             | No                             |

## Repository layout

```
lesson7/
├── LECTURE.md            — concepts and production patterns
├── LAB.md                — this file
├── gitops/
│   ├── versions.tf       — Terraform version and provider requirements
│   ├── provider.tf       — Mongey/kafka provider configuration
│   ├── variables.tf      — required inputs (bootstrap, credentials)
│   ├── topics.tf         — kafka_topic resources
│   ├── acls.tf           — kafka_acl resources
│   ├── outputs.tf        — summary outputs
│   ├── terraform.tfvars.example  — template (real file gitignored)
│   └── .gitignore        — excludes state, .terraform/, real tfvars
└── infra/
    ├── kafka.service     — systemd unit for the broker
    ├── server.properties — broker configuration (KRaft, SASL_PLAINTEXT)
    ├── kafka_server_jaas.conf.example — JAAS template (real one gitignored)
    ├── setup.sh          — bootstrap script for fresh EC2
    └── .gitignore
```

## Next steps beyond this lab

1. **Remote state.** Move `terraform.tfstate` to an S3 backend with
   DynamoDB locking. Required for any setup with more than one
   operator running Terraform.
2. **GitHub Actions.** Run `terraform plan` automatically on pull
   requests and post the plan as a PR comment. Run `terraform apply`
   on merge to main. With this in place, no human needs SSH access to
   the broker for routine topic management.
3. **`prevent_destroy` on more topics.** `logs` already has it; extend
   to any topic whose data loss would be operational impact.
4. **Topic naming policy.** Add validation in the Terraform module
   (or in CI) that enforces team prefixes, lowercase names, no dots,
   etc.
5. **Quota management.** Add `kafka_quota` resources for per-user
   throughput limits. Useful when multiple teams share a cluster.
6. **Schema Registry.** When schemas are added (Avro, Protobuf), they
   too become declarative resources via the
   `confluentinc/confluent` provider's `confluent_schema` or via
   Strimzi's `KafkaSchema` CRD.
