#!/usr/bin/env bash
# Provision a 3-node Kafka security lab on AWS.
#
# Hosts:
#   kafka    t3.micro, 8 GB    Apache Kafka 4.3 (KRaft + SASL/PLAIN + ACL)
#   elastic  t3.small, 16 GB   OpenSearch, OpenSearch Dashboards, Kafbat UI (Docker)
#   clients  t3.micro, 8 GB    Filebeat, Vector, log generator (Docker)
#
# Usage: see `aws-lab.sh help`

set -euo pipefail

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

PROJECT="otus-kafka-lab"
REGION="${AWS_REGION:-eu-north-1}"
KEY_NAME="${PROJECT}-key"
SG_NAME="${PROJECT}-sg"
HOSTS=(kafka elastic clients)
KEY_PATH="${HOME}/.ssh/${KEY_NAME}.pem"
UBUNTU_OWNER_ID="099720109477"
PORTS=(22 9092 9093 5601 8080)

host_type() {
  case "$1" in
    elastic) echo "t3.small" ;;
    *)       echo "t3.micro" ;;
  esac
}

host_disk() {
  case "$1" in
    elastic) echo 16 ;;
    *)       echo 8 ;;
  esac
}

# ----------------------------------------------------------------------------
# Output helpers
# ----------------------------------------------------------------------------

log()  { printf '%s\n' "$*"; }
err()  { printf 'error: %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

is_cloudshell() {
  [[ -n "${AWS_EXECUTION_ENV:-}" ]] && [[ "${AWS_EXECUTION_ENV}" == *CloudShell* ]]
}

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [args]

Commands:
  up                    Provision security group, key pair, and all instances
  info                  Print current state
  ssh <host> [cmd]      SSH into kafka|elastic|clients
  scp <src> <host:dst>  Copy file to host
  stop                  Stop instances (storage preserved)
  start                 Start instances
  down                  Terminate instances and delete security group + key
  add-ip <ip>           Authorize an additional IP on all configured ports

Environment:
  AWS_REGION   default: eu-north-1
  MY_IP        override IP detection
EOF
}

# ----------------------------------------------------------------------------
# AWS lookups
# ----------------------------------------------------------------------------

get_account_id() {
  aws sts get-caller-identity --query Account --output text
}

get_default_vpc() {
  aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
    --query 'Vpcs[0].VpcId' --output text
}

get_latest_ubuntu_ami() {
  aws ec2 describe-images \
    --owners "$UBUNTU_OWNER_ID" \
    --filters \
      'Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*' \
      'Name=state,Values=available' \
      'Name=architecture,Values=x86_64' \
    --query 'sort_by(Images, &CreationDate) | [-1].ImageId' --output text
}

get_my_ip() {
  if [[ -n "${MY_IP:-}" ]]; then
    echo "$MY_IP"
  else
    curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]'
  fi
}

get_sg_id() {
  aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SG_NAME}" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo None
}

get_instance_id() {
  aws ec2 describe-instances \
    --filters \
      "Name=tag:Name,Values=${PROJECT}-$1" \
      'Name=instance-state-name,Values=pending,running,stopping,stopped' \
    --query 'Reservations[].Instances[].InstanceId' --output text
}

get_all_instance_ids() {
  aws ec2 describe-instances \
    --filters \
      "Name=tag:Project,Values=${PROJECT}" \
      'Name=instance-state-name,Values=pending,running,stopping,stopped' \
    --query 'Reservations[].Instances[].InstanceId' --output text
}

get_public_dns() {
  aws ec2 describe-instances \
    --filters \
      "Name=tag:Name,Values=${PROJECT}-$1" \
      'Name=instance-state-name,Values=running' \
    --query 'Reservations[].Instances[].PublicDnsName' --output text
}

get_private_ip() {
  aws ec2 describe-instances \
    --filters \
      "Name=tag:Name,Values=${PROJECT}-$1" \
      'Name=instance-state-name,Values=running' \
    --query 'Reservations[].Instances[].PrivateIpAddress' --output text
}

# ----------------------------------------------------------------------------
# Security group ingress
# ----------------------------------------------------------------------------

authorize_ports_for_ip() {
  local sg_id="$1" ip="$2" port
  for port in "${PORTS[@]}"; do
    aws ec2 authorize-security-group-ingress \
      --group-id "$sg_id" --protocol tcp --port "$port" \
      --cidr "${ip}/32" 2>/dev/null || true
  done
}

authorize_self_reference() {
  aws ec2 authorize-security-group-ingress \
    --group-id "$1" --source-group "$1" --protocol all 2>/dev/null || true
}

ensure_my_ip_authorized() {
  local sg_id ip
  sg_id=$(get_sg_id)
  [[ -z "$sg_id" || "$sg_id" == None ]] && return
  ip=$(get_my_ip)
  authorize_ports_for_ip "$sg_id" "$ip" >/dev/null
}

# ----------------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------------

cmd_up() {
  log "account=$(get_account_id) region=${REGION}"

  local vpc_id ami_id my_ip sg_id
  vpc_id=$(get_default_vpc)
  [[ "$vpc_id" == None ]] && die "no default VPC in ${REGION}"

  ami_id=$(get_latest_ubuntu_ami)
  my_ip=$(get_my_ip)

  log "vpc=${vpc_id} ami=${ami_id} operator_ip=${my_ip}"

  # Key pair
  if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
    mkdir -p "$(dirname "$KEY_PATH")"
    aws ec2 create-key-pair --key-name "$KEY_NAME" \
      --query KeyMaterial --output text > "$KEY_PATH"
    chmod 600 "$KEY_PATH"
    log "created key pair: ${KEY_NAME} (${KEY_PATH})"
  fi

  # Security group
  sg_id=$(get_sg_id)
  if [[ "$sg_id" == None || -z "$sg_id" ]]; then
    sg_id=$(aws ec2 create-security-group \
      --group-name "$SG_NAME" \
      --description "Kafka security lab" \
      --vpc-id "$vpc_id" \
      --query GroupId --output text)
    aws ec2 create-tags --resources "$sg_id" \
      --tags "Key=Project,Value=${PROJECT}" "Key=Name,Value=${SG_NAME}"
    log "created security group: ${sg_id}"
  fi

  authorize_ports_for_ip "$sg_id" "$my_ip"
  authorize_self_reference "$sg_id"

  # Instances
  local host name type disk existing iid
  for host in "${HOSTS[@]}"; do
    name="${PROJECT}-${host}"
    type=$(host_type "$host")
    disk=$(host_disk "$host")
    existing=$(get_instance_id "$host")

    if [[ -n "$existing" ]]; then
      log "exists: ${name} (${existing}, ${type}, ${disk}GB)"
      continue
    fi

    iid=$(aws ec2 run-instances \
      --image-id "$ami_id" \
      --instance-type "$type" \
      --key-name "$KEY_NAME" \
      --security-group-ids "$sg_id" \
      --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${disk},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}},{Key=Project,Value=${PROJECT}},{Key=Role,Value=${host}}]" \
      --query 'Instances[0].InstanceId' --output text)
    log "launched: ${name} (${iid}, ${type}, ${disk}GB)"
  done

  local all_ids
  all_ids=$(get_all_instance_ids)
  if [[ -n "$all_ids" ]]; then
    # shellcheck disable=SC2086
    aws ec2 wait instance-running --instance-ids $all_ids
  fi

  cmd_info

  if is_cloudshell; then
    cat <<EOF

Note: when run from CloudShell, the security group is opened for
CloudShell's egress IP. To access from another network, run:

  $(basename "$0") add-ip <ip>
EOF
  fi
}

cmd_info() {
  echo
  aws ec2 describe-instances \
    --filters \
      "Name=tag:Project,Values=${PROJECT}" \
      'Name=instance-state-name,Values=pending,running,stopping,stopped' \
    --query "Reservations[].Instances[].{\
Name:Tags[?Key=='Name']|[0].Value,\
State:State.Name,\
Type:InstanceType,\
PublicDNS:PublicDnsName,\
PrivateIP:PrivateIpAddress,\
AZ:Placement.AvailabilityZone}" \
    --output table

  local host dns ip
  echo
  echo "SSH:"
  for host in "${HOSTS[@]}"; do
    dns=$(get_public_dns "$host" 2>/dev/null || true)
    [[ -n "$dns" && "$dns" != None ]] && \
      printf '  %-8s ssh -i %s ubuntu@%s\n' "$host" "$KEY_PATH" "$dns"
  done

  echo
  echo "Private IPs:"
  for host in "${HOSTS[@]}"; do
    ip=$(get_private_ip "$host" 2>/dev/null || true)
    [[ -n "$ip" && "$ip" != None ]] && \
      printf '  %-8s %s\n' "$host" "$ip"
  done

  echo
  dns=$(get_public_dns elastic 2>/dev/null || true)
  if [[ -n "$dns" && "$dns" != None ]]; then
    echo "URLs:"
    echo "  OpenSearch Dashboards  http://${dns}:5601"
    echo "  Kafbat UI              http://${dns}:8080"
  fi
}

cmd_ssh() {
  local host="${1:-}"
  shift || true
  [[ -z "$host" ]] && die "usage: ssh <kafka|elastic|clients> [command]"
  ensure_my_ip_authorized
  local dns
  dns=$(get_public_dns "$host")
  [[ -z "$dns" || "$dns" == None ]] && die "host ${host} not running"
  if (( $# > 0 )); then
    ssh -i "$KEY_PATH" -o StrictHostKeyChecking=accept-new "ubuntu@${dns}" "$@"
  else
    ssh -i "$KEY_PATH" -o StrictHostKeyChecking=accept-new "ubuntu@${dns}"
  fi
}

cmd_scp() {
  local src="${1:-}" dst="${2:-}"
  [[ -z "$src" || -z "$dst" ]] && die "usage: scp <local-src> <host:remote-dst>"
  ensure_my_ip_authorized
  local host="${dst%%:*}" path="${dst#*:}" dns
  dns=$(get_public_dns "$host")
  [[ -z "$dns" || "$dns" == None ]] && die "host ${host} not running"
  scp -i "$KEY_PATH" -o StrictHostKeyChecking=accept-new "$src" "ubuntu@${dns}:${path}"
}

cmd_stop() {
  local ids
  ids=$(get_all_instance_ids)
  [[ -z "$ids" ]] && { log "nothing to stop"; return; }
  # shellcheck disable=SC2086
  aws ec2 stop-instances --instance-ids $ids >/dev/null
  log "stopping: $ids"
}

cmd_start() {
  local ids
  ids=$(get_all_instance_ids)
  [[ -z "$ids" ]] && { log "no instances; run 'up' first"; return; }
  # shellcheck disable=SC2086
  aws ec2 start-instances --instance-ids $ids >/dev/null
  # shellcheck disable=SC2086
  aws ec2 wait instance-running --instance-ids $ids
  ensure_my_ip_authorized
  cmd_info
}

cmd_down() {
  printf "destroy all %s resources in account %s? type 'destroy' to confirm: " \
    "$PROJECT" "$(get_account_id)"
  read -r confirm
  [[ "$confirm" == destroy ]] || { log "aborted"; exit 0; }

  local ids sg_id
  ids=$(get_all_instance_ids)
  if [[ -n "$ids" ]]; then
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $ids >/dev/null
    # shellcheck disable=SC2086
    aws ec2 wait instance-terminated --instance-ids $ids
    log "terminated: $ids"
  fi

  sg_id=$(get_sg_id)
  if [[ -n "$sg_id" && "$sg_id" != None ]]; then
    local i
    for i in 1 2 3 4 5; do
      aws ec2 delete-security-group --group-id "$sg_id" 2>/dev/null && \
        { log "deleted sg: ${sg_id}"; break; } || sleep 5
    done
  fi

  aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1 && \
    aws ec2 delete-key-pair --key-name "$KEY_NAME" && log "deleted key pair"

  [[ -f "$KEY_PATH" ]] && log "local key retained: ${KEY_PATH}"
}

cmd_add_ip() {
  local ip="${1:-}"
  [[ -z "$ip" ]] && die "usage: add-ip <ip-address>"
  [[ "$ip" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || die "invalid IP: ${ip}"

  local sg_id
  sg_id=$(get_sg_id)
  [[ -z "$sg_id" || "$sg_id" == None ]] && die "security group ${SG_NAME} not found"
  authorize_ports_for_ip "$sg_id" "$ip"
  log "authorized: ${ip}"
}

# ----------------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------------

cmd="${1:-}"; shift || true
case "$cmd" in
  up)              cmd_up "$@" ;;
  info)            cmd_info "$@" ;;
  ssh)             cmd_ssh "$@" ;;
  scp)             cmd_scp "$@" ;;
  stop)            cmd_stop "$@" ;;
  start)           cmd_start "$@" ;;
  down)            cmd_down "$@" ;;
  add-ip)          cmd_add_ip "$@" ;;
  ''|-h|--help|help) usage ;;
  *)               err "unknown command: ${cmd}"; usage; exit 2 ;;
esac
