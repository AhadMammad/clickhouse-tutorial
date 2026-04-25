# Quick Setup

This project supports multiple users running simultaneously on the same machine. Each user gets isolated container names, ports, and data directories.

## First-time setup

```bash
# 1. Generate your personal .env.local (run once — interactive)
make init-user

# 2. Run the full demo
make demo
```

`make init-user` asks for your project name, password, and database, then **scans the host for ports already in use** and picks the first available ones starting from `8123` / `9000`.

## What the interactive prompt looks like

```
Personal environment setup
-------------------------------------------
  Project name          [ahad]:
  ClickHouse password   [test]:
  ClickHouse database   [default]:

Scanning for available ports...

Created .env.local
  COMPOSE_PROJECT_NAME         ahad_clickhouse
  CH_HTTP_PORT                 8124           ← first free port found
  CH_NATIVE_PORT               9001           ← first free port found
  CH_DATA_DIR                  ./clickhouse-data-ahad
  CLICKHOUSE_DATABASE          default
```

Press Enter to accept the default shown in `[brackets]`. The port scanner checks every port from `8123` / `9000` upward until it finds one nothing is bound to.

The generated `.env.local` is gitignored — it's yours, never shared.

## Common commands

| Command | Description |
|---------|-------------|
| `make up` | Start ClickHouse |
| `make down` | Stop containers |
| `make wait` | Wait until ClickHouse is healthy |
| `make setup` | Apply SQL schemas |
| `make generate` | Insert 100k sample rows |
| `make report` | Run analytics queries |
| `make sql` | Open ClickHouse shell |
| `make demo` | Full end-to-end run (up → wait → setup → generate → report) |
| `make logs` | Tail container logs |
| `make ps` | Show running containers |

## Verify isolation

```bash
# Should show your project-namespaced container, e.g. yourname_clickhouse-clickhouse-1
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

## Troubleshooting

**Port already in use after setup** — a new container started after you ran `make init-user`. Edit `.env.local` and increment `CH_HTTP_PORT` / `CH_NATIVE_PORT` manually, or delete `.env.local` and re-run `make init-user`.

**Want to reset your data** — stop containers and delete your data directory:
```bash
make down
rm -rf ./clickhouse-data-yourname
make up
```

**Regenerate .env.local** — delete it first, then re-run:
```bash
rm .env.local && make init-user
```
