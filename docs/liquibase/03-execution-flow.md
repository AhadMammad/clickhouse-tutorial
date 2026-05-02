# Liquibase — Execution Flow

## What Happens When You Run `liquibase update`

The `update` command is the core operation. It brings the target database schema up to date with the changelog files.

```mermaid
flowchart TD
    START([liquibase update])
    CONNECT["Connect to database\nvia JDBC"]
    INIT["Create DATABASECHANGELOG\nand DATABASECHANGELOGLOCK\nif they don't exist"]
    LOCK["Acquire lock\nDATABASECHANGELOGLOCK = true"]
    READ["Read master changelog\nand resolve all included files"]
    LOOP{{"For each changeset\nin order"}}
    CHECK{"Is this changeset\nin DATABASECHANGELOG?"}
    CHECKSUM{"Does MD5 checksum\nmatch?"}
    SKIP["Skip — already applied"]
    ERROR(["ERROR: changeset was\nmodified after being applied"])
    APPLY["Execute the change\nagainst the database"]
    RECORD["Record in DATABASECHANGELOG\n(id, author, filename, timestamp, MD5)"]
    DONE["All changesets processed"]
    UNLOCK["Release lock\nDATABASECHANGELOGLOCK = false"]
    END([Done])

    START --> CONNECT --> INIT --> LOCK --> READ --> LOOP
    LOOP --> CHECK
    CHECK -- "Yes" --> CHECKSUM
    CHECKSUM -- "Matches" --> SKIP --> LOOP
    CHECKSUM -- "Mismatch" --> ERROR
    CHECK -- "No" --> APPLY --> RECORD --> LOOP
    LOOP -- "No more changesets" --> DONE --> UNLOCK --> END
```

---

## Step-by-Step Walkthrough

### 1. Connect
Liquibase connects to the database using the JDBC URL, username, and password from the environment or properties file.

### 2. Bootstrap tracking tables
On first run, it creates `DATABASECHANGELOG` and `DATABASECHANGELOGLOCK` if they don't exist. This is safe and idempotent.

### 3. Acquire lock
Sets `DATABASECHANGELOGLOCK.LOCKED = true`. If another process holds the lock, Liquibase waits or fails with a timeout error.

### 4. Walk the changelog
Reads the master changelog file, which resolves all `include` references and produces an ordered list of every changeset.

### 5. Per-changeset decision
For each changeset (in order):

| Scenario | Action |
|----------|--------|
| Not in `DATABASECHANGELOG` | Execute the changeset, then record it |
| In `DATABASECHANGELOG`, checksum matches | Skip silently |
| In `DATABASECHANGELOG`, checksum mismatch | Throw error and stop |

### 6. Record execution
After successfully applying a changeset, one row is inserted into `DATABASECHANGELOG`. This is what makes subsequent runs idempotent.

### 7. Release lock
Clears `DATABASECHANGELOGLOCK` so other processes can run.

---

## Sequence Diagram: First Run vs. Incremental Run

```mermaid
sequenceDiagram
    participant LB as Liquibase
    participant TRK as DATABASECHANGELOG
    participant DB as PostgreSQL Schema

    Note over LB,DB: First run — empty database

    LB->>TRK: Does table exist?
    TRK-->>LB: No
    LB->>DB: CREATE TABLE DATABASECHANGELOG
    LB->>DB: CREATE TABLE DATABASECHANGELOGLOCK

    loop Each changeset (001 → 009)
        LB->>TRK: Has this changeset been applied?
        TRK-->>LB: No
        LB->>DB: Execute DDL (CREATE TABLE, CREATE INDEX, INSERT...)
        LB->>TRK: INSERT row (id, author, filename, MD5, timestamp)
    end

    Note over LB,DB: Second run — schema already exists

    loop Each changeset (001 → 009)
        LB->>TRK: Has this changeset been applied?
        TRK-->>LB: Yes — checksum matches
        LB-->>LB: Skip
    end

    Note over LB,DB: Future run — one new changeset added

    loop Changesets 001–009
        LB->>TRK: Has this been applied?
        TRK-->>LB: Yes — skip
    end
    LB->>TRK: Has changeset 010 been applied?
    TRK-->>LB: No
    LB->>DB: Execute DDL for changeset 010
    LB->>TRK: INSERT row for changeset 010
```

---

## What Can Go Wrong

| Error | Cause | Fix |
|-------|-------|-----|
| `Checksum mismatch` | An applied changeset was edited | Revert the edit; add a new changeset instead |
| `Lock held by another process` | Previous run crashed | Manually run `liquibase releaseLocks` |
| `Object already exists` | Schema was modified outside Liquibase | Add a `preconditions` guard or mark the changeset as already run |
| JDBC connection refused | PostgreSQL not up yet | Check `depends_on: condition: service_healthy` in Docker Compose |
