# High Availability

## Overview

In a basic HDFS setup, the NameNode is a **single point of failure** — if it goes down, the entire cluster becomes unavailable. HA mode eliminates this with an Active/Standby NameNode pair that automatically fails over in seconds.

---

## HA Architecture

```mermaid
graph TD
    subgraph Clients
        C["HDFS Clients"]
    end

    subgraph NameNode Hosts
        ANN["Active NameNode"]
        ZKFC1["ZKFC"]
        SNN["Standby NameNode"]
        ZKFC2["ZKFC"]
    end

    subgraph Journal Quorum - 3 nodes
        JN1["JournalNode 1"]
        JN2["JournalNode 2"]
        JN3["JournalNode 3"]
    end

    subgraph ZooKeeper Ensemble - 3 nodes
        ZK["ZooKeeper"]
    end

    subgraph DataNodes
        DN1["DataNode 1"]
        DN2["DataNode 2"]
        DN3["DataNode 3"]
    end

    C -->|metadata requests| ANN
    ANN -->|write edit log| JN1
    ANN -->|write edit log| JN2
    ANN -->|write edit log| JN3
    SNN -->|tail edit log| JN1
    SNN -->|tail edit log| JN2
    SNN -->|tail edit log| JN3
    ZKFC1 <-->|health monitor| ANN
    ZKFC2 <-->|health monitor| SNN
    ZKFC1 <-->|lock| ZK
    ZKFC2 <-->|lock| ZK
    DN1 -->|block reports| ANN
    DN1 -->|block reports| SNN
    DN2 -->|block reports| ANN
    DN2 -->|block reports| SNN
```

---

## Key Components

### JournalNodes
- The Active NameNode writes every edit to a **quorum** of JournalNodes before acknowledging the client
- The Standby NameNode continuously **tails** the journal to stay in sync (warm standby)
- Quorum write requires `N/2 + 1` nodes — 3 JNs can tolerate 1 failure
- JournalNodes replace the older shared NFS approach

### ZKFC (ZooKeeper Failover Controller)
- Runs as a separate process on each NameNode host
- Monitors NameNode health with periodic health checks
- Holds a ZooKeeper ephemeral lock — only the Active NN's ZKFC holds the lock
- When Active NN fails, the ZKFC session expires, Standby ZKFC acquires the lock and triggers failover

### Fencing
- **Mandatory** — prevents split-brain where both NNs think they are Active
- Before Standby becomes Active, the old Active is **fenced** (killed via SSH command or STONITH)
- Without fencing, two Active NNs could corrupt the namespace

---

## Failover Sequence

```mermaid
sequenceDiagram
    participant ZKFC1 as ZKFC (Active host)
    participant ANN as Active NameNode
    participant ZK as ZooKeeper
    participant ZKFC2 as ZKFC (Standby host)
    participant SNN as Standby NameNode

    ZKFC1->>ANN: health check
    ANN--xZKFC1: no response (failure)

    Note over ZKFC1: ZK session expires
    ZKFC2->>ZK: try acquire lock
    ZK-->>ZKFC2: lock granted

    ZKFC2->>ANN: fence (SSH kill / STONITH)
    ZKFC2->>SNN: transition to Active

    SNN-->>SNN: load journal edits, become Active
    Note over SNN: Now serving client requests
```

---

## Minimum HA Requirements

| Component | Minimum Count | Why |
|---|---|---|
| NameNodes | 2 | Active + Standby |
| JournalNodes | 3 | Tolerate 1 JN failure |
| ZooKeeper nodes | 3 | Tolerate 1 ZK failure |
| DataNodes | 1+ | Unchanged |

---

## Manual Failover

```bash
# Graceful switchover (e.g. for maintenance)
hdfs haadmin -failover nn1 nn2

# Check status of both NameNodes
hdfs haadmin -getServiceState nn1
hdfs haadmin -getServiceState nn2
```

---

## Key Configuration Properties

```xml
<!-- hdfs-site.xml (HA config) -->
<property>
    <name>dfs.nameservices</name>
    <value>mycluster</value>
</property>
<property>
    <name>dfs.ha.namenodes.mycluster</name>
    <value>nn1,nn2</value>
</property>
<property>
    <name>dfs.ha.automatic-failover.enabled</name>
    <value>true</value>
</property>
<property>
    <name>dfs.ha.fencing.methods</name>
    <value>sshfence</value>
</property>
```

> This bootcamp runs a **non-HA single NameNode** (`hadoop-config/hdfs-site.xml`). HA is a production concern — understand it for architecture interviews and production deployments.
