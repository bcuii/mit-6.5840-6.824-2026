# MapReduce — Paper Reading Notes

> Dean & Ghemawat, *MapReduce: Simplified Data Processing on Large Clusters*, OSDI 2004

---

## 0. The core idea in one sentence

**Trade a restricted programming model for automatic parallelization and transparent fault tolerance.**

The user writes only two pure functions, Map and Reduce, giving up the freedom of arbitrary control flow; in exchange, the framework handles splitting, scheduling, fault tolerance, load balancing, and locality optimization automatically.

This is the first lesson the paper itself lists in its conclusion, and it is the common source of thinking behind later frameworks — Spark, Flink, serverless compute.

---

## 1. Three-party communication

### The governing principle

**The master carries only control information and metadata; the real data flows directly between workers.**

📎 **Full sequence diagram: [`mapreduce-communication.html`](./mapreduce-communication.html)** (open it in a browser)

Counting control flow and data flow separately, there are eight paths (the paper itself never enumerates them this way — the split is here to make clear which one carries the data):

| Path | Content | Key mechanism |
|---|---|---|
| master → map | Task assignment: input split filename + byte range | Prefers a machine holding a replica = locality optimization |
| map → master | Completion report: locations and sizes of the R intermediate files | **Duplicate reports are ignored** — this is what dedupes re-execution |
| master → reduce | Partition number + intermediate file locations | **Locations are pushed incrementally** → shuffle can overlap with the map phase |
| reduce → map | RPC request: "give me the segment for partition k" | The location came from the master, not from asking the map worker |
| **map → reduce** | **The actual data for this partition** | **The only data path — the whole 1TB rides on this line** |
| reduce → master | Completion report | Temp file has already been atomically renamed to the final output |
| master ↔ worker | Heartbeat ping / pong | The response piggybacks counter values; on timeout → completed map tasks on that machine are re-run |
| worker → master | Bad-record sequence number (UDP last gasp) | The process is dying — no time for a TCP handshake |

**Sense of scale**: all control messages added together amount to roughly 60MB of metadata through the master for the entire job; that one data path alone carries 1TB. **Four orders of magnitude apart** — this is why a single master can serve 1700 workers.

### Locations vs. data: the three steps of the chain

```
1. map completes    → reports to master: where the R files sit on local disk
2. master forwards  → reduce worker now knows "where to fetch", but has not one byte of data
3. reduce sends RPC → only this step actually moves the data
```

**The location information comes from the master; the RPC pulls the data itself** — without the location you cannot even issue the RPC. Strictly speaking every communication is an RPC, but only step 3 carries heavy traffic.

Three consequences:

- **Why locations must be centralized at the master**: once a map task finishes its process exits, so there is no one left to ask; and a reduce worker has no idea which of the 1700 machines to ask.
- **Locations are pushed incrementally**: every time the master receives a new batch of reports it pushes them to the reduce workers. This is the mechanism that lets shuffle overlap with the map phase (§5.3: shuffle starts as soon as the first batch of maps completes).
- **There is no control-plane communication between map and reduce**: map does not know who will read it, reduce does not announce itself; the association is established entirely by the master relaying locations. This loose coupling lets a map process exit as soon as it finishes, instead of staying alive until every reducer has fetched its share.

The master therefore stores only two kinds of state (§3.2): per task, the state (idle / in-progress / completed) plus the worker it is on; and for each **completed map task**, the locations and sizes of its R intermediate files — the latter is the source of the **O(M×R)** term, 15000 × 4000 ≈ 60 million entries, about 60MB. **"Metadata centralized, data distributed"** matches GFS's master/chunkserver exactly — same team, same design philosophy.

---

## 2. Mechanisms worth memorizing

### 1. Intermediate data goes to local disk, not GFS

**Motivation**: avoid the 3× write amplification and the cross-network traffic. Network bandwidth is the scarcest resource in the whole system.

**Cost**: intermediate data has no redundant replica.

Note: when a worker machine fails, **the map tasks it had already completed must be re-run too** — the data was sitting on that machine's local disk and is gone with the machine, while all R reducers were counting on fetching their share from it.

By contrast: completed reduce tasks do **not** need re-running, since their output is on GFS.

### 2. Atomic commit gives exactly-once semantics

As long as the user's Map/Reduce are deterministic functions, the distributed execution produces exactly the same result as a sequential execution.

- **map completes**: rename the R temp files into place, report to the master; **the master ignores duplicate reports for an already-completed task**
- **reduce completes**: atomically rename the temp file to the final filename

This relies on the atomicity of the underlying file system's rename. With this machinery in place, backup tasks and failure re-execution are safe — **the dedup logic was designed for failure re-execution in the first place; backup tasks merely reuse it and introduce no new consistency mechanism**.

That same "distributed execution == sequential execution" guarantee is exactly what makes local sequential execution usable for debugging (§3.3).

### 3. Backup tasks against stragglers

As the job nears the end, redundant executions are launched for **all still-running tasks**; whichever finishes first wins.

**A backup task takes over no progress — it recomputes from scratch:**
- Backup **map** task: reads its input from GFS itself (still with locality optimization). If the original machine was slow because of a bad disk, the backup reads a different replica on a different disk, sidestepping the ailment entirely.
- Backup **reduce** task: the entire shuffle is redone, which is far more expensive.

**This is why backups only start near completion** — running everything doubled from the start would saturate the network and the disks.

> With backup tasks disabled, the 1TB sort goes from 891 seconds to 1283 seconds, **44% slower**. The last 5 stragglers held it up for nearly 1000 seconds.

### 4. Locality optimization

GFS stores 3 replicas of every block, and the master tries to schedule each map task onto a **machine holding a replica of that input**, or onto a machine in the same rack. Most input reads consume no network bandwidth at all.

This explains a counterintuitive observation in §5.3: input rate > shuffle rate > output rate. Input comes off local disk, shuffle goes over the network, and output goes over the network *and* writes two replicas.

### 5. Far more tasks than machines

M = 15000, R = 4000, 1746 machines. One machine executes many tasks, **one after another**.

| | Meaning | Who decides |
|---|---|---|
| **M** | number of input splits = number of map tasks | Derived from the split size (16–64MB, aligned to GFS blocks) |
| **R** | number of partitions = number of reduce tasks = number of output files | Specified directly by the user |

**Why it must far exceed the machine count**:
- Dynamic load balancing: fast machines naturally pick up more tasks
- Fast failure recovery: what gets re-run is 8–9 small tasks, and they can be spread out and run in parallel
- Better locality scheduling: finer granularity makes it easier to find an idle machine that holds a replica

**Upper bounds**:
- The master's O(M×R) state
- Fragmentation of the intermediate files (see below)

### 6. What shuffle actually looks like (an M × R all-to-all)

```
15000 map tasks × 4000 reduce tasks = 60 million reads
each map outputs 1TB/15000 ≈ 64MB, cut into 4000 pieces → about 16KB each
```

Every reducer must touch the output of **all** 15000 map tasks, because the key range it owns is scattered across every single map task.

**16KB is the pain point**: a mechanical disk seek costs about 10ms, while reading 16KB takes a fraction of a millisecond — it is almost entirely seek overhead.

Mitigations:
- **Coalesce by machine**: the 15000 tasks live on only 1746 machines, so one connection fetches 8–9 segments ≈ 140KB, cutting connections from 60 million down to about 7 million
- **Page cache**: the intermediate files were written recently and are likely still in memory
- **Stagger the access order**: keep every reducer from starting its fetch at the same machine

**This is the real reason M and R cannot be cranked up blindly** — fine enough granularity vs. sequential enough reads is a genuine tension.

### 7. How much work each reducer does (a sense of scale)

For the sort job: 1TB / 4000 ≈ **250MB**, 2.5 million records.

250MB fits in memory (a 4GB machine still has 2.5GB+ after other tasks take their share), so the sort happens in memory with no external merge needed. **One implicit goal in choosing R = 4000 is to keep each partition within what memory can swallow.**

Too small an R is dangerous: at R=100 each reducer handles 10GB, which inevitably triggers an external sort, while the coarser task granularity also degrades load balancing and failure recovery.

**What determines R is the amount of intermediate data, not the amount of input.** grep's R=1 is perfectly fine, because its output is only 90,000 matches.

---

## 3. Other things worth remembering

### The precondition for a Combiner (the paper does not put it in bold, but it matters)

**The Reduce operation must be commutative and associative.**

- Sum, max: fine
- Average: **no**, `avg(avg(1,2), avg(3)) ≠ avg(1,2,3)`. You have to emit `(sum, count)` pairs to make it combinable

Combiner and Reduce code is usually identical; the only difference is **where the output goes**: the Combiner writes an intermediate file (to be sent to reduce), while Reduce writes the final output file.

### Global sort = a composition of two primitives

**MapReduce implements no "distributed sorting algorithm" of its own.** It provides two primitives, and the user composes them to get a global sort in under 50 lines of code.

```
globally sorted = ordered across partitions + ordered within a partition
                  ↑                           ↑
             range partitioner (4.1)     reduce sorts anyway (4.2, free)
```

**The key insight: the ordering across partitions is not produced by *sorting*, it is produced by *partitioning*.**

Change the partition function from a hash to a range split:

```
partition 0 accepts only  key < "d"
partition 1 accepts only  "d" ≤ key < "s"
partition 2 accepts only  key ≥ "s"
```

Once that is the rule, "every key in partition 0 < every key in partition 1" is a **direct consequence** of the rule; no machine has to verify it. It is like sorting mail by postal code — the order among the bins was fixed the moment the sorting happened.

**Three easy mistakes**:
1. Every mapper must use the **same** boundary array, or the partitioning semantics collapse → which is why sampling has to be a separate, prior job
2. Swap in a hash and it falls apart completely: each partition is still sorted internally, but concatenating them yields garbage
3. Boundaries are determined by **sampling for quantiles**, to avoid data skew (TeraSort's keys are random bytes and uniformly distributed, so the paper skips sampling entirely)

### What is clever about skipping bad records

1. A signal handler catches SIGSEGV / SIGBUS
2. **Before** calling the user's function, the record's sequence number is stored in a global variable
3. On a crash, a UDP packet is sent to the master (**UDP because the process is dying and there is no time for a TCP handshake**)
4. The master only orders a record skipped after seeing it fail **twice** (**so that a one-off machine fault is not misdiagnosed as a deterministically bad record**)

### The correctness detail in counters

A worker's counter values are sent to the master **piggybacked on the ping response** (reusing the existing heartbeat channel at zero extra cost).

**The master only accumulates counts from successfully completed tasks, and counts each task exactly once** — otherwise backup tasks and failure re-execution would double-count.

### Key numbers

| Item | Value |
|---|---|
| Backup tasks disabled | Sort **44%** slower (891 → 1283 s) |
| 200 workers killed | Only **5%** slower (891 → 933 s) |
| Indexing system rewrite | 3800 lines → **700 lines** of C++ |
| Average machine failures per job | **1.2** (at a scale of 157 machines) |
| 1TB sort / 1TB grep | 891 s / 150 s (including 60 s of startup overhead) |
| Cumulative (through Aug 2004) | 29423 jobs, 3288TB input → 758TB intermediate → 193TB output |

**Put the three timing numbers side by side and you have the paper's whole argument**:

> Deliberately inflicted large-scale failure (+5%) costs far less than tolerating a few slow nodes (+44%).

**"1.2 machine failures per job on average"** is the premise the entire paper rests on — at this scale failure is the norm, not the exception, and fault tolerance is not optional.

The funnel shape of **3288TB input → 193TB output** shows that the typical job is "large input, heavy reduction" — the opposite of the 1:1:1 extreme that sorting represents.

### §6.1 The indexing system: its real, underrated value

3800 → 700 lines is only the surface. The real change is that **once scanning became cheap, people could finally organize code by logical clarity rather than under performance pressure** — no longer cramming unrelated logic into a single pass just to amortize the cost. Changes that used to take months now take days.

### The limits of this paper

- **Not suited to low latency or small jobs**: 60 of grep's 150 seconds are startup overhead (distributing the binary, opening 1000 GFS files, obtaining locality information)
- **Not suited to iterative computation**: every round's intermediate results must hit disk and be read back, which is extremely expensive for repeated iteration like machine learning → precisely the gap Spark moved into with in-memory RDDs
- **The single master has no high availability**: the paper admits that in their implementation, if the master dies the whole job restarts, and waves it off as unlikely
- **Limited originality**: Related Work concedes that the map/reduce primitives come from functional languages, that distributed sorting databases already existed, and that River had done redundant execution. The contribution is **the combination under a specific set of constraints**

### Two philosophies about stragglers (§7)

River predicts precisely and dynamically balances producer/consumer rates; MapReduce does not predict at all, and falls back on fine-grained tasks + redundant execution. The latter is crude, but more robust in an environment of thousands of machines with a high failure rate — a trade-off that recurs repeatedly in later distributed systems.

### The weak guarantee for non-deterministic functions

If the user's Map is non-deterministic, backup executions A and B may produce different intermediate results, and different reduce tasks may read data from different executions. The semantics degrade to "each reduce task's output corresponds to *some* sequential execution, but the whole need not correspond to the same one" (end of §3.3).

---

## 4. A self-check while writing Lab 1

**For every piece of code you write, try to say which constraint it corresponds to.**

| Design decision | The constraint behind it |
|---|---|
| Intermediate data not written to GFS | Network bandwidth is scarce |
| Completed map tasks re-run on machine failure | The direct cost of the line above |
| Far more tasks than machines | Failure is the norm + machine speeds vary |
| Fault tolerance by re-execution rather than replication | Cheap machines; compute is cheaper than storage |
| Splits aligned to 16–64MB | GFS block size + the balance of seek overhead vs. granularity |
| Atomic rename to commit | Repeated execution must be supported |
| Master stores metadata only | A single master must not become a bandwidth bottleneck |
| Backup tasks only start near the end | Redundant execution is expensive; only worth paying for tail latency |
