# OS Jackfruit – Container Runtime

---

## 1. Team Information

* **Adarsh Rajesh** – SRN: PES1UG24CS020
* **Abhinav Agraharam** – SRN: PES1UG24CS001

---

## 2. Build, Load, and Run Instructions

### 🔹 Build

```bash
make
```

---

### 🔹 Load Kernel Module

```bash
sudo insmod monitor.ko
```

Verify:

```bash
ls -l /dev/container_monitor
```

---

### 🔹 Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

---

### 🔹 Create Writable RootFS Copies

```bash
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

---

### 🔹 Run Containers

```bash
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96
```

---

### 🔹 List Containers

```bash
sudo ./engine ps
```

---

### 🔹 View Logs

```bash
sudo ./engine logs alpha
```

---

### 🔹 Run Workloads

```bash
sudo ./engine start alpha ./rootfs-alpha /memory_hog
sudo ./engine start beta ./rootfs-beta /cpu_hog --nice -5
```

---

### 🔹 Stop Containers

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

---

### 🔹 Kernel Logs

```bash
dmesg | tail
```

---

### 🔹 Unload Module

```bash
sudo rmmod monitor
```

---

## 3. Demo with Screenshots

### 1. Multi-container supervision

<img width="940" height="258" alt="image" src="https://github.com/user-attachments/assets/037502bf-9da1-4156-9a00-2723389df0e7" />
<img width="940" height="211" alt="image" src="https://github.com/user-attachments/assets/b7b6dad3-c5fd-4cba-bee4-0b6e03b9d3b2" />

---

### 2. Metadata tracking (`ps`)

<img width="291" height="75" alt="image" src="https://github.com/user-attachments/assets/90ac9585-acc2-438e-9b33-71311df48e25" />
<img width="940" height="146" alt="image" src="https://github.com/user-attachments/assets/59f3dae3-cb82-4bab-a002-d6ce4193af87" />
<img width="940" height="156" alt="image" src="https://github.com/user-attachments/assets/7a4fa727-fdf4-4a7b-87b7-b972252f86b7" />

---

### 3. Bounded-buffer logging

<img width="940" height="328" alt="image" src="https://github.com/user-attachments/assets/32279167-ae7a-47fe-924a-49a7060a5f67" />

---

### 4. CLI and IPC interaction

<img width="1402" height="90" alt="image" src="https://github.com/user-attachments/assets/29178961-85bf-4359-ac7b-089557835ee4" />

---

### 5. Soft-limit warning

<img width="907" height="258" alt="WhatsApp Image 2026-04-13 at 11 37 42" src="https://github.com/user-attachments/assets/e394ce22-a7fa-4764-94e9-84044f183884" />


---

### 6. Hard-limit enforcement

<img width="1094" height="68" alt="image" src="https://github.com/user-attachments/assets/b51556b4-3a68-46e8-9d72-1b1e70cbfc37" />


---

### 7. Scheduling experiment

<img width="814" height="147" alt="WhatsApp Image 2026-04-13 at 12 43 27" src="https://github.com/user-attachments/assets/601cdcf9-18ed-4af5-a81b-c4483ee021fc" />
<img width="782" height="149" alt="WhatsApp Image 2026-04-13 at 12 44 31" src="https://github.com/user-attachments/assets/219a94c8-4c3e-4d31-9b35-15f601d6a056" />

---

### 8. Clean teardown (no zombies)

<img width="838" height="66" alt="WhatsApp Image 2026-04-13 at 12 51 45" src="https://github.com/user-attachments/assets/7b31e2d0-e558-44be-8068-036d1716b7a9" />

---

## 4. Engineering Analysis

### 1. Isolation Mechanisms

The runtime uses Linux namespaces to achieve process and filesystem isolation. PID namespaces ensure each container has its own process ID space, preventing visibility of host processes. Mount namespaces isolate filesystem views, allowing each container to operate on its own root filesystem. UTS namespaces provide isolated hostname environments.

The use of `chroot()` changes the apparent root directory for the process, restricting filesystem access. Additionally, `pivot_root()` ensures a clean separation between the container filesystem and the host.

However, the host kernel is still shared among all containers. This means scheduling, memory management, and system calls are still handled globally by the host OS.

---

### 2. Supervisor and Process Lifecycle

A long-running supervisor process simplifies container management by acting as the parent of all container processes. It is responsible for process creation using `fork()` and `exec()`, maintaining metadata, and handling lifecycle events.

The parent-child relationship allows the supervisor to track container states and reap child processes using `waitpid()`, preventing zombie processes. Signal handling enables controlled termination (`SIGTERM`) or forced kill (`SIGKILL`) depending on context.

This centralized design ensures proper cleanup and consistent lifecycle management.

---

### 3. IPC, Threads, and Synchronization

Two IPC mechanisms are used:

* UNIX domain sockets (CLI ↔ supervisor communication)
* Pipes (container stdout/stderr → logging system)

The logging system uses a producer-consumer model. The container acts as the producer writing to pipes, while the supervisor consumes data and writes it to log files.

Synchronization is handled using mutexes to protect shared buffers. Without synchronization, race conditions such as buffer overwrites or inconsistent reads could occur. The bounded buffer ensures controlled memory usage and prevents overflow.

---

### 4. Memory Management and Enforcement

The kernel module tracks RSS (Resident Set Size), which represents the actual physical memory used by a process. RSS excludes swapped-out pages, making it a reliable metric for real memory pressure.

Soft and hard limits serve different purposes:

* Soft limit → warning (non-intrusive)
* Hard limit → enforced termination

Enforcement must occur in kernel space because only the kernel has accurate and authoritative access to process memory statistics. User-space monitoring would be unreliable and susceptible to delays or manipulation.

---

### 5. Scheduling Behavior

Experiments using CPU-bound workloads demonstrated the effect of Linux scheduling.

Processes with lower nice values (higher priority) received more CPU time, while those with higher nice values received less. This reflects the behavior of the Completely Fair Scheduler (CFS), which aims to balance fairness while honoring priority.

The results show that scheduling directly impacts responsiveness and throughput, with higher-priority processes completing faster.

---

## 5. Design Decisions and Tradeoffs

### Namespace Isolation

* **Choice:** Use Linux namespaces + chroot
* **Tradeoff:** Lightweight but not as secure as full virtualization
* **Justification:** Suitable for educational container runtime

---

### Supervisor Architecture

* **Choice:** Single long-running parent process
* **Tradeoff:** Simpler design but limited concurrency
* **Justification:** Easier lifecycle management and debugging

---

### IPC and Logging

* **Choice:** Pipes + bounded buffer
* **Tradeoff:** Requires synchronization complexity
* **Justification:** Reliable and efficient logging pipeline

---

### Kernel Monitor

* **Choice:** Linked list with mutex protection
* **Tradeoff:** Slight overhead for locking
* **Justification:** Safe concurrent access

---

### Scheduling Experiments

* **Choice:** nice-based priority testing
* **Tradeoff:** Limited control vs advanced schedulers
* **Justification:** Clearly demonstrates Linux scheduling behavior

---

## 6. Scheduler Experiment Results

### Observations

| Process         | Nice Value | CPU Usage       |
| --------------- | ---------- | --------------- |
| cpu_hog (beta)  | -5         | High (~90–100%) |
| cpu_hog (alpha) | 10         | Lower           |

---

### Conclusion

The Linux scheduler allocates CPU time based on priority. Higher-priority processes receive more CPU share, leading to faster execution. This demonstrates the fairness and responsiveness goals of the scheduler.

---
