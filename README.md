# OS Jackfruit – Lightweight Container Runtime

## 📌 Overview

This project implements a lightweight container runtime in Linux with support for process isolation, inter-process communication, logging, kernel-level monitoring, and scheduling experiments.

The system consists of a supervisor process that manages multiple containers and communicates with them using IPC mechanisms.

---

## ⚙️ Features Implemented

### 🔹 Task 1: Multi-Container Supervisor

* Supervisor process manages multiple containers concurrently
* Tracks container metadata (ID, PID, state)
* Proper child reaping to avoid zombie processes

---

### 🔹 Task 2: CLI and IPC

* CLI commands implemented:

  * `start` – run container in background
  * `run` – run container in foreground
  * `ps` – list containers
  * `logs` – view container logs
  * `stop` – terminate container
* Communication via UNIX domain sockets
* Supervisor handles all container lifecycle events

---

### 🔹 Task 3: Bounded Buffer Logging

* Captures container stdout and stderr via pipes
* Logging pipeline implemented using producer-consumer model
* Logs stored in per-container files (`logs/<id>.log`)
* Ensures no data loss and proper synchronization

---

### 🔹 Task 4: Kernel Memory Monitoring

* Kernel module (`monitor.ko`) tracks container memory usage
* Uses `/dev/container_monitor` device and `ioctl` interface
* Maintains a linked list of monitored processes

#### Behavior:

* **Soft Limit** → Logs warning in `dmesg`

* **Hard Limit** → Kills process using `SIGKILL`

* Supervisor registers container PID with kernel module

* Kernel safely removes entries when processes exit

---

### 🔹 Task 5: Scheduler Experiment

* CPU-bound workload (`cpu_hog`) used for testing
* Experiments conducted with different nice values:

  * High priority: `nice = -5`
  * Low priority: `nice = 10`

#### Observation:

* Higher priority process received more CPU time (~90–100%)
* Lower priority process received reduced CPU share

#### Conclusion:

Linux Completely Fair Scheduler (CFS) prioritizes processes based on nice values and distributes CPU time accordingly.

---

### 🔹 Task 6: Resource Cleanup

* Verified correct cleanup of all resources

#### Verified:

* No zombie processes (`ps aux | grep defunct`)
* All child processes reaped by supervisor
* Logging completes and file descriptors are closed
* No lingering container processes
* Kernel entries freed on module unload (`rmmod monitor`)

---

## 🧠 Design Decisions

* Used **UNIX domain sockets** for reliable IPC
* Implemented logging using **pipes + file I/O**
* Used **mutex locking** in kernel module for safe shared access
* Chose **single-threaded supervisor** for simplicity
* Used **static binaries** for compatibility with Alpine rootfs

---

## ⚠️ Challenges Faced

* Ensuring binaries run correctly inside container rootfs
* Handling IPC communication between CLI and supervisor
* Debugging kernel module issues and memory tracking
* Managing process lifecycle and avoiding zombies
* Synchronizing logging without data loss

---

## 🚀 How to Run

```bash
make
sudo insmod monitor.ko
sudo ./engine supervisor ./rootfs-base
```

### Run container:

```bash
sudo ./engine start alpha ./rootfs-alpha /bin/sh
```

### Check containers:

```bash
sudo ./engine ps
```

### View logs:

```bash
cat logs/alpha.log
```

### Stop container:

```bash
sudo ./engine stop alpha
```

---

## 📊 Key Concepts Demonstrated

* Process isolation and management
* Inter-process communication (IPC)
* Kernel-user space interaction
* Memory monitoring and enforcement
* Linux scheduling behavior
* Resource cleanup and lifecycle management

---

## ✅ Conclusion

This project demonstrates a complete mini container runtime system integrating user-space control, kernel-level monitoring, and system-level experimentation.

It provides hands-on understanding of core Operating System concepts including process management, scheduling, IPC, and kernel programming.

---
