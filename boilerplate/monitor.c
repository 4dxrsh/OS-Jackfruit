#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/slab.h>

#include "monitor_ioctl.h"

// Define the name that will appear in /proc/devices and /dev/
#define DEVICE_NAME "container_monitor"
// Polling interval for checking container memory footprints
#define CHECK_INTERVAL_MS 100


// ---------------- DATA STRUCTURES ----------------
// Represents a single container being actively monitored by the kernel
struct monitor_entry {
    pid_t pid;                                   // Process ID of the container's root process
    char container_id[MONITOR_NAME_LEN];         // Unique string identifier for the container

    unsigned long soft_limit;                    // Threshold for warning (in bytes)
    unsigned long hard_limit;                    // Threshold for termination (in bytes)

    int soft_triggered;                          // Boolean flag to ensure we only warn once

    struct list_head list;                       // Kernel linked list node
};


// ---------------- GLOBAL VARIABLES ----------------
// Head of the linked list containing all active monitor entries
static LIST_HEAD(monitor_list);
// Mutex to protect the monitor_list from concurrent access by the timer and ioctl threads
static DEFINE_MUTEX(monitor_lock);

// Kernel timer for periodic memory polling
static struct timer_list monitor_timer;

// Character device variables for user-space communication
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;


// ---------------- RSS CALCULATION ----------------
// Safely calculates the Resident Set Size (RSS) of a given process in bytes
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    // Use RCU read lock to safely look up the task structure
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1; // Process no longer exists
    }
    // Increment reference count to ensure task doesn't disappear
    get_task_struct(task);
    rcu_read_unlock();

    // Grab the memory descriptor for the task
    mm = get_task_mm(task);
    if (mm) {
        // Retrieve the number of physical pages currently mapped
        rss_pages = get_mm_rss(mm);
        mmput(mm); // Release memory descriptor reference
    }

    // Release task structure reference
    put_task_struct(task);

    // Convert pages to bytes
    return rss_pages * PAGE_SIZE;
}


// ---------------- SOFT LIMIT HANDLER ----------------
// Logs a kernel warning when a container exceeds its soft memory limit
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[MemMonitor] SOFT LIMIT TRIGGERED: container=%s pid=%d current_rss=%ld max_limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}


// ---------------- HARD LIMIT HANDLER ----------------
// Sends SIGKILL to the main process and all its children if the hard limit is breached
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task, *child;

    // Safely look up the target task
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return; // Process already dead, nothing to kill
    }
    get_task_struct(task);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[MemMonitor] HARD LIMIT BREACH (OOM KILL): container=%s pid=%d current_rss=%ld max_limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);

    // 1. Terminate the main container process
    send_sig(SIGKILL, task, 0);

    // 2. Perform a best-effort termination of all immediate child processes
    rcu_read_lock();
    list_for_each_entry(child, &task->children, sibling) {
        send_sig(SIGKILL, child, 0);
    }
    rcu_read_unlock();

    put_task_struct(task);
}


// ---------------- KERNEL TIMER CALLBACK ----------------
// Periodically iterates through all registered containers to check memory usage
static void timer_callback(struct timer_list *t)
{
    struct monitor_entry *entry, *tmp;

    // Lock the list while we iterate
    mutex_lock(&monitor_lock);

    // Use the safe version of list traversal since we might delete nodes
    list_for_each_entry_safe(entry, tmp, &monitor_list, list) {

        long rss = get_rss_bytes(entry->pid);

        // If RSS is negative, the process has exited naturally. Clean up the tracker.
        if (rss < 0) {
            printk(KERN_INFO
                   "[MemMonitor] PROCESS EXITED (CLEANUP): pid=%d\n",
                   entry->pid);

            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        // Soft limit check: Trigger only once per container lifecycle
        if (!entry->soft_triggered && rss > entry->soft_limit) {
            log_soft_limit_event(entry->container_id,
                                 entry->pid,
                                 entry->soft_limit,
                                 rss);
            entry->soft_triggered = 1;
        }

        // Hard limit check: If exceeded, kill the process and remove from monitoring list
        if (rss > entry->hard_limit) {
            kill_process(entry->container_id,
                         entry->pid,
                         entry->hard_limit,
                         rss);

            list_del(&entry->list);
            kfree(entry);
            continue;
        }
    }

    mutex_unlock(&monitor_lock);

    // Reschedule the timer for the next interval
    mod_timer(&monitor_timer,
              jiffies + msecs_to_jiffies(CHECK_INTERVAL_MS));
}


// ---------------- IOCTL INTERFACE ----------------
// Handles communication from user-space (supervisor daemon)
static long monitor_ioctl(struct file *f,
                          unsigned int cmd,
                          unsigned long arg)
{
    struct monitor_request req;

    // Securely copy the request struct from user space to kernel space
    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;

    // -------- REGISTER NEW CONTAINER --------
    if (cmd == MONITOR_REGISTER) {

        struct monitor_entry *entry, *tmp;

        mutex_lock(&monitor_lock);

        // Check for duplicates to prevent tracking the same PID twice
        list_for_each_entry(tmp, &monitor_list, list) {
            if (tmp->pid == req.pid) {
                mutex_unlock(&monitor_lock);
                return -EEXIST;
            }
        }

        // Allocate memory for the new tracking entry
        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) {
            mutex_unlock(&monitor_lock);
            return -ENOMEM;
        }

        // Populate the entry data
        entry->pid = req.pid;
        strncpy(entry->container_id, req.container_id, MONITOR_NAME_LEN - 1);
        entry->container_id[MONITOR_NAME_LEN - 1] = '\0'; // Ensure null termination

        entry->soft_limit = req.soft_limit_bytes;
        entry->hard_limit = req.hard_limit_bytes;
        entry->soft_triggered = 0;

        // Add to the global tracking list
        list_add(&entry->list, &monitor_list);

        mutex_unlock(&monitor_lock);

        printk(KERN_INFO
               "[MemMonitor] REGISTERED: container=%s pid=%d soft=%lu hard=%lu\n",
               entry->container_id,
               entry->pid,
               entry->soft_limit,
               entry->hard_limit);

        return 0;
    }

    // -------- UNREGISTER CONTAINER --------
    if (cmd == MONITOR_UNREGISTER) {

        struct monitor_entry *entry, *tmp;

        mutex_lock(&monitor_lock);

        // Find the specific PID and remove it from the list
        list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
            if (entry->pid == req.pid) {
                list_del(&entry->list);
                kfree(entry);

                mutex_unlock(&monitor_lock);

                printk(KERN_INFO
                       "[MemMonitor] UNREGISTERED: pid=%d\n",
                       req.pid);

                return 0;
            }
        }

        mutex_unlock(&monitor_lock);
        return -ENOENT; // PID not found in the tracking list
    }

    return -EINVAL; // Unknown IOCTL command
}


// ---------------- FILE OPERATIONS ----------------
// Map the IOCTL system call to our custom handler
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};


// ---------------- MODULE INITIALIZATION ----------------
// Sets up the character device, class, and starts the background timer
static int __init monitor_init(void)
{
    // Dynamically allocate a major number
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

// Handle kernel API changes for class creation
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif

    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    // Create the device node in /dev/
    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    // Initialize and add the character device
    cdev_init(&c_dev, &fops);

    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    // Initialize the polling timer
    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer,
              jiffies + msecs_to_jiffies(CHECK_INTERVAL_MS));

    printk(KERN_INFO "[MemMonitor] Kernel Module Loaded Successfully\n");
    return 0;
}


// ---------------- MODULE EXIT ----------------
// Cleans up all resources, stops the timer, and frees memory
static void __exit monitor_exit(void)
{
    struct monitor_entry *entry, *tmp;

    // Stop the timer and wait for any running callbacks to finish
    del_timer_sync(&monitor_timer);

    // Free any remaining entries in the linked list
    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    mutex_unlock(&monitor_lock);

    // Remove the character device and class
    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[MemMonitor] Kernel Module Unloaded\n");
}


module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Container Memory Monitor Module");
