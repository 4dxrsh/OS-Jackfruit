#define _GNU_SOURCE
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <sys/resource.h>

#include "monitor_ioctl.h"

// Define constants for the supervisor environment
#define SOCKET_PATH "/tmp/container_socket"
#define STACK_SIZE (1024 * 1024) // 1MB stack size for cloned processes
#define BUFFER_SIZE 100
#define MAX_CONTAINERS 50

// Global flag to signal threads to wrap up during a clean shutdown
volatile int shutdown_flag = 0;

// ---------- CONTAINER METADATA ----------
// Structure to keep track of each container's lifecycle and resources
struct container {
    char id[32];               // Unique identifier for the container
    pid_t pid;                 // Process ID of the container init
    char state[32];            // Current execution state (e.g., running, exited)
    time_t start_time;         // Timestamp when the container was launched

    int stop_requested;        // Flag indicating if a manual stop was triggered
    int client_fd;             // Socket file descriptor for the blocking 'run' command
    
    // Pointers for memory cleanup after the container exits
    void *stack_ptr;
    void *args_ptr;
};

// Global registry of all containers
struct container containers[MAX_CONTAINERS];
int container_count = 0;

// ---------- SHARED LOG BUFFER ----------
// Ring buffer implementation for the producer-consumer logging system
typedef struct {
    char data[BUFFER_SIZE][256]; // Array holding the actual log strings
    char cid[BUFFER_SIZE][32];   // Array holding the corresponding container IDs
    int in, out, count;          // Ring buffer indices and current count

    pthread_mutex_t mutex;       // Mutex for thread-safe access
    pthread_cond_t not_full;     // Condition variable to block producers if buffer is full
    pthread_cond_t not_empty;    // Condition variable to block consumer if buffer is empty
} log_buffer_t;

log_buffer_t buffer;

// ---------- LOG PRODUCER ----------
// Arguments passed to the producer thread
struct producer_args {
    int fd;         // Pipe file descriptor to read container output from
    char cid[32];   // Container ID associated with this output
};

// Thread routine that continuously reads from a container's pipe and pushes to the ring buffer
void *producer(void *arg) {
    struct producer_args *p = arg;
    char buf[256];

    while (1) {
        // Read raw output from the container's stdout/stderr pipe
        int n = read(p->fd, buf, sizeof(buf) - 1);
        if (n <= 0) break; // Exit loop if pipe is closed or error occurs

        buf[n] = '\0'; // Null-terminate the incoming string

        // Lock the buffer before modifying it
        pthread_mutex_lock(&buffer.mutex);

        // If the buffer is full, wait until the consumer frees up space
        while (buffer.count == BUFFER_SIZE)
            pthread_cond_wait(&buffer.not_full, &buffer.mutex);

        // Copy the log data and container ID into the ring buffer
        strcpy(buffer.data[buffer.in], buf);
        strcpy(buffer.cid[buffer.in], p->cid);

        // Update buffer indices
        buffer.in = (buffer.in + 1) % BUFFER_SIZE;
        buffer.count++;

        // Signal the consumer that new data is available
        pthread_cond_signal(&buffer.not_empty);
        pthread_mutex_unlock(&buffer.mutex);
    }

    // Clean up resources once the container finishes outputting
    close(p->fd);
    free(p);
    return NULL;
}

// ---------- LOG CONSUMER ----------
// Thread routine that pulls logs from the ring buffer and writes them to disk
void *consumer(void *arg) {
    while (1) {
        pthread_mutex_lock(&buffer.mutex);

        // Wait if there's nothing to process, unless we are shutting down
        while (buffer.count == 0 && !shutdown_flag)
            pthread_cond_wait(&buffer.not_empty, &buffer.mutex);

        // If shutdown is triggered and buffer is empty, exit the thread gracefully
        if (shutdown_flag && buffer.count == 0) {
            pthread_mutex_unlock(&buffer.mutex);
            break;
        }

        // Extract the log entry from the ring buffer
        char line[256], cid[32];
        strcpy(line, buffer.data[buffer.out]);
        strcpy(cid, buffer.cid[buffer.out]);

        // Update buffer indices
        buffer.out = (buffer.out + 1) % BUFFER_SIZE;
        buffer.count--;

        // Signal producers that space has opened up
        pthread_cond_signal(&buffer.not_full);
        pthread_mutex_unlock(&buffer.mutex);

        // Format the output filename based on the container ID
        char filename[64];
        sprintf(filename, "%s.log", cid);

        // Append the log line to the respective container's log file
        FILE *f = fopen(filename, "a");
        if (f) {
            fprintf(f, "%s", line);
            fclose(f);
        }
    }
    return NULL;
}

// ---------- CHILD NAMESPACE INITIALIZATION ----------
// Arguments passed to the cloned child process
struct child_args {
    char rootfs[128];
    int pipefd[2];
    char cmd[256];
    int nice_val;
};

// The main function executed by the new container process
int child_func(void *arg) {
    struct child_args *args = arg;

    // Set a custom hostname for the isolated UTS namespace
    sethostname("container", 9);
    
    // Change root directory to isolate the filesystem
    chroot(args->rootfs);
    chdir("/");
    
    // Mount the proc filesystem so tools like 'ps' work inside the container
    mount("proc", "/proc", "proc", 0, NULL);

    // Redirect standard output and error to the writing end of the pipe
    dup2(args->pipefd[1], STDOUT_FILENO);
    dup2(args->pipefd[1], STDERR_FILENO);

    // Close original pipe descriptors as they are no longer needed
    close(args->pipefd[0]);
    close(args->pipefd[1]);

    // Disable buffering to ensure real-time log capturing
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    // Apply the CPU scheduler priority (nice value)
    setpriority(PRIO_PROCESS, 0, args->nice_val);

    // Execute the requested command using the shell
    execl("/bin/sh", "sh", "-c", args->cmd, NULL);

    // If execl returns, something went wrong
    perror("Execution failed inside container");
    return 1;
}

// ---------- CONTAINER LAUNCHER ----------
// Function to handle the 'start' and 'run' commands
pid_t start_container(char *id, char *rootfs, char *cmd, int soft_mib, int hard_mib, int nice_val, int client_fd) {

    // Create a pipe for IPC logging
    int pipefd[2];
    pipe(pipefd);

    // Allocate and populate arguments for the child process
    struct child_args *args = malloc(sizeof(struct child_args));
    strcpy(args->rootfs, rootfs);
    strcpy(args->cmd, cmd);
    args->pipefd[0] = pipefd[0];
    args->pipefd[1] = pipefd[1];
    args->nice_val = nice_val;

    // Allocate an independent stack for the cloned process
    void *stack = malloc(STACK_SIZE);

    // Clone the process into new UTS, Mount, and PID namespaces
    pid_t pid = clone(child_func,
                      stack + STACK_SIZE,
                      CLONE_NEWUTS | CLONE_NEWNS | CLONE_NEWPID | SIGCHLD,
                      args);

    // Close the write end of the pipe in the supervisor
    close(pipefd[1]);

    // -------- KERNEL MODULE REGISTRATION --------
    // Register the new container with the custom kernel monitor for resource limits
    int fd = open("/dev/container_monitor", O_RDWR);
    if (fd >= 0) {
        struct monitor_request req;
        req.pid = pid;
        strncpy(req.container_id, id, MONITOR_NAME_LEN);
        req.soft_limit_bytes = (unsigned long)soft_mib * 1024 * 1024;
        req.hard_limit_bytes = (unsigned long)hard_mib * 1024 * 1024;

        ioctl(fd, MONITOR_REGISTER, &req);
        close(fd);
    }

    // Set up and launch the producer thread for logging
    struct producer_args *p = malloc(sizeof(struct producer_args));
    p->fd = pipefd[0];
    strcpy(p->cid, id);

    pthread_t t;
    pthread_create(&t, NULL, producer, p);
    pthread_detach(t); // Detach thread so it cleans up automatically upon exit

    // Store container metadata in the global registry
    strcpy(containers[container_count].id, id);
    containers[container_count].pid = pid;
    strcpy(containers[container_count].state, "running");
    containers[container_count].start_time = time(NULL);
    containers[container_count].stop_requested = 0;
    containers[container_count].client_fd = client_fd;
    containers[container_count].stack_ptr = stack;
    containers[container_count].args_ptr = args;

    container_count++;
    
    // Status update log
    printf("[Supervisor Log] Launched %s (PID: %d | Soft: %dMiB | Hard: %dMiB | Nice: %d)\n", id, pid, soft_mib, hard_mib, nice_val);

    return pid;
}

// ---------- STATE MANAGEMENT ----------
// Updates the state of a container when it terminates
void update_state(pid_t pid, int status) {
    for (int i = 0; i < container_count; i++) {
        if (containers[i].pid == pid && strcmp(containers[i].state, "running") == 0) {

            // Determine why the container stopped
            if (containers[i].stop_requested) {
                strcpy(containers[i].state, "stopped");
            }
            else if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) {
                strcpy(containers[i].state, "hard_limit_killed");
            }
            else {
                strcpy(containers[i].state, "exited");
            }

            // -------- KERNEL MODULE UNREGISTRATION --------
            // Remove the container from the custom kernel monitor
            int fd = open("/dev/container_monitor", O_RDWR);
            if (fd >= 0) {
                struct monitor_request req;
                req.pid = pid;
                ioctl(fd, MONITOR_UNREGISTER, &req);
                close(fd);
            }

            // If a client is blocking on a 'run' command, notify them of completion
            if (containers[i].client_fd != -1) {
                char resp[64];
                sprintf(resp, "DONE (Exit code: %d)\n", WEXITSTATUS(status));
                write(containers[i].client_fd, resp, strlen(resp));
                close(containers[i].client_fd);
                containers[i].client_fd = -1;
            }

            // Free the memory allocated for the container's stack and arguments
            free(containers[i].stack_ptr);
            free(containers[i].args_ptr);
            
            // Status update log
            printf("[Supervisor Log] Reaped process %s (PID: %d) | Final State: %s\n", containers[i].id, pid, containers[i].state);
        }
    }
}

// ---------- STOP CONTAINER ----------
// Sends a SIGKILL to terminate a running container
void stop_container(char *id) {
    for (int i = 0; i < container_count; i++) {
        if (strcmp(containers[i].id, id) == 0 && strcmp(containers[i].state, "running") == 0) {
            containers[i].stop_requested = 1;
            kill(containers[i].pid, SIGKILL);
        }
    }
}

// ---------- LIST CONTAINERS (PS) ----------
// Generates a formatted string of all containers and their statuses
void list_containers(int fd) {
    char out[2048] = "CONTAINER_ID\tPID\tSTATUS\t\tSTART_TIME\n";

    for (int i = 0; i < container_count; i++) {
        char line[128];
        sprintf(line, "%s\t\t%d\t%s\t%ld\n",
                containers[i].id,
                containers[i].pid,
                containers[i].state,
                containers[i].start_time);
        strcat(out, line);
    }
    // Send the formatted list back to the client
    write(fd, out, strlen(out));
}

// ---------- SIGNAL HANDLER ----------
// Handles SIGINT (Ctrl+C) for graceful termination
void handle_shutdown(int sig) {
    (void)sig; // Explicitly suppress unused variable warning
    shutdown_flag = 1;

    // Wake up the consumer thread so it can exit its loop
    pthread_mutex_lock(&buffer.mutex);
    pthread_cond_broadcast(&buffer.not_empty);
    pthread_mutex_unlock(&buffer.mutex);

    printf("\n[Supervisor System] Initiating clean shutdown sequence...\n");
}

// ---------- SUPERVISOR MAIN LOOP ----------
// Initializes the server socket and processes incoming client requests
void run_supervisor() {
    // Bind the shutdown handler
    signal(SIGINT, handle_shutdown);

    // Initialize ring buffer components
    buffer.in = buffer.out = buffer.count = 0;
    pthread_mutex_init(&buffer.mutex, NULL);
    pthread_cond_init(&buffer.not_full, NULL);
    pthread_cond_init(&buffer.not_empty, NULL);

    // Start the global logging consumer thread
    pthread_t cons;
    pthread_create(&cons, NULL, consumer, NULL);

    // Setup UNIX domain socket for client-server communication
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, SOCKET_PATH);

    // Ensure the socket path is clear before binding
    unlink(SOCKET_PATH);
    bind(server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(server_fd, 5);

    printf("[Supervisor System] Daemon is active and listening...\n");

    // Main event loop
    while (!shutdown_flag) {
        int status;
        pid_t pid;

        // Non-blocking waitpid to reap any zombie processes in the background
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            update_state(pid, status);
        }

        // Setup select() to multiplex socket reading with a 1-second timeout
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(server_fd, &fds);
        struct timeval tv = {1, 0}; 

        // Wait for incoming client connections
        if (select(server_fd + 1, &fds, NULL, NULL, &tv) <= 0)
            continue; // Timeout occurred, loop back around to reap zombies

        // Accept and process client request
        int client_fd = accept(server_fd, NULL, NULL);
        char buf[1024] = {0};
        read(client_fd, buf, sizeof(buf));

        // Default resource limits
        int soft_mib = 40, hard_mib = 64, nice_val = 0;

        // Parse optional configuration flags from the command buffer
        char *soft_ptr = strstr(buf, "--soft-mib");
        if (soft_ptr) sscanf(soft_ptr, "--soft-mib %d", &soft_mib);

        char *hard_ptr = strstr(buf, "--hard-mib");
        if (hard_ptr) sscanf(hard_ptr, "--hard-mib %d", &hard_mib);

        char *nice_ptr = strstr(buf, "--nice");
        if (nice_ptr) sscanf(nice_ptr, "--nice %d", &nice_val);

        // Sanitize the buffer by truncating at the first flag, isolating the core command
        char *first_flag = strstr(buf, "--");
        if (first_flag) *(first_flag - 1) = '\0';

        // Extract core arguments: [command] [container_id] [rootfs_path] [execution_cmd]
        char cmd_type[32] = {0}, id[32] = {0}, rootfs[128] = {0}, cmd[256] = {0};
        sscanf(buf, "%s %s %s %[^\n]", cmd_type, id, rootfs, cmd);

        // Command routing logic
        if (strcmp(cmd_type, "start") == 0) {
            // Start detached container
            start_container(id, rootfs, cmd, soft_mib, hard_mib, nice_val, -1);
            write(client_fd, "OK\n", 3);
            close(client_fd);
        }
        else if (strcmp(cmd_type, "run") == 0) {
            // Start attached container (keep client_fd open until process exits)
            start_container(id, rootfs, cmd, soft_mib, hard_mib, nice_val, client_fd);
        }
        else if (strcmp(cmd_type, "ps") == 0) {
            // Return process list
            list_containers(client_fd);
            close(client_fd);
        }
        else if (strcmp(cmd_type, "stop") == 0) {
            // Terminate specific container
            stop_container(id);
            write(client_fd, "STOPPED\n", 8);
            close(client_fd);
        }
        else if (strcmp(cmd_type, "logs") == 0) {
            // Provide instructions for log retrieval
            char log_msg[64];
            sprintf(log_msg, "View the %s.log file for output\n", id);
            write(client_fd, log_msg, strlen(log_msg));
            close(client_fd);
        } else {
            // Unrecognized command
            close(client_fd);
        }
    }
    
    // Final cleanup procedures
    pthread_join(cons, NULL);
    close(server_fd);
    unlink(SOCKET_PATH);
}

// ---------- CLI CLIENT ROUTINE ----------
// Connects to the supervisor socket and transmits the command line arguments
void run_client(int argc, char *argv[]) {
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, SOCKET_PATH);

    // Attempt connection to the running supervisor daemon
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Failed to establish connection with supervisor");
        return;
    }

    // Reconstruct the command arguments into a single string buffer
    char buf[1024] = {0};
    for (int i = 1; i < argc; i++) {
        strcat(buf, argv[i]);
        if (i < argc - 1) strcat(buf, " ");
    }

    // Transmit to supervisor
    write(sock, buf, strlen(buf));

    // Await and print response
    char response[2048] = {0};
    int n = read(sock, response, sizeof(response));
    if (n > 0) printf("%s", response);

    close(sock);
}

// ---------- PROGRAM ENTRY POINT ----------
// Routes execution based on whether the binary was called as the supervisor or a client
int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage format:\n  engine supervisor <base-rootfs>\n  engine start|run|ps|stop|logs ...\n");
        return 1;
    }
    
    // Check if the user is requesting to spin up the daemon
    if (strcmp(argv[1], "supervisor") == 0)
        run_supervisor();
    else
        run_client(argc, argv);
        
    return 0;
}
