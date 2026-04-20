#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

// Helper function to safely parse command line arguments into seconds
static unsigned int parse_seconds(const char *arg, unsigned int fallback)
{
    char *end = NULL;
    
    // Convert the string argument to an unsigned long
    unsigned long value = strtoul(arg, &end, 10);

    // If the input is null, empty, or contains invalid characters, return the fallback value
    if (!arg || *arg == '\0' || (end && *end != '\0'))
        return fallback;

    return (unsigned int)value;
}

int main(int argc, char *argv[])
{
    // Set default duration to 0, which means the program will run infinitely
    unsigned int duration = 0; 
    
    // Check if a command line argument was provided and parse it
    if (argc > 1)
        duration = parse_seconds(argv[1], 0);

    // Initialize timers to track execution
    time_t start = time(NULL);
    time_t last_report = start;
    
    // Use volatile to prevent the compiler from optimizing away the math loop
    volatile unsigned long long accumulator = 0;

    // Main execution loop
    while (1) {
        // Perform arbitrary calculations to hog CPU cycles
        accumulator = accumulator * 1664525ULL + 1013904223ULL;

        time_t now = time(NULL);

        // Check if a full second has passed to print the status
        if (now != last_report) {
            last_report = now;
            
            // Output current status
            printf("cpu_hog alive elapsed=%ld accumulator=%llu\n",
                   (long)(now - start),
                   accumulator);
            
            // Force the output to print immediately
            fflush(stdout);
        }

        // If a specific duration was set, check if we need to exit the loop
        if (duration > 0 && (unsigned int)(now - start) >= duration) {
            break;
        }
    }

    // Final log before the program terminates
    printf("cpu_hog done duration=%u accumulator=%llu\n",
           duration, accumulator);

    return 0;
}
