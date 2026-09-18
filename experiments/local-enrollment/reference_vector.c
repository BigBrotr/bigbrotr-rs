/* Independent baseline: compile against the pinned PHC reference, never product Rust. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "argon2.h"
int main(int argc, char **argv) {
    uint8_t salt[16], output[32];
    const char password[] = "Independent owner password 2026";
    memset(salt, 2, sizeof(salt));
    if (argc == 2) {
        if (strlen(argv[1]) != 32) return 2;
        for (unsigned i = 0; i < sizeof(salt); ++i) {
            unsigned value;
            if (sscanf(argv[1] + 2 * i, "%2x", &value) != 1) return 2;
            salt[i] = (uint8_t)value;
        }
    } else if (argc != 1) return 2;
    if (argon2id_hash_raw(3, 65536, 1, password, strlen(password), salt,
                         sizeof(salt), output, sizeof(output)) != ARGON2_OK)
        return 1;
    for (unsigned i = 0; i < sizeof(output); ++i) printf("%02x", output[i]);
    puts("");
    return 0;
}
