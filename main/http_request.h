#ifndef __HTTP_REQUEST_H__
#define __HTTP_REQUEST_H__

#include <stdbool.h>

#define API_HOST "83cd1aa837661fab941b2c5a2a65424b.jm.net.br"
#define API_PORT "2087"
#define API_BASE_PATH "/67ZlfPVt"

// #define MAIN_ROUTE "https://jalles.aclsconnect.com/67ZlfPVt" // Jalles Acls
#define MAIN_ROUTE "https://" API_HOST ":" API_PORT API_BASE_PATH // Jalles
// #define MAIN_ROUTE "http://77.37.126.66:3050/67ZlfPVt" // Jalles
// #define MAIN_ROUTE "http://192.168.1.111:8003/67ZlfPVt" // Local

// Enumeração para métodos HTTP
typedef enum { HTTP_POST, HTTP_PATCH } http_method_t;

typedef struct {
    char *ptr_buffer;
    int len;
} http_get_fw_buffer_t;

extern char token_global[700]; // Variável global para o token
extern int Connectado;         // Variável global para status de conexão

bool fazer_login(const char *usuario, const char *senha);
int server_request(const char *url, const char *body, http_method_t method);
int server_get_json(const char *full_url, char **out_buffer, int *out_len);

#endif // __HTTP_REQUEST_H__
