#include "config.h" //Configuração do projeto
#include "network_mode.h"

#define BLE_STARTUP_WINDOW_MS (30 * 1000U)
#define HTTP_MUTEX_WAIT_MS 20000U
#if (GTW_ROLE_TX_ONLY == 0)
#define PATCH_BACKOFF_MIN_MS 2000U
#define PATCH_BACKOFF_MAX_MS 30000U
#define PATCH_BACKOFF_JITTER_MS 1000U
#endif
#define GTW_RX_DUP_CACHE_SIZE 64

#if (GTW_ROLE_TX_ONLY == 0)

static PatchRequest patchPool[QUEUE_LENGTH]; // Array estático de objetos

typedef enum {
    PATCH_SLOT_FREE = 0,
    PATCH_SLOT_QUEUED,
    PATCH_SLOT_PROCESSING,
} patch_slot_state_t;

#endif

#if (GTW_ROLE_RX_ONLY == 0)
typedef struct {
    bool active;
    uint8_t msg_id;
    uint16_t tank_id;
    TaskHandle_t waiter;
} gtw_ack_wait_t;
#endif

#if (GTW_ROLE_TX_ONLY == 0)
typedef struct {
    bool used;
    uint16_t src_id;
    uint8_t msg_id;
    TickType_t accepted_at;
} gtw_rx_dup_t;
#endif

typedef struct {
    uint32_t timeout;
    uint32_t frame_ok;
    uint32_t incomplete;
    uint32_t bad_preamble;
    uint32_t bad_crc;
    uint32_t semantic_invalid;
    uint32_t wrong_dst;
    uint32_t ack_rx;
    uint32_t ack_match;
    uint32_t ack_unexpected;
    uint32_t ping_rx;
    uint32_t data_rx;
    uint32_t duplicate;
    uint32_t queue_accept;
    uint32_t queue_reject;
    uint32_t ack_sent;
} gtw_lora_rx_stats_t;

#if (GTW_ROLE_RX_ONLY == 0)
static SemaphoreHandle_t gtw_ack_mutex = NULL;
static SemaphoreHandle_t ws_lifecycle_mutex = NULL;
// Permanece ativo ate uma nova task WebSocket ser criada com sucesso.
static volatile bool websocket_restart_requested = false;
#endif
static SemaphoreHandle_t gtw_request_mutex = NULL;
#if (GTW_ROLE_TX_ONLY == 0)
static SemaphoreHandle_t patch_pool_mutex = NULL;
static patch_slot_state_t patchPoolState[QUEUE_LENGTH] = {0};
#endif
#if (GTW_ROLE_RX_ONLY == 0)
static gtw_ack_wait_t gtw_ack_wait = {0};
#endif
#if (GTW_ROLE_TX_ONLY == 0)
static gtw_rx_dup_t gtw_rx_dup_cache[GTW_RX_DUP_CACHE_SIZE] = {0};
#endif
static volatile int64_t gtw_last_lora_rx_ms = 0;
static volatile bool gtw_lora_ready = false;
static uint8_t gtw_lora_uart_error_streak = 0;
static uint8_t gtw_lora_transaction_failures = 0;
static volatile bool ble_config_mode_active = false;
static volatile bool ble_startup_window_active = false;
static TaskHandle_t ble_config_mode_task_handle = NULL;

static void gtw_lora_log_rx_stats(const gtw_lora_rx_stats_t *s, const char *motivo) {
    if (!s)
        return;

    ESP_LOGW(
        "GTW_LORA_STATS",
        "motivo=%s timeout=%lu frame_ok=%lu incomplete=%lu preamble=%lu crc=%lu semantic=%lu wrong_dst=%lu "
        "ack_rx=%lu ack_match=%lu ack_unexp=%lu ping=%lu data=%lu dup=%lu queue_ok=%lu queue_fail=%lu ack_sent=%lu",
        motivo, (unsigned long)s->timeout, (unsigned long)s->frame_ok, (unsigned long)s->incomplete,
        (unsigned long)s->bad_preamble, (unsigned long)s->bad_crc, (unsigned long)s->semantic_invalid,
        (unsigned long)s->wrong_dst, (unsigned long)s->ack_rx, (unsigned long)s->ack_match,
        (unsigned long)s->ack_unexpected, (unsigned long)s->ping_rx, (unsigned long)s->data_rx,
        (unsigned long)s->duplicate, (unsigned long)s->queue_accept, (unsigned long)s->queue_reject,
        (unsigned long)s->ack_sent);
}

void wifi_task(void *pv);

static void tentar_conectar_wifi(void);

void login_task(void *pvParameters);

static bool start_token_timer(void);

static void token_timer_cb(void *arg);

void ConnectRest();

#if (GTW_ROLE_RX_ONLY == 0)
void connect_to_websocket(void *pvParameters);
static bool websocket_task_start_if_needed(void);
static bool websocket_task_is_running(void);
static bool websocket_is_connected(void);

static void handle_bomba_control(cJSON *json);
#endif

void save_SIIDWifi(const char *device_name);

void save_PasswordWifi(const char *password);

void save_UsetGTW(const char *device_name);

void save_PasswordGTW(const char *password);

void save_idGtw(int state);

void save_idUnidadeGtw(int state);

#if (GTW_ROLE_RX_ONLY == 0)
static void ws_msg_processor_task(void *pv);

static void websocket_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data);
#endif

static void lora_setup_gtw(void);
static bool lora_apply_config_with_retries(int max_attempts, bool temporary);

void gtw_lora_rx_task(void *pvParameters);
static void gtw_lora_health_task(void *pvParameters);

uint16_t lora_crc16(const uint8_t *data, size_t len);
static size_t lora_frame_air_len(const lora_app_frame_t *frame);
static int lora_send_frame_air(lora_app_frame_t *frame);
static bool lora_received_frame_valid(const lora_app_frame_t *rx, int air_len, uint16_t *out_crc_calc,
                                      uint16_t *out_crc_rx);

#if (GTW_ROLE_TX_ONLY == 0)
bool queue_patch_request(const char *path, const char *body);
#endif

void vTaskImprimirUsoMemoria(void *pvParameters);

#if (GTW_ROLE_TX_ONLY == 0)
void gtw_send_ack(const lora_app_frame_t *rx);

void patch_task(void *pvParameters);
#endif

#if (GTW_ROLE_RX_ONLY == 0)
bool gtw_send_to_tank(uint16_t tank_id, const char *msg, uint16_t bomba_id);

static void handle_PingTanque(cJSON *json);

static void handle_BTOn(cJSON *json);

bool gtw_ping_tank(uint16_t tank_id);

bool ws_send_json(const char *json_msg);
#endif

void bt_message_received_callback(const char *message);

void bt_client_connected_callback(void);
void bt_client_disconnected_callback(void);

static void bt_send_config_snapshot(void);
static void ble_config_mode_task(void *pvParameters);
static void ble_startup_window_task(void *pvParameters);

void app_main(void) {
    ESP_LOGI(TAG, "Inicializando sistema...");
    ESP_LOGI(TAG, "Modo de operacao: %s (TX_ONLY=%d RX_ONLY=%d)", GTW_ROLE_NAME, GTW_ROLE_TX_ONLY, GTW_ROLE_RX_ONLY);

    /****************************************
     * 1️⃣ Inicializa NVS
     ****************************************/
    esp_err_t errNVS = nvs_flash_init();

    if (errNVS == ESP_ERR_NVS_NO_FREE_PAGES || errNVS == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_LOGW(TAG, "NVS corrompida ou cheia. Apagando...");
        ESP_ERROR_CHECK(nvs_flash_erase());
        errNVS = nvs_flash_init();
    }

    if (errNVS != ESP_OK) {
        ESP_LOGE(TAG, "Erro ao inicializar NVS: %s", esp_err_to_name(errNVS));
        ConnectRest();
    }

    /****************************************
     * 2️⃣ Criação de Filas
     ****************************************/

    // Pool de índices livres
#if (GTW_ROLE_TX_ONLY == 0)
    xPatchFreeQueue = xQueueCreate(QUEUE_LENGTH, sizeof(uint8_t));
    if (!xPatchFreeQueue) {
        ESP_LOGE(TAG, "Erro ao criar xPatchFreeQueue");
        ConnectRest();
    }

    // Preenche com índices (0..QUEUE_LENGTH-1)
    for (uint8_t i = 0; i < QUEUE_LENGTH; i++) {
        xQueueSend(xPatchFreeQueue, &i, 0);
    }

    // Fila principal de Patch Requests
    xPatchQueue = xQueueCreate(QUEUE_LENGTH, sizeof(PatchRequest *));
    if (!xPatchQueue) {
        ESP_LOGE(TAG, "Erro ao criar xPatchQueue");
        ConnectRest();
    }
#else
    ESP_LOGI(TAG, "Modo TX_ONLY: fila PATCH nao criada");
#endif

#if (GTW_ROLE_RX_ONLY == 0)
    // Fila mensagens WebSocket
    ws_msg_queue = xQueueCreate(WS_QUEUE_LEN, sizeof(ws_msg_item_t));
    if (!ws_msg_queue) {
        ESP_LOGE(TAG, "Erro ao criar ws_msg_queue");
        ConnectRest();
    }
#else
    ESP_LOGI(TAG, "Modo RX_ONLY: fila WebSocket nao criada");
#endif

    /****************************************
     * 3️⃣ Mutex
     ****************************************/
    MutexHTTP = xSemaphoreCreateMutex();
    if (!MutexHTTP) {
        ESP_LOGE(TAG, "Falha ao criar MutexHTTP");
        ConnectRest();
    }

    MutexLora = xSemaphoreCreateMutex();
    if (!MutexLora) {
        ESP_LOGE(TAG, "Falha ao criar MutexLora");
        ConnectRest();
    }

#if (GTW_ROLE_RX_ONLY == 0)
    gtw_ack_mutex = xSemaphoreCreateMutex();
    ws_lifecycle_mutex = xSemaphoreCreateMutex();
#endif
    gtw_request_mutex = xSemaphoreCreateMutex();
#if (GTW_ROLE_TX_ONLY == 0)
    patch_pool_mutex = xSemaphoreCreateMutex();
#endif
    if (!gtw_request_mutex
#if (GTW_ROLE_RX_ONLY == 0)
        || !gtw_ack_mutex || !ws_lifecycle_mutex
#endif
#if (GTW_ROLE_TX_ONLY == 0)
        || !patch_pool_mutex
#endif
    ) {
        ESP_LOGE(TAG, "Falha ao criar controle de ACK LoRa");
        ConnectRest();
    }

    /****************************************
     * 4️⃣ Credenciais: NVS primeiro, valores fixos como fallback
     ****************************************/

    load_save_SIIDWifi(wifi_ssid, sizeof(wifi_ssid));
    load_PasswordWifi(wifi_password, sizeof(wifi_password));
    load_save_UsetGTW(userNameHTTPs, sizeof(userNameHTTPs));
    load_PasswordGTW(passwordHTTPs, sizeof(passwordHTTPs));
    ID_GATEWAY = load_idGTW();
    DEVICE_ID = load_idUnidadeGTW();

    if (DEVICE_ID > 0 && ID_GATEWAY != DEVICE_ID) {
        ESP_LOGW(TAG, "IDs antigos divergentes (gateway=%d device=%d); usando DEVICE_ID=%d", ID_GATEWAY, DEVICE_ID,
                 DEVICE_ID);
        ID_GATEWAY = DEVICE_ID;
        save_idGtw(ID_GATEWAY);
    } else if (DEVICE_ID == 0 && ID_GATEWAY > 0) {
        DEVICE_ID = ID_GATEWAY;
        save_idUnidadeGtw(DEVICE_ID);
    }

    // if (wifi_ssid[0] == '\0') {
    //     strncpy(wifi_ssid, "Acls_R", sizeof(wifi_ssid));
    // }
    // if (wifi_password[0] == '\0') {
    //     strncpy(wifi_password, "Acls@1234", sizeof(wifi_password));
    // }
    // if (userNameHTTPs[0] == '\0') {
    //     strncpy(userNameHTTPs, "ACLSGTWTEST", sizeof(userNameHTTPs));
    // }
    // if (passwordHTTPs[0] == '\0') {
    //     strncpy(passwordHTTPs, "Acls@123", sizeof(passwordHTTPs));
    // }
    // if (ID_GATEWAY == 0) {
    //     ID_GATEWAY = 3;
    // }
    // if (DEVICE_ID == 0) {
    //     DEVICE_ID = ID_GATEWAY;
    // }

    if (wifi_ssid[0] == '\0' || wifi_password[0] == '\0' || ID_GATEWAY == 0 || DEVICE_ID == 0 ||
        userNameHTTPs[0] == '\0' || passwordHTTPs[0] == '\0') {

#if DEBUG_MODE
        ESP_LOGE("Main", "Algumas configurações não estão configuradas! Aguardando configuração via Bluetooth.");
#endif
        int initBT = 0;

        while (wifi_ssid[0] == '\0' || wifi_password[0] == '\0' || ID_GATEWAY == 0 || DEVICE_ID == 0 ||
               userNameHTTPs[0] == '\0' || passwordHTTPs[0] == '\0') {

            if (initBT == 0) {
                bluetooth_config_start(0);
                printf("Bluetooth iniciado para configuração.\n");
            }

            if (wifi_ssid[0] == '\0') {
                ESP_LOGE("NVS", "Wifi_ssid não configurado.");
            }
            if (wifi_password[0] == '\0') {
                ESP_LOGE("NVS", "Wifi_password não configurado.");
            }

            if (ID_GATEWAY == 0) {
                ESP_LOGE("NVS", "ID_GATEWAY não configurado.");
            }
            if (DEVICE_ID == 0) {
                ESP_LOGE("NVS", "ID_UNIDADE não configurado.");
            }
            if (userNameHTTPs[0] == '\0') {
                ESP_LOGE("NVS", "userNameHTTPs não configurado.");
            }
            if (passwordHTTPs[0] == '\0') {
                ESP_LOGE("NVS", "passwordHTTPs não configurado.");
            }

            if (initBT == 0) {
                initBT++;
            }

            vTaskDelay(pdMS_TO_TICKS(5000)); // Aguardar 5 segundo antes de verificar novamente
        }
    }

    float firmware_salvo = nvs_resgatar_float(NVS_KEY_Fversion);
    if (firmware_salvo > 0) // Verifica se há algum valor de firmware dentro do NVS
    {
        Firmware_version = firmware_salvo;
        ESP_LOGI("NVS", ">>>>>>>>>>>>>>>>>>>>>> Firmware_version : %f", Firmware_version);
    }

    /****************************************
     * 5️⃣ Configuração LoRa
     ****************************************/
    ESP_LOGI(TAG, "Configurando LoRa...");
    lora_setup_gtw();

    ESP_LOGI(TAG, "BLE aberto no boot por 30 segundos; Wi-Fi inicia depois da janela BLE");
    ble_startup_window_active = true;

    esp_err_t ble_start_err = bluetooth_config_start(BLE_STARTUP_WINDOW_MS);
    if (ble_start_err != ESP_OK) {
        ble_startup_window_active = false;
        ESP_LOGE(TAG, "Falha ao abrir BLE no boot: %s; Wi-Fi sera iniciado", esp_err_to_name(ble_start_err));
    } else if (xTaskCreate(ble_startup_window_task, "ble_boot_wait", 3072, NULL, 4, NULL) != pdPASS) {
        ble_startup_window_active = false;
        bluetooth_config_stop();
        ESP_LOGE(TAG, "Falha ao criar task da janela BLE; Wi-Fi sera iniciado");
    }

    /****************************************
     * 6️⃣ Tasks
     ****************************************/
    ESP_LOGI(TAG, "Criando tasks...");

    if (xTaskCreate(wifi_task, "Conecta_Wifi", 4096, NULL, 5, &taskConecta_WIFI) != pdPASS) {
        ESP_LOGE(TAG, "Falha ao criar task WiFi");
        ConnectRest();
    }

    if (xTaskCreate(gtw_lora_rx_task, "gtw_lora_rx_task", 4096, NULL, 5, NULL) != pdPASS) {
        ESP_LOGE(TAG, "Falha ao criar task LoRa RX");
        ConnectRest();
    }

    if (xTaskCreate(gtw_lora_health_task, "gtw_lora_health", 3072, NULL, 4, NULL) != pdPASS) {
        ESP_LOGE(TAG, "Falha ao criar task de saúde LoRa");
        ConnectRest();
    }

#if (GTW_ROLE_RX_ONLY == 0)
    if (xTaskCreate(ws_msg_processor_task, "ws_msg_proc", 9216, NULL, 4, NULL) != pdPASS) {
        ESP_LOGE(TAG, "Falha ao criar task WebSocket Processor");
        ConnectRest();
    }
#else
    ESP_LOGW(TAG, "Modo RX_ONLY: WebSocket Processor desativado");
#endif

    if (xTaskCreate(vTaskImprimirUsoMemoria, "MonitorMemoria", 6144, NULL, 3, &taskImprimirUsoMemoria) != pdPASS) {
        ESP_LOGE(TAG, "Falha ao criar task Monitor de Memória");
        ConnectRest();
    }

    /****************************************
     * 7️⃣ Sistema inicializado
     ****************************************/
    ESP_LOGI(TAG, "Sistema inicializado com sucesso.");
}

static void tentar_conectar_wifi(void) {
    if (ble_config_mode_active || ble_startup_window_active) {
        ESP_LOGW(TAG, "Janela BLE ativa; Wi-Fi nao sera iniciado");
        return;
    }

    if (!wifi_start_driver()) {
        ESP_LOGE(TAG, "Falha ao iniciar driver Wi-Fi");
        return;
    }

    if (usando_secundario && wifi_secundario) {
        ESP_LOGI(TAG, "Conectando ao Wi-Fi SECUNDÁRIO (%s)", WIFI_SECONDARY_SSID);
        wifi_connect_credentials(WIFI_SECONDARY_SSID, WIFI_SECONDARY_PASS, WIFI_CONNECT_TIMEOUT_MS);
    } else {
        ESP_LOGI(TAG, "Conectando ao Wi-Fi PRIMÁRIO (%s)", wifi_ssid);
        wifi_connect_credentials(wifi_ssid, wifi_password, WIFI_CONNECT_TIMEOUT_MS);
    }
}

void wifi_task(void *pv) {
    ESP_LOGI(TAG, "Iniciando rotina de Wi-Fi...");
    esp_task_wdt_add(NULL);
    tentar_conectar_wifi();

    static int falhas_wifi = 0;
    static int falhas_ip = 0;
    static int falhas_internet = 0;
    static int ciclos_religar = 0;
    int64_t inicio_falha_internet_ms = 0;
    int wifi_4G = 0;

    while (1) {
        esp_task_wdt_reset();
        ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(WIFI_MONITOR_INTERVAL_MS));

        if (ble_config_mode_active || ble_startup_window_active) {
            if (wifi_is_active()) {
                ESP_LOGW(TAG, "Janela BLE ativa; desligando Wi-Fi");
                wifi_stop_driver();
            }
            Connectado = 0;
            continue;
        }

        if (!wifi_is_active()) {
            ESP_LOGW(TAG, "Driver Wi-Fi inativo — reiniciando driver...");
            tentar_conectar_wifi();

            if (!wifi_is_active()) {
                ciclos_religar++;
                ESP_LOGE(TAG, "Driver Wi-Fi nao iniciou (%d/%d)", ciclos_religar, WIFI_CYCLE_MAX_RETRIES);
                if (ciclos_religar >= WIFI_CYCLE_MAX_RETRIES) {
                    ESP_LOGE(TAG, "Driver Wi-Fi permaneceu inativo; reiniciando ESP32");
                    esp_restart();
                }
            } else {
                falhas_wifi = 0;
            }
            continue;
        }

        // === Está conectado ao AP? ===
        if (wifi_sta_connected()) {
            falhas_wifi = 0;

            if (!wifi_sta_has_ip()) {
                Connectado = 0;
                falhas_internet = 0;
                inicio_falha_internet_ms = 0;
                falhas_ip++;
                ESP_LOGW(TAG, "Associado ao roteador, mas sem IP (%d/%d)", falhas_ip, WIFI_IP_MAX_FAILS);

                if (falhas_ip >= WIFI_IP_MAX_FAILS) {
                    ESP_LOGW(TAG, "Sem IP por 60 segundos; reiniciando driver Wi-Fi");
                    wifi_stop_driver();
                    vTaskDelay(pdMS_TO_TICKS(WIFI_DRIVER_RESTART_DELAY_MS));

                    if (wifi_secundario)
                        usando_secundario = !usando_secundario;

                    tentar_conectar_wifi();
                    falhas_ip = 0;
                    ciclos_religar++;
                    if (ciclos_religar >= WIFI_CYCLE_MAX_RETRIES) {
                        ESP_LOGE(TAG, "Sem IP apos %d ciclos; reiniciando ESP32", ciclos_religar);
                        esp_restart();
                    }
                }
                continue;
            }

            falhas_ip = 0;
            ciclos_religar = 0;

            // A task de login precisa existir antes do WebSocket, pois e ela que obtem o token.
            // Portanto, sua criacao depende da rede/IP e nao da flag Connectado do TX.
            if (Task_login_task == NULL) {
                if (xTaskCreate(login_task, "login_task", 1024 * 8, NULL, 5, &Task_login_task) == pdPASS) {
                    ESP_LOGI(TAG, "Task de login criada; aguardando API para obter o token");
                } else {
                    Task_login_task = NULL;
                    ESP_LOGE(TAG, "Falha ao criar task de login; nova tentativa no proximo ciclo");
                }
            }

            bool websocket_conectado = false;
            bool internet_google = false;

#if (GTW_ROLE_RX_ONLY == 0)
            websocket_conectado = websocket_is_connected();
#endif

            // No TX, online significa exclusivamente WebSocket conectado.
            Connectado = websocket_conectado ? 1 : 0;

            if (!websocket_conectado) {
                internet_google = wifi_check_internet(INTERNET_CHECK_TIMEOUT_MS);
            }

            if (websocket_conectado || internet_google) {
                falhas_internet = 0;
                inicio_falha_internet_ms = 0;
#if DEBUG_MODE
                if (websocket_conectado) {
                    ESP_LOGI(TAG, "TX online: WebSocket conectado");
                } else {
                    ESP_LOGW(TAG, "Internet confirmada pelo Google, mas TX offline: WebSocket desconectado");
                }
#endif

                if (wifi_secundario) {
                    if (TokenOk == 1 && wifi_secundario_ativo == 1 && wifi_4G == 0) {
                        wifi_4G = 1;
                    }
                }
            } else {
                Connectado = 0;
                falhas_internet++;
                int64_t agora_ms = esp_timer_get_time() / 1000;
                if (inicio_falha_internet_ms == 0)
                    inicio_falha_internet_ms = agora_ms;

                uint32_t sem_internet_ms = (uint32_t)(agora_ms - inicio_falha_internet_ms);
                ESP_LOGW(TAG, "Wi-Fi associado, mas sem internet ha %lu s (falha %d)",
                         (unsigned long)(sem_internet_ms / 1000U), falhas_internet);

                if (sem_internet_ms >= WIFI_INTERNET_RESTART_DELAY_MS) {
                    ESP_LOGW(TAG, "Sem internet por 5 minutos; reiniciando somente o driver Wi-Fi");
                    falhas_internet = 0;
                    inicio_falha_internet_ms = 0;

                    wifi_stop_driver();
                    vTaskDelay(pdMS_TO_TICKS(WIFI_DRIVER_RESTART_DELAY_MS));

                    if (wifi_secundario) {
                        usando_secundario = !usando_secundario;
                        if (!usando_secundario) {
                            wifi_4G = 0;
                        }
                    }

                    tentar_conectar_wifi();
                }
            }
        } else {
            // Não conectado ao AP
            Connectado = 0;
            falhas_ip = 0;
            falhas_internet = 0;
            inicio_falha_internet_ms = 0;
            falhas_wifi++;
            ESP_LOGW(TAG, "Wi-Fi desconectado (%d/%d)", falhas_wifi, WIFI_CONNECT_MAX_FAILS);

            if (falhas_wifi < WIFI_CONNECT_MAX_FAILS) {
                tentar_conectar_wifi();
                continue;
            }

            if (falhas_wifi >= WIFI_CONNECT_MAX_FAILS) {
                ESP_LOGE(TAG, "Falhou reconexão Wi-Fi — reiniciando ciclo");

                wifi_stop_driver();

                vTaskDelay(pdMS_TO_TICKS(WIFI_DRIVER_RESTART_DELAY_MS));

                if (wifi_secundario)
                    usando_secundario = !usando_secundario;

                tentar_conectar_wifi();

                falhas_wifi = 0;
                ciclos_religar++;
            }
        }

        if (ciclos_religar >= WIFI_CYCLE_MAX_RETRIES) {
            ESP_LOGE(TAG, "❌ Falhou após %d ciclos — Reiniciando ESP32", ciclos_religar);
            esp_restart();
        }
    }
}

// Callback do timer: notifica a task
static void token_timer_cb(void *arg) {
    if (Task_login_task != NULL) {
        xTaskNotify(Task_login_task, 1, eSetValueWithOverwrite);
    }
}

static bool start_token_timer(void) {
    if (token_timer == NULL) {
        const esp_timer_create_args_t timer_args = {
            .callback = &token_timer_cb, .arg = NULL, .dispatch_method = ESP_TIMER_TASK, .name = "token_timer"};

        esp_err_t create_err = esp_timer_create(&timer_args, &token_timer);
        if (create_err != ESP_OK) {
            ESP_LOGE(TAG, "Falha ao criar timer do token: %s", esp_err_to_name(create_err));
            token_timer = NULL;
            return false;
        }
    } else {
        esp_err_t stop_err = esp_timer_stop(token_timer);
        if (stop_err != ESP_OK && stop_err != ESP_ERR_INVALID_STATE) {
            ESP_LOGW(TAG, "Falha ao parar timer anterior do token: %s", esp_err_to_name(stop_err));
        }
    }

    esp_err_t start_err = esp_timer_start_once(token_timer, 20ULL * 3600ULL * 1000000ULL);
    if (start_err != ESP_OK) {
        ESP_LOGE(TAG, "Falha ao iniciar timer de renovacao do token: %s", esp_err_to_name(start_err));
        return false;
    }

    ESP_LOGI(TAG, "Proxima renovacao do token agendada para daqui a 20 horas");
    return true;
}

void login_task(void *pvParameters) {

    Task_login_task = xTaskGetCurrentTaskHandle(); // salva o handle
    esp_task_wdt_add(NULL);

    while (1) {
        esp_task_wdt_reset();
#if DEBUG_MODE
        UBaseType_t stack_remain = uxTaskGetStackHighWaterMark(NULL);
        ESP_LOGI("STACK", "\033[1;35m*** Task [%s] - mínimo livre: %u words (~%u bytes) ***\033[0m",
                 pcTaskGetName(NULL), stack_remain, stack_remain * sizeof(StackType_t));
#endif

        if (!wifi_sta_connected() || !wifi_sta_has_ip()) {
            ESP_LOGI(TAG, "Aguardando Wi-Fi e IP para obter ou renovar o token...");
            vTaskDelay(pdMS_TO_TICKS(10000));
            continue;
        }

        bool login_ok = false;
        if (MutexHTTP == NULL) {
            ESP_LOGW(TAG, "MutexHTTP nulo; login sem mutex");
            login_ok = fazer_login(userNameHTTPs, passwordHTTPs);
        } else if (xSemaphoreTake(MutexHTTP, pdMS_TO_TICKS(HTTP_MUTEX_WAIT_MS)) == pdTRUE) {
            login_ok = fazer_login(userNameHTTPs, passwordHTTPs);
            xSemaphoreGive(MutexHTTP);
        } else {
            ESP_LOGW(TAG, "Login aguardando HTTP livre; MutexHTTP ocupado");
        }
        esp_task_wdt_reset();

        if (login_ok) {
            ESP_LOGI(TAG, "Login bem-sucedido; token novo confirmado");
            TokenOk = 1;

#if (GTW_ROLE_RX_ONLY == 0)
            if (!websocket_task_is_running()) {
                if (!websocket_task_start_if_needed()) {
                    ESP_LOGE(TAG, "Falha ao criar task do WebSocket");
                }
            } else {
                ESP_LOGW(TAG, "Task de WebSocket já está em execução.");
                Connectado = 0;
                stop_websocket_task = true;
                for (int i = 0; i < 400 && websocket_task_is_running(); i++) {
                    if ((i % 100) == 0)
                        esp_task_wdt_reset();
                    vTaskDelay(pdMS_TO_TICKS(100));
                }
#if DEBUG_MODE
                ESP_LOGE("Login", "Reiniciando a task de WebSocket...\n");
#endif

                if (!websocket_task_is_running()) {
                    if (!websocket_task_start_if_needed()) {
                        ESP_LOGE(TAG, "Falha ao recriar task do WebSocket");
                    }
                }
            }

            // Em vez de delay de 20h → espera notificação do timer
#else
            ESP_LOGI(TAG, "Modo RX_ONLY: WebSocket desativado; login mantido para PATCH");
#endif

#if (GTW_ROLE_TX_ONLY == 0)
            if (task_patch == NULL) {
#if (GTW_ROLE_RX_ONLY == 0)
                for (int i = 0; i < 80 && taskConnect_to_websocket != NULL; i++) {
                    if (ws_client != NULL && esp_websocket_client_is_connected(ws_client)) {
                        break;
                    }
                    vTaskDelay(pdMS_TO_TICKS(100));
                }

#endif
                xTaskCreate(patch_task, "patch_task", 1024 * 8, NULL, 5, &task_patch);
            }
#else
            ESP_LOGI(TAG, "Modo TX_ONLY: patch_task desativada");
#endif

            bool refresh_requested_while_scheduling = false;
            while (!start_token_timer()) {
                ESP_LOGW(TAG, "Timer do token indisponivel; novo agendamento em 30s");

                uint32_t notified_while_scheduling = 0;
                if (xTaskNotifyWait(0, UINT32_MAX, &notified_while_scheduling, pdMS_TO_TICKS(30000)) == pdTRUE) {
                    ESP_LOGW(TAG, "Renovacao antecipada do token solicitada");
                    refresh_requested_while_scheduling = true;
                    break;
                }
                esp_task_wdt_reset();
            }

            if (!refresh_requested_while_scheduling) {
                uint32_t notified;
                while (xTaskNotifyWait(0, UINT32_MAX, &notified, pdMS_TO_TICKS(30000)) != pdTRUE)
                    esp_task_wdt_reset();
            }

            ESP_LOGI(TAG, "Renovacao solicitada; token e WebSocket atuais permanecem ativos");
            continue;
        } else {
            if (TokenOk == 1 && token_global[0] != '\0') {
                ESP_LOGW(TAG, "Renovacao falhou; token anterior mantido. Nova tentativa em 30s");
            } else {
                ESP_LOGW(TAG, "Login falhou; nova tentativa em 30s");
            }
            esp_task_wdt_reset();
            vTaskDelay(pdMS_TO_TICKS(30000)); // 30s
        }
    }
}

/*############################################## WebSocket ################################################*/

// Função principal da tarefa de conexão WebSocket
#if (GTW_ROLE_RX_ONLY == 0)
static bool websocket_task_is_running(void) {
    if (!ws_lifecycle_mutex || xSemaphoreTake(ws_lifecycle_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return true;

    bool running = taskConnect_to_websocket != NULL;
    xSemaphoreGive(ws_lifecycle_mutex);
    return running;
}

static bool websocket_is_connected(void) {
    if (!ws_lifecycle_mutex || xSemaphoreTake(ws_lifecycle_mutex, pdMS_TO_TICKS(100)) != pdTRUE)
        return false;

    bool connected = ws_client != NULL && esp_websocket_client_is_connected(ws_client);
    xSemaphoreGive(ws_lifecycle_mutex);
    return connected;
}

static bool websocket_task_start_if_needed(void) {
    if (TokenOk != 1 || token_global[0] == '\0' || !wifi_sta_connected() || !wifi_sta_has_ip())
        return false;

    if (!ws_lifecycle_mutex || xSemaphoreTake(ws_lifecycle_mutex, pdMS_TO_TICKS(2000)) != pdTRUE)
        return false;

    bool ok = true;
    if (taskConnect_to_websocket == NULL) {
        Connectado = 0;
        stop_websocket_task = false;
        if (xTaskCreate(connect_to_websocket, "connect_to_websocket", 2048 * 6, NULL, 5, &taskConnect_to_websocket) !=
            pdPASS) {
            taskConnect_to_websocket = NULL;
            ok = false;
        } else {
            websocket_restart_requested = false;
        }
    }

    xSemaphoreGive(ws_lifecycle_mutex);
    return ok;
}

static bool websocket_wait_before_setup_retry(uint32_t *setup_failure_ms) {
    Connectado = 0;

    if (stop_websocket_task) {
        return false;
    }

    *setup_failure_ms += WS_RETRY_DELAY_MS;
    if (*setup_failure_ms >= WS_RECREATE_AFTER_MS) {
        ESP_LOGE(TAG, "Inicializacao WebSocket falhou por %lu s; destruindo task para recriar",
                 (unsigned long)(*setup_failure_ms / 1000U));
        websocket_restart_requested = true;
        stop_websocket_task = true;
        return false;
    }

    vTaskDelay(pdMS_TO_TICKS(WS_RETRY_DELAY_MS));
    return true;
}

void connect_to_websocket(void *pvParameters) {

    static char tmp[900];
    static char ws_path[800];
    esp_task_wdt_add(NULL);
    uint32_t setup_failure_ms = 0;

    while (!stop_websocket_task) {
        esp_task_wdt_reset();
        // Com token valido, continua tentando mesmo se o teste externo do Google falhar temporariamente.
        if (TokenOk != 1 || token_global[0] == '\0' || !wifi_sta_connected() || !wifi_sta_has_ip()) {
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }

        snprintf(ws_path, sizeof(ws_path), "/ws-native/native-ws?token=%s", token_global);
        snprintf(tmp, sizeof(tmp), "wss://" API_HOST ":" API_PORT "%s", ws_path);
        char *local_url = strdup(tmp);

        if (!local_url) {
            ESP_LOGE("WS", "Sem memoria para a URL do WebSocket; nova tentativa em 5s");
            if (!websocket_wait_before_setup_retry(&setup_failure_ms))
                break;
            continue;
        }

        gtw_websocket_transport_t network_transport = {0};
        esp_err_t transport_err = gtw_websocket_transport_init(&network_transport, ws_path, rootCaCerticate);
        if (transport_err != ESP_OK) {
            ESP_LOGE(TAG, "Falha ao preparar transporte WebSocket %s: %s", GTW_IP_VERSION_NAME,
                     esp_err_to_name(transport_err));
            free(local_url);
            if (!websocket_wait_before_setup_retry(&setup_failure_ms))
                break;
            continue;
        }

        esp_websocket_client_config_t websocket_cfg = {
            .uri = local_url,
            .ext_transport = network_transport.websocket,
            .reconnect_timeout_ms = 5000,
            .network_timeout_ms = 30000,
            .disable_auto_reconnect = false,
        };

        esp_websocket_client_handle_t local_client = esp_websocket_client_init(&websocket_cfg);
        if (local_client == NULL) {
            ESP_LOGE(TAG, "Falha ao inicializar WebSocket; nova tentativa em 5s");
            gtw_websocket_transport_cleanup(&network_transport);
            free(local_url);
            if (!websocket_wait_before_setup_retry(&setup_failure_ms))
                break;
            continue;
        }

        esp_err_t event_err = esp_websocket_register_events(local_client, WEBSOCKET_EVENT_ANY, websocket_event_handler,
                                                            (void *)ws_msg_queue);
        if (event_err != ESP_OK) {
            ESP_LOGE(TAG, "Falha ao registrar eventos WebSocket: %s", esp_err_to_name(event_err));
            esp_websocket_client_destroy(local_client);
            gtw_websocket_transport_cleanup(&network_transport);
            free(local_url);
            if (!websocket_wait_before_setup_retry(&setup_failure_ms))
                break;
            continue;
        }

        xSemaphoreTake(ws_lifecycle_mutex, portMAX_DELAY);
        ws_client = local_client;
        websocket_url = local_url;
        xSemaphoreGive(ws_lifecycle_mutex);

        esp_err_t start_err = esp_websocket_client_start(local_client);
        if (start_err != ESP_OK) {
            ESP_LOGE(TAG, "Erro ao iniciar WebSocket: %s; nova tentativa em 5s", esp_err_to_name(start_err));
            xSemaphoreTake(ws_lifecycle_mutex, portMAX_DELAY);
            ws_client = NULL;
            websocket_url = NULL;
            xSemaphoreGive(ws_lifecycle_mutex);
            esp_websocket_client_destroy(local_client);
            gtw_websocket_transport_cleanup(&network_transport);
            free(local_url);
            if (!websocket_wait_before_setup_retry(&setup_failure_ms))
                break;
            continue;
        }

        setup_failure_ms = 0;
        ESP_LOGI(TAG, "Cliente WebSocket iniciado; aguardando conexao...");
        uint32_t disconnected_log_ms = 0;
        uint32_t disconnected_total_ms = 0;

        while (!stop_websocket_task) {
            esp_task_wdt_reset();
            if (esp_websocket_client_is_connected(local_client)) {
                disconnected_log_ms = 0;
                disconnected_total_ms = 0;
            } else {
                disconnected_log_ms += 1000U;
                disconnected_total_ms += 1000U;

                if (disconnected_log_ms >= WS_RETRY_DELAY_MS) {
                    ESP_LOGW(TAG, "WebSocket desconectado ha %lu s; reconexao automatica continua ativa",
                             (unsigned long)(disconnected_total_ms / 1000U));
                    disconnected_log_ms = 0;
                }

                if (disconnected_total_ms >= WS_RECREATE_AFTER_MS) {
                    ESP_LOGE(TAG, "WebSocket nao reconectou em %lu s; destruindo cliente e task para recriar",
                             (unsigned long)(WS_RECREATE_AFTER_MS / 1000U));
                    websocket_restart_requested = true;
                    stop_websocket_task = true;
                    break;
                }
            }

            vTaskDelay(pdMS_TO_TICKS(1000));
        }

        ESP_LOGI(TAG, "Encerrando instancia atual do WebSocket...");
        Connectado = 0;
        xSemaphoreTake(ws_lifecycle_mutex, portMAX_DELAY);
        esp_websocket_client_stop(local_client);
        esp_websocket_client_destroy(local_client);
        if (ws_client == local_client)
            ws_client = NULL;
        if (websocket_url == local_url)
            websocket_url = NULL;
        xSemaphoreGive(ws_lifecycle_mutex);

        gtw_websocket_transport_cleanup(&network_transport);
        free(local_url);

        if (!stop_websocket_task) {
            vTaskDelay(pdMS_TO_TICKS(WS_RETRY_DELAY_MS));
        }
    }

    ESP_LOGI(TAG, "Task WebSocket encerrada por solicitacao");
    Connectado = 0;
    xSemaphoreTake(ws_lifecycle_mutex, portMAX_DELAY);
    taskConnect_to_websocket = NULL;
    xSemaphoreGive(ws_lifecycle_mutex);
    esp_task_wdt_delete(NULL);
    vTaskDelete(NULL);
}

static void websocket_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data) {
    QueueHandle_t q = (QueueHandle_t)arg;
    esp_websocket_event_data_t *data = (esp_websocket_event_data_t *)event_data;

    if (!q || !data)
        return;

    switch (event_id) {
    case WEBSOCKET_EVENT_CONNECTED:
        Connectado = 1;
#if DEBUG_MODE
        ESP_LOGI(TAG_Websocket, "WebSocket conectado");
#endif
        break;

    case WEBSOCKET_EVENT_DISCONNECTED:
        Connectado = 0;
#if DEBUG_MODE
        ESP_LOGW(TAG_Websocket, "WebSocket desconectado; reconexao automatica mantida");
#endif
        break;

    case WEBSOCKET_EVENT_DATA:
        if (data->op_code == 0x1) {
#if DEBUG_MODE
            ESP_LOGI("Websoket", "______________________mensagem\n");
#endif
            // texto
            if (data->data_len == 0 || data->data_len >= WS_MSG_MAX_LEN) {
#if DEBUG_MODE
                ESP_LOGE(TAG_Websocket, "Mensagem muito grande: %d bytes", data->data_len);
#endif
                break;
            }

            ws_msg_item_t item = {0};

            // aloca buffer
            item.data = malloc(data->data_len + 1);
            if (!item.data) {
                ESP_LOGE(TAG_Websocket, "Sem memória p/ buffer WS");
                return;
            }

            memcpy(item.data, data->data_ptr, data->data_len);
            item.data[data->data_len] = '\0';
            item.len = data->data_len;

            if (xQueueSend(q, &item, 0) != pdTRUE) {
                ESP_LOGW(TAG_Websocket, "Fila WS cheia — mensagem descartada");
                free(item.data);
            }
        }
        break;

    case WEBSOCKET_EVENT_ERROR:
        Connectado = 0;
        ESP_LOGE(TAG_Websocket,
                 "Erro WebSocket: tipo=%d http=%d tls=0x%x stack=0x%x errno=%d",
                 data->error_handle.error_type, data->error_handle.esp_ws_handshake_status_code,
                 (unsigned)data->error_handle.esp_tls_last_esp_err,
                 (unsigned)data->error_handle.esp_tls_stack_err,
                 data->error_handle.esp_transport_sock_errno);

        if (data->error_handle.esp_ws_handshake_status_code == 401 ||
            data->error_handle.esp_ws_handshake_status_code == 403) {
            ESP_LOGW(TAG_Websocket, "Token recusado pelo WebSocket; solicitando novo login");
            token_global[0] = '\0';
            TokenOk = 0;
            websocket_restart_requested = true;
            stop_websocket_task = true;
            if (Task_login_task != NULL)
                xTaskNotifyGive(Task_login_task);
        }
        break;

    default:
        break;
    }
}

static void ws_msg_processor_task(void *pv) {
    ws_msg_item_t item;
    esp_task_wdt_add(NULL);
    while (1) {
        esp_task_wdt_reset();
        if (xQueueReceive(ws_msg_queue, &item, pdMS_TO_TICKS(2000)) == pdTRUE) {

#if DEBUG_MODE
            ESP_LOGI("Websoket", "______________________Mensagem WS recebida: %s\n", item.data);
#endif
            cJSON *json = cJSON_Parse(item.data);

            // liberar o buffer da mensagem sempre
            if (!json) {
                ESP_LOGW(TAG_Websocket, "JSON inválido recebido");
                free(item.data); // ✅ libera aqui
                continue;
            }
            cJSON *event_item = cJSON_GetObjectItemCaseSensitive(json, "event");
            if (!event_item || !cJSON_IsString(event_item)) {
                cJSON_Delete(json);
                free(item.data);
                continue;
            }

            const char *event_type = event_item->valuestring;

            if (strcmp(event_type, "reset_gtw") == 0) {
                // handle_reset_command(json);
            } else if (strcmp(event_type, "ControlerBomba_edit") == 0) {
                handle_bomba_control(json);

                ESP_LOGI(TAG_Websocket, "Comando de controle de bomba recebido");
            } else if (strcmp(event_type, "PingGtw") == 0) {
                handle_PingTanque(json);

                ESP_LOGI(TAG_Websocket, "Ping recebido");
            } else if (strcmp(event_type, "BtON_gateway") == 0) {
                handle_BTOn(json);

                ESP_LOGI(TAG_Websocket, "BT recebido");
            } else {
                ESP_LOGW(TAG_Websocket, "Evento desconhecido: %s", event_type);
            }

            cJSON_Delete(json);
            free(item.data);
        }
        // else timeout: volta checando stop flag
    }

    ESP_LOGI(TAG_Websocket, "ws_msg_processor_task finalizando");
    vTaskDelete(NULL);
}

static void handle_bomba_control(cJSON *json) {

    cJSON *data_obj = cJSON_GetObjectItemCaseSensitive(json, "data");
    if (data_obj == NULL) {
#if DEBUG_MODE
        ESP_LOGE(TAG_Websocket, "Erro: 'data' não encontrado no JSON");
#endif
        return;
    }

    cJSON *idBomba = cJSON_GetObjectItemCaseSensitive(data_obj, "bombaId");
    cJSON *idControle = cJSON_GetObjectItemCaseSensitive(data_obj, "id");
    cJSON *comando = cJSON_GetObjectItemCaseSensitive(data_obj, "comando");
    cJSON *status = cJSON_GetObjectItemCaseSensitive(data_obj, "stausBOmba");
    cJSON *gtwId = cJSON_GetObjectItemCaseSensitive(data_obj, "gtwId");
    cJSON *tanqueId = cJSON_GetObjectItemCaseSensitive(data_obj, "tanqueId");
    cJSON *vazao = cJSON_GetObjectItemCaseSensitive(data_obj, "vazao");

    bool status_valido = cJSON_IsNumber(status) && (status->valuedouble == 0.0 || status->valuedouble == 1.0);
    bool ids_validos = cJSON_IsNumber(idBomba) && idBomba->valuedouble > 0.0 && idBomba->valuedouble <= UINT16_MAX &&
                       idBomba->valuedouble == (double)idBomba->valueint && cJSON_IsNumber(tanqueId) &&
                       tanqueId->valuedouble > 0.0 && tanqueId->valuedouble <= UINT16_MAX &&
                       tanqueId->valuedouble == (double)tanqueId->valueint && cJSON_IsNumber(gtwId) &&
                       gtwId->valuedouble > 0.0 && gtwId->valuedouble == (double)gtwId->valueint;
    bool tem_vazao = cJSON_IsNumber(vazao);
    bool vazao_valida =
        tem_vazao && isfinite(vazao->valuedouble) && vazao->valuedouble >= 0.0 && vazao->valuedouble <= 100.0;
    bool executar_comando = cJSON_IsBool(comando) && cJSON_IsTrue(comando);

    if (!ids_validos || !cJSON_IsBool(comando) || !status_valido ||
        ((vazao != NULL && !cJSON_IsNull(vazao)) && !vazao_valida) || (!executar_comando && !vazao_valida)) {
#if DEBUG_MODE
        ESP_LOGE(TAG_Websocket,
                 "Comando descartado: IDs/comando/status/vazao ausentes ou invalidos (bomba=%d tanque=%d)",
                 cJSON_IsNumber(idBomba) ? idBomba->valueint : -1, cJSON_IsNumber(tanqueId) ? tanqueId->valueint : -1);
#endif
        return;
    }

    if (gtwId->valueint != DEVICE_ID) {
#if DEBUG_MODE
        ESP_LOGW(TAG_Websocket, "Comando ignorado: destino GTW=%d, este GTW=%d", gtwId->valueint, DEVICE_ID);
#endif
        return;
    }

    float valor_vazao = 0.0f;
    if (vazao_valida) {
        valor_vazao = (float)vazao->valuedouble;
    }
    int controle_id = cJSON_IsNumber(idControle) ? idControle->valueint : idBomba->valueint;

    ESP_LOGI(TAG_Websocket, "Controlando bomba ID %d: comando=%s", idBomba->valueint,
             status->valueint ? "DESLIGAR" : "LIGAR");
    ESP_LOGI(TAG_Websocket, "Para tanque ID %d via gateway ID %d", tanqueId->valueint, gtwId->valueint);

    char URL_OTA[96];

    if (cJSON_IsNumber(tanqueId)) {
        int id = tanqueId->valueint;

        snprintf(URL_OTA, sizeof(URL_OTA), "%s%s%d", MAIN_ROUTE, leituraGtw, id);

        ESP_LOGI("Comando Web", "Comando bomba : %s", URL_OTA);

        char *json_buffer = NULL;
        int json_len = 0;
        bool enviar_lora = false;

        int ok = 0;
        if (MutexHTTP != NULL && xSemaphoreTake(MutexHTTP, pdMS_TO_TICKS(12000)) == pdTRUE) {
            ok = server_get_json(URL_OTA, &json_buffer, &json_len);
            xSemaphoreGive(MutexHTTP);
        } else {
            ESP_LOGE("Comando Web", "GET indisponivel para tanque %d; assumindo OFFLINE e enviando LoRa", id);
            enviar_lora = true;
        }

        if (!enviar_lora && (ok != 1 || json_buffer == NULL || json_len <= 0)) {
            ESP_LOGE("Comando Web", "Falha ao consultar tanque %d (ok=%d, len=%d); assumindo OFFLINE", id, ok,
                     json_len);
            free(json_buffer);
            json_buffer = NULL;
            enviar_lora = true;
        }

        cJSON *Json_GTW = NULL;
        if (!enviar_lora) {
            Json_GTW = cJSON_Parse(json_buffer); // Salva o Json que foi pego na requisicao HTTP

            if (Json_GTW == NULL) {
                ESP_LOGE("Comando Web", "Erro no cJson Parse; assumindo OFFLINE");
                enviar_lora = true;
            }
        }

        if (json_buffer != NULL) {
            if (!enviar_lora) {
#if DEBUG_MODE
                ESP_LOGI("Comando Web", "Json recebido: %s", json_buffer);
#endif
            }
            free(json_buffer);
            json_buffer = NULL;
        }

        if (Json_GTW != NULL) {
            cJSON *online = cJSON_GetObjectItemCaseSensitive(Json_GTW, "online");

            if (online && cJSON_IsBool(online)) {

                if (cJSON_IsTrue(online)) {
                    ESP_LOGI("Comando Web", "Tanque ONLINE");
                    // coloque sua lógica para online aqui
                } else {
                    ESP_LOGW("Comando Web", "Tanque OFFLINE");
                    // coloque sua lógica para offline aqui
                    enviar_lora = true;
                }
            } else {
                ESP_LOGE("Comando Web", "Campo 'online' inválido ou inexistente");
            }
        }

        if (Json_GTW != NULL && !enviar_lora) {
            cJSON *online_check = cJSON_GetObjectItemCaseSensitive(Json_GTW, "online");
            if (!(online_check && cJSON_IsBool(online_check) && cJSON_IsTrue(online_check))) {
                enviar_lora = true;
            }
        }

        cJSON_Delete(Json_GTW);

        if (enviar_lora) {
            char msg[64];
            int msg_len;
            if (vazao_valida) {
                msg_len = snprintf(msg, sizeof(msg), "{\"s\":%d,\"v\":%.2f,\"c\":%d,\"q\":%d}", status->valueint,
                                   valor_vazao, controle_id, executar_comando ? 1 : 0);
            } else {
                msg_len = snprintf(msg, sizeof(msg), "{\"s\":%d,\"c\":%d,\"q\":%d}", status->valueint, controle_id,
                                   executar_comando ? 1 : 0);
            }

            if (msg_len < 0 || msg_len >= (int)sizeof(msg)) {
                ESP_LOGE("Comando Web", "Comando LoRa excedeu o buffer e foi descartado");
                return;
            }

            bool lora_ok = gtw_send_to_tank(tanqueId->valueint, msg, idBomba->valueint);
            ESP_LOGI("Comando Web", "Resultado LoRa tanque %d: %s", tanqueId->valueint,
                     lora_ok ? "ACK recebido" : "sem ACK");
        }
    }
}

static void handle_PingTanque(cJSON *json) {

    cJSON *data_obj = cJSON_GetObjectItemCaseSensitive(json, "data");
    if (data_obj == NULL) {
#if DEBUG_MODE
        ESP_LOGE(TAG_Websocket, "Erro: 'data' não encontrado no JSON");
#endif
        return;
    }

    cJSON *gtwId = cJSON_GetObjectItemCaseSensitive(data_obj, "gtw_tanque");
    cJSON *tanqueId = cJSON_GetObjectItemCaseSensitive(data_obj, "id");

    if (!cJSON_IsNumber(gtwId) || !cJSON_IsNumber(tanqueId)) {
#if DEBUG_MODE
        ESP_LOGE(TAG_Websocket, "Campos 'tanqueId' ou 'gtwId' ausentes/inválidos");
#endif
        return;
    }

    printf("Ping recebido para tanque ID %d via gateway ID %d\n", tanqueId->valueint, gtwId->valueint);
    printf("Comparando com ID deste gateway: %d\n", DEVICE_ID);

    if (gtwId->valueint != DEVICE_ID) {
#if DEBUG_MODE
        ESP_LOGW(TAG_Websocket, "Ping ignorado: destino GTW=%d, este GTW=%d", gtwId->valueint, DEVICE_ID);
#endif
        return;
    }

    ESP_LOGI(TAG_Websocket, "Ping para tanque ID %d via gateway ID %d", tanqueId->valueint, gtwId->valueint);

    bool pingPong = gtw_ping_tank(tanqueId->valueint);

    ESP_LOGI(TAG_Websocket, "Ping para tanque ID %d: %s", tanqueId->valueint, pingPong ? "SUCESSO" : "FALHA");

    char msg[200];
    snprintf(msg, sizeof(msg), "{\"type\":\"ping_result\",\"tankId\":%d,\"status\":\"%s\"}", tanqueId->valueint,
             pingPong ? "success" : "fail");

    ws_send_json(msg);
}

static void handle_BTOn(cJSON *json) {
    cJSON *data_obj = cJSON_GetObjectItemCaseSensitive(json, "data");
    if (!cJSON_IsObject(data_obj)) {
        ESP_LOGE(TAG_Websocket, "'data' inválido");
        return;
    }

    cJSON *gtwId = cJSON_GetObjectItemCaseSensitive(data_obj, "id");
    if (!cJSON_IsNumber(gtwId)) {
        ESP_LOGE(TAG_Websocket, "'id' inválido ou não numérico");
        return;
    }

    int received_id = gtwId->valueint;

    if (received_id != DEVICE_ID) {
        ESP_LOGI(TAG_Websocket, "Mensagem não é para este gateway (%d)", received_id);
        return;
    }

    printf("Comando BLE recebido para gateway ID %d\n", received_id);

    esp_err_t err = bluetooth_config_start(BLE_STARTUP_WINDOW_MS);
    if (err != ESP_OK) {
        ESP_LOGE(TAG_Websocket, "Falha ao abrir BLE: %s", esp_err_to_name(err));
        return;
    }

    ESP_LOGI(TAG_Websocket, "BLE aberto por 30 segundos para configuracao");
}

bool ws_send_json(const char *json_msg) {
    if (!json_msg || !ws_lifecycle_mutex || xSemaphoreTake(ws_lifecycle_mutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGE("WS", "WebSocket ocupado ou mensagem invalida");
        return false;
    }

    esp_websocket_client_handle_t client = ws_client;
    if (!client) {
        ESP_LOGE("WS", "WebSocket client NULL");
        xSemaphoreGive(ws_lifecycle_mutex);
        return false;
    }

    if (!esp_websocket_client_is_connected(client)) {
        ESP_LOGE("WS", "WebSocket não conectado");
        xSemaphoreGive(ws_lifecycle_mutex);
        return false;
    }

    int len = strlen(json_msg);

    int sent = esp_websocket_client_send_text(client, json_msg, len, pdMS_TO_TICKS(5000));
    xSemaphoreGive(ws_lifecycle_mutex);

    if (sent < 0) {
        ESP_LOGE("WS", "Falha ao enviar mensagem WebSocket");
        return false;
    }

    ESP_LOGI("WS", "Mensagem enviada WS: %s", json_msg);
    return true;
}
#endif

/*######################################### Fila De Patch ############################################*/

#if (GTW_ROLE_TX_ONLY == 0)
static bool is_tank_level_patch(const char *path, const char *body, http_method_t method) {
    return method == HTTP_PATCH && path && body && strncmp(path, "/tanque/", 8) == 0 &&
           strstr(body, "nivel_atual") != NULL;
}

static bool coalesce_queued_tank_level(const char *path, const char *body) {
    if (!patch_pool_mutex || !path || !body) {
        return false;
    }

    if (xSemaphoreTake(patch_pool_mutex, pdMS_TO_TICKS(20)) != pdTRUE) {
        return false;
    }

    for (int i = 0; i < QUEUE_LENGTH; i++) {
        PatchRequest *request = &patchPool[i];

        if (patchPoolState[i] == PATCH_SLOT_QUEUED && request->method == HTTP_PATCH &&
            strcmp(request->path, path) == 0 && strstr(request->body, "nivel_atual") != NULL) {
            strncpy(request->body, body, BODY_SIZE - 1);
            request->body[BODY_SIZE - 1] = '\0';
            xSemaphoreGive(patch_pool_mutex);
            ESP_LOGW("HTTP_QUEUE", "Nivel coalescido para %s; mantendo apenas valor mais novo", path);
            return true;
        }
    }

    xSemaphoreGive(patch_pool_mutex);
    return false;
}

static void patch_slot_set(uint8_t idx, patch_slot_state_t state) {
    if (!patch_pool_mutex || idx >= QUEUE_LENGTH) {
        return;
    }

    if (xSemaphoreTake(patch_pool_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        patchPoolState[idx] = state;
        xSemaphoreGive(patch_pool_mutex);
    }
}

static bool queue_http_request(const char *path, const char *body, http_method_t method) {
    if (!path || !body) {
        return false;
    }

    if (strlen(path) >= PATH_SIZE || strlen(body) >= BODY_SIZE) {
        ESP_LOGE("HTTP_QUEUE", "Mensagem excede o tamanho da fila");
        return false;
    }

    bool tank_level_patch = is_tank_level_patch(path, body, method);
    if (tank_level_patch && coalesce_queued_tank_level(path, body)) {
        return true;
    }

    uint8_t idx;
    if (xQueueReceive(xPatchFreeQueue, &idx, pdMS_TO_TICKS(100)) != pdPASS) {
        if (tank_level_patch) {
            if (coalesce_queued_tank_level(path, body)) {
                return true;
            }

            ESP_LOGW("HTTP_QUEUE", "Fila cheia; nivel atual de %s confirmado sem enfileirar para evitar avalanche LoRa",
                     path);
            return true;
        }

        ESP_LOGW("HTTP_QUEUE", "Fila cheia, mensagem LoRa nao confirmada");
        return false;
    }

    PatchRequest *request = &patchPool[idx];
    strncpy(request->path, path, PATH_SIZE - 1);
    request->path[PATH_SIZE - 1] = '\0';
    strncpy(request->body, body, BODY_SIZE - 1);
    request->body[BODY_SIZE - 1] = '\0';
    request->method = method;
    patch_slot_set(idx, PATCH_SLOT_QUEUED);

    if (xQueueSend(xPatchQueue, &request, pdMS_TO_TICKS(500)) != pdPASS) {
        ESP_LOGE("HTTP_QUEUE", "Falha ao enfileirar requisicao");
        patch_slot_set(idx, PATCH_SLOT_FREE);
        xQueueSend(xPatchFreeQueue, &idx, 0);
        return false;
    }

    return true;
}

bool queue_patch_request(const char *path, const char *body) { return queue_http_request(path, body, HTTP_PATCH); }

static uint32_t patch_backoff_delay_ms(uint32_t fail_count) {
    uint32_t delay_ms = PATCH_BACKOFF_MIN_MS;

    for (uint32_t i = 1; i < fail_count && delay_ms < PATCH_BACKOFF_MAX_MS; i++) {
        delay_ms *= 2;
        if (delay_ms > PATCH_BACKOFF_MAX_MS) {
            delay_ms = PATCH_BACKOFF_MAX_MS;
        }
    }

    return delay_ms + (esp_random() % PATCH_BACKOFF_JITTER_MS);
}

// Tarefa para processar a fila
void patch_task(void *pvParameters) {
    PatchRequest *request;
    uint32_t mutex_fail_count = 0;
    uint32_t http_fail_total = 0;

    while (1) {
        if (xQueueReceive(xPatchQueue, &request, portMAX_DELAY) == pdTRUE) {
            uint8_t idx = (uint8_t)(request - patchPool);
            int status = -1;
            patch_slot_set(idx, PATCH_SLOT_PROCESSING);

            while (status < 200 || status >= 300) {
                if (Connectado != 1 || TokenOk != 1) {
                    vTaskDelay(pdMS_TO_TICKS(1000));
                    continue;
                }

#if (GTW_ROLE_RX_ONLY == 0)
                if (ws_client != NULL && !esp_websocket_client_is_connected(ws_client)) {
                    ESP_LOGW("PATCH_TASK", "WebSocket conectando; aguardando antes do PATCH");
                    vTaskDelay(pdMS_TO_TICKS(1000));
                    continue;
                }
#endif

                if (xSemaphoreTake(MutexHTTP, pdMS_TO_TICKS(12000)) == pdTRUE) {
                    mutex_fail_count = 0;
                    status = server_request(request->path, request->body, request->method);
                    xSemaphoreGive(MutexHTTP);

                    if (status >= 200 && status < 300) {
                        http_fail_total = 0;
                        break;
                    }

                    http_fail_total++;
                    ESP_LOGW("PATCH_TASK", "Falha HTTP (%d), status=%d", http_fail_total, status);

#if (GTW_ROLE_RX_ONLY == 0)
                    if (status == -1 && ws_client != NULL && esp_websocket_client_is_connected(ws_client)) {
                        ESP_LOGW("PATCH_TASK", "Falha TLS/HTTP com WebSocket ativo; mantendo WebSocket ligado");
                    }
#endif
                } else {
                    mutex_fail_count++;
                    ESP_LOGW("PATCH_TASK", "MutexHTTP timeout (%d)", mutex_fail_count);
                    if (mutex_fail_count >= 5)
                        ConnectRest();
                }

                uint32_t backoff_ms = patch_backoff_delay_ms(http_fail_total + mutex_fail_count);
                ESP_LOGW("PATCH_TASK", "Nova tentativa HTTP em %u ms", (unsigned)backoff_ms);
                vTaskDelay(pdMS_TO_TICKS(backoff_ms));
            }

            patch_slot_set(idx, PATCH_SLOT_FREE);
            xQueueSend(xPatchFreeQueue, &idx, 0);
        }
    }
}

#endif

/*############################################## Lora  ################################################*/

static bool lora_apply_config_with_retries(int max_attempts, bool temporary) {
    for (int attempt = 1; attempt <= max_attempts; attempt++) {
        esp_err_t err = temporary ? lora_e32_apply_cfg_temp() : lora_e32_apply_cfg();
        if (err == ESP_OK) {
            gtw_lora_ready = true;
            gtw_lora_uart_error_streak = 0;
            gtw_lora_transaction_failures = 0;
            return true;
        }

        ESP_LOGW("LORA_SETUP", "Falha ao configurar E32 tentativa %d/%d: %s", attempt, max_attempts,
                 esp_err_to_name(err));
        if (attempt < max_attempts)
            vTaskDelay(pdMS_TO_TICKS(1000));
    }

    gtw_lora_ready = false;
    return false;
}

static void lora_setup_gtw(void) {
    msg_counter_gtw = (uint8_t)esp_random();

    lora_e32_config_t cfg = {
        .uart_num = UART_NUM_1,
        .tx_pin = 16,
        .rx_pin = 18,
        .m0_pin = 12,
        .m1_pin = 14,
        // E32-900T30S não possui RESET dedicado.
        .rst_pin = -1,
        .uart_baudrate = 9600,

        // exatamente o que seu teste que funcionou usa:
        .head = 0xC0,
        .addh = 0x00,
        .addl = 0x01,
        .speed = 0x18, // configuracao legada usada pelos tanques instalados em campo
        .channel = 0x17,
        .option = 0x64,
    };

    esp_err_t init_err = lora_e32_init(&cfg);
    if (init_err != ESP_OK) {
        gtw_lora_ready = false;
        ESP_LOGE("LORA_SETUP", "Falha ao inicializar E32: %s", esp_err_to_name(init_err));
        return;
    }

    if (!lora_apply_config_with_retries(MAX_RETRIES_CONFIG, false)) {
        ESP_LOGE("LORA_SETUP", "E32 indisponivel; gateway continuara online e tentara recuperar em segundo plano");
    }
}

#if (GTW_ROLE_RX_ONLY == 0)
static bool gtw_ack_wait_is_active(void) {
    bool active = false;

    if (gtw_ack_mutex && xSemaphoreTake(gtw_ack_mutex, pdMS_TO_TICKS(10)) == pdTRUE) {
        active = gtw_ack_wait.active;
        xSemaphoreGive(gtw_ack_mutex);
    }

    return active;
}
#endif

void gtw_lora_rx_task(void *pvParameters) {
    lora_app_frame_t rx;
    gtw_lora_rx_stats_t stats = {0};
    int64_t last_stats_log_ms = esp_timer_get_time() / 1000;

#if (GTW_ROLE_TX_ONLY != 0)
    ESP_LOGI(TAG, "LoRa TX_ONLY: escutando ACKs dos tanques (ID=%d)", DEVICE_ID);
#else
    ESP_LOGI(TAG, "LoRa RX_ONLY: recebendo telemetria dos tanques para PATCH (ID=%d)", DEVICE_ID);
#endif
    esp_task_wdt_add(NULL);

    while (1) {
        esp_task_wdt_reset();
        int len = 0;
        memset(&rx, 0, sizeof(rx));
        int64_t now_ms = esp_timer_get_time() / 1000;
        uint32_t rx_timeout_ms = GTW_LORA_RX_TIMEOUT_MS;
        bool tx_waiting_ack = true;

#if (GTW_ROLE_TX_ONLY != 0)
        tx_waiting_ack = gtw_ack_wait_is_active();
        if (!tx_waiting_ack) {
            vTaskDelay(pdMS_TO_TICKS(GTW_TX_IDLE_RX_SLEEP_MS));
            continue;
        }
#endif

        if ((now_ms - last_stats_log_ms) >= 60000) {
            gtw_lora_log_rx_stats(&stats, "periodico_60s");
            last_stats_log_ms = now_ms;
        }

        if (xSemaphoreTake(MutexLora, pdMS_TO_TICKS(GTW_LORA_MUTEX_WAIT_MS)) == pdTRUE) {
            len = lora_e32_receive_raw((uint8_t *)&rx, sizeof(rx), rx_timeout_ms);
            xSemaphoreGive(MutexLora);
        }

        if (len < 0) {
            gtw_lora_uart_error_streak++;
            ESP_LOGE(TAG, "Falha UART LoRa consecutiva %u/3", gtw_lora_uart_error_streak);
            if (gtw_lora_uart_error_streak >= 3) {
                gtw_lora_ready = false;
                gtw_lora_uart_error_streak = 0;
            }
            vTaskDelay(pdMS_TO_TICKS(100));
            continue;
        }

        if (len == 0) {
            stats.timeout++;
            if ((stats.timeout % 500U) == 0U)
                gtw_lora_log_rx_stats(&stats, "timeout_500");
#if (GTW_ROLE_TX_ONLY != 0)
            vTaskDelay(pdMS_TO_TICKS(tx_waiting_ack ? 20 : GTW_TX_IDLE_RX_SLEEP_MS));
#else
            vTaskDelay(pdMS_TO_TICKS(20));
#endif
            continue;
        }

        gtw_lora_ready = true;
        gtw_lora_uart_error_streak = 0;
        stats.frame_ok++;
#if (GTW_ROLE_TX_ONLY == 0)
        ESP_LOGI(TAG,
                 "RX frame bruto ok #%lu bytes=%d src_type=%u src_id=%u dst_type=%u dst_id=%u type=%u msg_id=%u len=%u",
                 (unsigned long)stats.frame_ok, len, rx.src_type, rx.src_id, rx.dst_type, rx.dst_id, rx.msg_type,
                 rx.msg_id, rx.len);
#endif

        size_t expected_air_len = lora_frame_air_len(&rx);
        if (expected_air_len == 0 || len != (int)expected_air_len) {
            stats.incomplete++;
            ESP_LOGW(TAG, "Frame LoRa incompleto: %d/%u", len, (unsigned)expected_air_len);
            gtw_lora_log_rx_stats(&stats, "incomplete");
            continue;
        }

#if (GTW_ROLE_TX_ONLY == 0)
        ESP_LOGI(TAG, "RX src=%d id=%d type=%d msg=%.*s", rx.src_type, rx.src_id, rx.msg_type, rx.len, rx.payload);
#endif

        if (rx.preamble != LORA_PREAMBLE) {
            stats.bad_preamble++;
            gtw_lora_log_rx_stats(&stats, "bad_preamble");
            printf("Preamble inválido: 0x%02X\n", rx.preamble);
            continue;
        }

        uint16_t crc_calc = 0;
        uint16_t crc_rx = 0;

        if (!lora_received_frame_valid(&rx, len, &crc_calc, &crc_rx)) {
            stats.bad_crc++;
            gtw_lora_log_rx_stats(&stats, "bad_crc");
            ESP_LOGW(TAG, "CRC inválido");
            continue;
        }

        if (rx.len > sizeof(rx.payload) ||
            (rx.src_type != DEV_TANK && rx.src_type != DEV_GTW && rx.src_type != DEV_REP) || rx.msg_type < MSG_DATA ||
            rx.msg_type > MSG_PING) {
            stats.semantic_invalid++;
            gtw_lora_log_rx_stats(&stats, "semantic_invalid");
            ESP_LOGW(TAG, "Frame LoRa semanticamente invalido");
            continue;
        }

#if (GTW_ROLE_TX_ONLY != 0)
        if (rx.src_type == DEV_GTW && rx.src_id == DEVICE_ID && rx.dst_type == DEV_TANK) {
            stats.wrong_dst++;
            continue;
        }
#endif

        if (rx.dst_type != DEV_GTW || rx.dst_id != DEVICE_ID) {
            stats.wrong_dst++;
#if (GTW_ROLE_TX_ONLY == 0)
            gtw_lora_log_rx_stats(&stats, "wrong_dst");
            ESP_LOGW(TAG, "Frame não é para mim");
#endif
            continue;
        }

        gtw_last_lora_rx_ms = esp_timer_get_time() / 1000;

#if (GTW_ROLE_RX_ONLY == 0)
        if (rx.msg_type == MSG_ACK) {
            stats.ack_rx++;
            TaskHandle_t waiter = NULL;

            xSemaphoreTake(gtw_ack_mutex, portMAX_DELAY);
            if (gtw_ack_wait.active && gtw_ack_wait.msg_id == rx.msg_id && gtw_ack_wait.tank_id == rx.src_id) {
                waiter = gtw_ack_wait.waiter;
                gtw_ack_wait.active = false;
                gtw_ack_wait.waiter = NULL;
            }
            xSemaphoreGive(gtw_ack_mutex);

            if (waiter) {
                stats.ack_match++;
                xTaskNotifyGive(waiter);
            } else {
                stats.ack_unexpected++;
            }
            continue;
        }
#else
        if (rx.msg_type == MSG_ACK) {
            stats.ack_unexpected++;
            continue;
        }
#endif

#if (GTW_ROLE_TX_ONLY != 0)
        stats.data_rx++;
        continue;
#else

        if (rx.src_type == DEV_TANK && rx.msg_type == MSG_PING) {
            stats.ping_rx++;
            gtw_send_ack(&rx);
            stats.ack_sent++;
            ESP_LOGI(TAG, "Heartbeat LoRa recebido do tanque %u", rx.src_id);
            continue;
        }

        if (rx.src_type == DEV_TANK && rx.msg_type == MSG_DATA) {
            stats.data_rx++;
            TickType_t now = xTaskGetTickCount();
            gtw_rx_dup_t *dup = &gtw_rx_dup_cache[(rx.src_id ^ rx.msg_id) % GTW_RX_DUP_CACHE_SIZE];

            if (dup->used && dup->src_id == rx.src_id && dup->msg_id == rx.msg_id &&
                (now - dup->accepted_at) < pdMS_TO_TICKS(15000)) {
                stats.duplicate++;
                gtw_send_ack(&rx);
                stats.ack_sent++;
                gtw_lora_log_rx_stats(&stats, "duplicate_ack");
                continue;
            }

            size_t body_len = rx.len;
            if (body_len > sizeof(rx.payload))
                body_len = sizeof(rx.payload);

            char body[sizeof(rx.payload) + 1] = {0};
            memcpy(body, rx.payload, body_len);
            body[body_len] = '\0';

            bool accepted = false;

            if (rx.has_bomba_id == 1) {
                printf("Enviando PATCH para BOMBA %d (Tanque %d)\n", rx.bomba_id, rx.src_id);

                char pathBomba[40];
                snprintf(pathBomba, sizeof(pathBomba), "/bomba/%d", rx.bomba_id);

                accepted = queue_patch_request(pathBomba, body);
            } else if (rx.has_bomba_id == 0) {
                printf("Enviando PATCH para TANQUE %d\n", rx.src_id);

                char pathTanque[30];
                snprintf(pathTanque, sizeof(pathTanque), "/tanque/%d", rx.src_id);

                accepted = queue_patch_request(pathTanque, body);
            } else if (rx.has_bomba_id == 2) {
                printf("Enviando PATCH para Alertas %d\n", rx.src_id);

                char pathTanque[30];
                snprintf(pathTanque, sizeof(pathTanque), "/alertas/tanque");

                accepted = queue_http_request(pathTanque, body, HTTP_POST);
            }

            if (accepted) {
                stats.queue_accept++;
                dup->used = true;
                dup->src_id = rx.src_id;
                dup->msg_id = rx.msg_id;
                dup->accepted_at = now;
                gtw_send_ack(&rx);
                stats.ack_sent++;
            } else {
                stats.queue_reject++;
                ESP_LOGW(TAG, "Telemetria nao aceita; ACK nao enviado");
                gtw_lora_log_rx_stats(&stats, "queue_reject");
            }
        }
#endif
    }
}

static void gtw_lora_health_task(void *pvParameters) {
#if (GTW_ROLE_TX_ONLY == 0)
    int recovery_cycles = 0;
    gtw_last_lora_rx_ms = esp_timer_get_time() / 1000;
#endif
    esp_task_wdt_add(NULL);

    while (1) {
        esp_task_wdt_reset();
        vTaskDelay(pdMS_TO_TICKS(30000));

#if (GTW_ROLE_TX_ONLY != 0)
        if (gtw_lora_ready)
            continue;

        ESP_LOGW("LORA_HEALTH", "E32 marcado como indisponivel; tentando reconfigurar");
        if (xSemaphoreTake(gtw_request_mutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
            if (xSemaphoreTake(MutexLora, pdMS_TO_TICKS(5000)) == pdTRUE) {
                bool recovered = lora_e32_reinit() == ESP_OK && lora_apply_config_with_retries(1, true);
                xSemaphoreGive(MutexLora);
                ESP_LOGI("LORA_HEALTH", "Recuperacao E32: %s", recovered ? "OK" : "FALHOU");
            }
            xSemaphoreGive(gtw_request_mutex);
        }
        continue;
#else
        int64_t now_ms = esp_timer_get_time() / 1000;
        int64_t silent_ms = now_ms - gtw_last_lora_rx_ms;

        if (silent_ms < 180000) {
            recovery_cycles = 0;
            continue;
        }

        ESP_LOGW("LORA_HEALTH", "Sem frame LoRa válido há %lld s; reconfigurando E32", silent_ms / 1000);

        if (xSemaphoreTake(gtw_request_mutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
            esp_err_t err = ESP_ERR_TIMEOUT;

            if (xSemaphoreTake(MutexLora, pdMS_TO_TICKS(5000)) == pdTRUE) {
                err = lora_e32_apply_cfg_temp();
                xSemaphoreGive(MutexLora);
            }

            xSemaphoreGive(gtw_request_mutex);

            if (err == ESP_OK)
                ESP_LOGI("LORA_HEALTH", "E32 reconfigurado por M0/M1");
            else
                ESP_LOGE("LORA_HEALTH", "Falha ao reconfigurar E32: %s", esp_err_to_name(err));
        }

        recovery_cycles++;
        gtw_last_lora_rx_ms = now_ms;

        if (recovery_cycles >= 5) {
            // Ausência de frames também pode significar tanque desligado ou fora
            // de alcance. O gateway deve permanecer online e continuar tentando.
            ESP_LOGE("LORA_HEALTH", "LoRa sem comunicação prolongada; gateway permanece online");
            recovery_cycles = 0;
        }
#endif
    }
}

uint16_t lora_crc16(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= (uint16_t)data[i] << 8;
        for (int bit = 0; bit < 8; bit++)
            crc = (crc & 0x8000) ? (uint16_t)((crc << 1) ^ 0x1021) : (uint16_t)(crc << 1);
    }
    return crc;
}

static size_t lora_frame_air_len(const lora_app_frame_t *frame) {
    if (!frame || frame->len > sizeof(frame->payload))
        return 0;

    return offsetof(lora_app_frame_t, payload) + frame->len + sizeof(frame->crc);
}

static int lora_send_frame_air(lora_app_frame_t *frame) {
    if (!frame)
        return -1;

    size_t header_len = offsetof(lora_app_frame_t, payload);
    size_t payload_len = frame->len;
    size_t air_len = lora_frame_air_len(frame);

    if (air_len == 0 || air_len > sizeof(lora_app_frame_t))
        return -1;

    uint8_t raw[sizeof(lora_app_frame_t)] = {0};
    memcpy(raw, frame, header_len + payload_len);

    frame->crc = lora_crc16(raw, header_len + payload_len);
    memcpy(raw + header_len + payload_len, &frame->crc, sizeof(frame->crc));

    return lora_e32_send_raw(raw, (int)air_len);
}

static bool lora_received_frame_valid(const lora_app_frame_t *rx, int air_len, uint16_t *out_crc_calc,
                                      uint16_t *out_crc_rx) {
    if (!rx || air_len < (int)(offsetof(lora_app_frame_t, payload) + sizeof(rx->crc)))
        return false;

    size_t expected_len = lora_frame_air_len(rx);
    if (expected_len == 0 || air_len != (int)expected_len)
        return false;

    const uint8_t *raw = (const uint8_t *)rx;
    uint16_t crc_rx = 0;
    memcpy(&crc_rx, raw + expected_len - sizeof(crc_rx), sizeof(crc_rx));
    uint16_t crc_calc = lora_crc16(raw, expected_len - sizeof(crc_rx));

    if (out_crc_calc)
        *out_crc_calc = crc_calc;
    if (out_crc_rx)
        *out_crc_rx = crc_rx;

    return crc_calc == crc_rx;
}

#if (GTW_ROLE_TX_ONLY == 0)
void gtw_send_ack(const lora_app_frame_t *rx) {
    lora_app_frame_t ack = {0};

    ack.preamble = LORA_PREAMBLE;

    ack.src_type = DEV_GTW;
    ack.src_id = DEVICE_ID;

    ack.dst_type = rx->src_type;
    ack.dst_id = rx->src_id;

    ack.msg_type = MSG_ACK;
    ack.msg_id = rx->msg_id;
    ack.len = 0;

    if (xSemaphoreTake(MutexLora, pdMS_TO_TICKS(2500)) == pdTRUE) {
        size_t air_len = lora_frame_air_len(&ack);
        int sent = lora_send_frame_air(&ack);
        xSemaphoreGive(MutexLora);
        if (sent == (int)air_len)
            ESP_LOGI(TAG, "ACK enviado id=%d bytes=%d", ack.msg_id, sent);
        else
            ESP_LOGE(TAG, "ACK incompleto id=%d bytes=%d/%u", ack.msg_id, sent, (unsigned)air_len);
    } else {
        ESP_LOGW(TAG, "Nao foi possivel enviar ACK id=%d", ack.msg_id);
    }
}
#endif

#if (GTW_ROLE_RX_ONLY == 0)
static bool gtw_send_frame_wait_ack(lora_app_frame_t *frame) {
    if (!frame || !gtw_request_mutex || !gtw_ack_mutex)
        return false;

    esp_task_wdt_reset();
    if (xSemaphoreTake(gtw_request_mutex, pdMS_TO_TICKS(15000)) != pdTRUE)
        return false;

    if (!gtw_lora_ready) {
        bool recovered = false;
        if (xSemaphoreTake(MutexLora, pdMS_TO_TICKS(5000)) == pdTRUE) {
            recovered = lora_e32_reinit() == ESP_OK && lora_apply_config_with_retries(MAX_RETRIES_CONFIG, true);
            xSemaphoreGive(MutexLora);
        }

        if (!recovered) {
            ESP_LOGE("GTW", "E32 indisponivel; transacao LoRa cancelada sem bloquear a fila");
            xSemaphoreGive(gtw_request_mutex);
            return false;
        }
    }

    bool success = false;
    const bool is_ping = frame->msg_type == MSG_PING;
    const int attempt_limit = is_ping ? LORA_PING_ACK_RETRIES : ACK_RETRIES;
    const uint32_t ack_timeout_ms = is_ping ? LORA_PING_ACK_TIMEOUT_MS : ACK_TIMEOUT_MS;

    for (int attempt = 1; attempt <= attempt_limit; attempt++) {
        esp_task_wdt_reset();
        ulTaskNotifyTake(pdTRUE, 0);

        xSemaphoreTake(gtw_ack_mutex, portMAX_DELAY);
        gtw_ack_wait.active = true;
        gtw_ack_wait.msg_id = frame->msg_id;
        gtw_ack_wait.tank_id = frame->dst_id;
        gtw_ack_wait.waiter = xTaskGetCurrentTaskHandle();
        xSemaphoreGive(gtw_ack_mutex);

        if (xSemaphoreTake(MutexLora, pdMS_TO_TICKS(2500)) == pdTRUE) {
            lora_e32_flush_rx();
            size_t air_len = lora_frame_air_len(frame);
            int sent = lora_send_frame_air(frame);
            xSemaphoreGive(MutexLora);
            if (sent != (int)air_len) {
                gtw_lora_ready = false;
                ESP_LOGE("GTW", "TX LoRa incompleto: %d/%u", sent, (unsigned)air_len);
                xSemaphoreTake(gtw_ack_mutex, portMAX_DELAY);
                gtw_ack_wait.active = false;
                gtw_ack_wait.waiter = NULL;
                xSemaphoreGive(gtw_ack_mutex);
                continue;
            }
        } else {
            xSemaphoreTake(gtw_ack_mutex, portMAX_DELAY);
            gtw_ack_wait.active = false;
            gtw_ack_wait.waiter = NULL;
            xSemaphoreGive(gtw_ack_mutex);
            continue;
        }

        if (ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(ack_timeout_ms)) > 0) {
            success = true;
            break;
        }
        esp_task_wdt_reset();

        xSemaphoreTake(gtw_ack_mutex, portMAX_DELAY);
        gtw_ack_wait.active = false;
        gtw_ack_wait.waiter = NULL;
        xSemaphoreGive(gtw_ack_mutex);

        ESP_LOGW("GTW", "Sem ACK do tanque %u, tentativa %d", frame->dst_id, attempt);
        if (attempt < attempt_limit) {
            vTaskDelay(pdMS_TO_TICKS(LORA_RETRY_BACKOFF_MIN_MS + (esp_random() % LORA_RETRY_BACKOFF_JITTER_MS)));
        }
    }

    if (success) {
        gtw_lora_transaction_failures = 0;
    } else {
        gtw_lora_transaction_failures++;
        if (gtw_lora_transaction_failures >= GTW_LORA_FAILURES_BEFORE_RECOVERY) {
            gtw_lora_transaction_failures = 0;
            gtw_lora_ready = false;
            ESP_LOGW("LORA_HEALTH", "Falhas LoRa consecutivas; E32 marcado para reinicializacao validada");
        }
    }

    // O ping termina no ACK e precisa apenas de uma janela curta. Comandos
    // mantêm a proteção maior porque podem gerar telemetria logo depois.
    uint32_t guard_ms = is_ping ? LORA_PING_GUARD_MS : LORA_TRANSACTION_GUARD_MS;
    vTaskDelay(pdMS_TO_TICKS(guard_ms));
    esp_task_wdt_reset();

    xSemaphoreGive(gtw_request_mutex);
    return success;
}

bool gtw_send_to_tank(uint16_t tank_id, const char *msg, uint16_t bomba_id) {
    lora_app_frame_t frame = {0};

    frame.preamble = LORA_PREAMBLE;
    frame.src_type = DEV_GTW;
    frame.src_id = DEVICE_ID;

    frame.dst_type = DEV_TANK;
    frame.dst_id = tank_id;

    if (bomba_id == 0xFFFF || bomba_id == 0) {
        frame.has_bomba_id = 0;
        frame.bomba_id = 0;
    } else {
        frame.has_bomba_id = 1;
        frame.bomba_id = bomba_id;
    }

    frame.msg_type = MSG_CMD;
    frame.msg_id = msg_counter_gtw++;

    frame.len = (uint8_t)strnlen(msg ? msg : "", sizeof(frame.payload));
    if (frame.len > 0)
        memcpy(frame.payload, msg, frame.len);

    bool ok = gtw_send_frame_wait_ack(&frame);
    ESP_LOGI("GTW", "CMD tanque %d: %s", tank_id, ok ? "ACK recebido" : "sem resposta");
    return ok;
}

bool gtw_ping_tank(uint16_t tank_id) {
    lora_app_frame_t frame = {0};

    frame.preamble = LORA_PREAMBLE;

    frame.src_type = DEV_GTW;
    frame.src_id = DEVICE_ID;

    frame.dst_type = DEV_TANK;
    frame.dst_id = tank_id;

    frame.msg_type = MSG_PING;
    frame.msg_id = msg_counter_gtw++;
    frame.len = 0;

    bool ok = gtw_send_frame_wait_ack(&frame);
    ESP_LOGI("GTW", "PING tanque %d: %s", tank_id, ok ? "OK" : "FALHOU");
    return ok;
}
#endif
/*############################################ BT ############################################*/

static void ble_config_mode_task(void *pvParameters) {
#if (GTW_ROLE_RX_ONLY == 0)
    ESP_LOGW(TAG, "Entrando em modo configuracao BLE: encerrando WebSocket e Wi-Fi");
#else
    ESP_LOGW(TAG, "Entrando em modo configuracao BLE: encerrando Wi-Fi");
#endif

    ble_startup_window_active = false;
    Connectado = 0;

#if (GTW_ROLE_RX_ONLY == 0)
    if (websocket_task_is_running()) {
        stop_websocket_task = true;

        for (int i = 0; i < 350 && websocket_task_is_running(); i++) {
            vTaskDelay(pdMS_TO_TICKS(100));
        }

        if (websocket_task_is_running())
            ESP_LOGW(TAG, "WebSocket ainda finalizando; Wi-Fi sera desligado para concluir o encerramento");
    }
#endif

    if (taskConecta_WIFI != NULL) {
        xTaskNotifyGive(taskConecta_WIFI);
    }

    ESP_LOGI(TAG, "Modo configuracao BLE ativo; desligamento do Wi-Fi solicitado a task responsavel");
    ble_config_mode_task_handle = NULL;
    vTaskDelete(NULL);
}

static void ble_startup_window_task(void *pvParameters) {
    while (ble_startup_window_active) {
        if (!bluetooth_config_is_active()) {
            ble_startup_window_active = false;
            ESP_LOGI(TAG, "Janela BLE do boot encerrada; Wi-Fi sera iniciado pela task");
            if (taskConecta_WIFI != NULL) {
                xTaskNotifyGive(taskConecta_WIFI);
            }
            break;
        }

        if (ble_config_mode_active) {
            ble_startup_window_active = false;
            break;
        }

        vTaskDelay(pdMS_TO_TICKS(1000));
    }

    vTaskDelete(NULL);
}

void bt_client_connected_callback(void) {
    ble_config_mode_active = true;
    ble_startup_window_active = false;

    if (ble_config_mode_task_handle == NULL) {
        xTaskCreate(ble_config_mode_task, "ble_cfg_mode", 4096, NULL, 6, &ble_config_mode_task_handle);
    }
}

void bt_client_disconnected_callback(void) {
    ble_config_mode_active = false;
    ble_startup_window_active = false;
    ESP_LOGI(TAG, "Saindo do modo configuracao BLE; Wi-Fi sera retomado pela task");
    if (taskConecta_WIFI != NULL) {
        xTaskNotifyGive(taskConecta_WIFI);
    }
}

static void bt_send_config_snapshot(void) {
    cJSON *root = cJSON_CreateObject();
    cJSON *wifi = cJSON_CreateObject();
    cJSON *gateway = cJSON_CreateObject();

    if (!root || !wifi || !gateway) {
        cJSON_Delete(root);
        cJSON_Delete(wifi);
        cJSON_Delete(gateway);
        bluetooth_send_message("{\"ok\":false,\"error\":\"no_mem\"}");
        return;
    }

    cJSON_AddBoolToObject(root, "ok", true);
    cJSON_AddStringToObject(root, "type", "Config");

    cJSON_AddStringToObject(wifi, "SSID", wifi_ssid);
    cJSON_AddStringToObject(wifi, "password", wifi_password);
    cJSON_AddItemToObject(root, "wifi", wifi);

    cJSON_AddStringToObject(gateway, "UserGtw", userNameHTTPs);
    cJSON_AddStringToObject(gateway, "password", passwordHTTPs);
    cJSON_AddNumberToObject(gateway, "IdUser", ID_GATEWAY);
    cJSON_AddNumberToObject(gateway, "unidade", DEVICE_ID);
    cJSON_AddItemToObject(root, "gateway", gateway);

    char *response = cJSON_PrintUnformatted(root);
    if (response) {
        bluetooth_send_message(response);
        cJSON_free(response);
    } else {
        bluetooth_send_message("{\"ok\":false,\"error\":\"json_print\"}");
    }

    cJSON_Delete(root);
}

void bt_message_received_callback(const char *message) {
    printf("MSG Bluetooth Recebido \n");

    if (!message) {
        printf("Erro: Dados recebidos são nulos\n");
        return;
    }

    cJSON *jsonBluetooth = cJSON_Parse(message);

    if (jsonBluetooth == NULL) {
        printf("Erro ao parsear JSON\n");
        bluetooth_send_message("{\"ok\":false,\"error\":\"invalid_json\"}");
        return;
    }

    cJSON *JsonTypeCmd = cJSON_GetObjectItem(jsonBluetooth, "type");
    if (!cJSON_IsString(JsonTypeCmd)) {
        JsonTypeCmd = cJSON_GetObjectItem(jsonBluetooth, "cmd");
    }
    const char *cmd = cJSON_GetStringValue(JsonTypeCmd);

    printf("Comando recebido: %s\n", cmd ? cmd : "NULL");

    if (cmd && strcmp(cmd, "GetConfig") == 0) {
        bt_send_config_snapshot();
        cJSON_Delete(jsonBluetooth);
        return;
    }

    if (cmd && strcmp(cmd, "GtwConfig") == 0) {
        cJSON *JsonUserGtw = cJSON_GetObjectItem(jsonBluetooth, "UserGtw");
        cJSON *JsonUserId = cJSON_GetObjectItem(jsonBluetooth, "IdUser");
        cJSON *JsonPassword = cJSON_GetObjectItem(jsonBluetooth, "password");
        cJSON *JsonUnidade = cJSON_GetObjectItem(jsonBluetooth, "unidade");

        // Verifica se todos os campos estão presentes e são válidos
        if (!cJSON_IsString(JsonUserGtw) || JsonUserGtw->valuestring == NULL) {
#if DEBUG_MODE
            printf("Erro: Campo 'UserGtw' ausente ou inválido\n");
#endif
            cJSON_Delete(jsonBluetooth);
            return;
        }

        if (!cJSON_IsString(JsonPassword) || JsonPassword->valuestring == NULL) {
#if DEBUG_MODE
            printf("Erro: Campo 'password' ausente ou inválido\n");
#endif
            cJSON_Delete(jsonBluetooth);
            return;
        }

        if (!cJSON_IsNumber(JsonUserId)) {
#if DEBUG_MODE
            printf("Erro: Campo 'IdUser' ausente ou inválido\n");
#endif
            cJSON_Delete(jsonBluetooth);
            return;
        }

        if (!cJSON_IsNumber(JsonUnidade)) {
#if DEBUG_MODE

            printf("Erro: Campo 'unidade' ausente ou inválido\n");
#endif
            cJSON_Delete(jsonBluetooth);
            return;
        }

        if (JsonUserId->valueint <= 0 || JsonUnidade->valueint <= 0 ||
            JsonUserId->valuedouble != (double)JsonUserId->valueint ||
            JsonUnidade->valuedouble != (double)JsonUnidade->valueint ||
            JsonUserId->valueint != JsonUnidade->valueint) {
            bluetooth_send_message("{\"ok\":false,\"error\":\"gateway_ids_must_match\"}");
            cJSON_Delete(jsonBluetooth);
            return;
        }

        // Salva os valores no NVS
        save_UsetGTW(JsonUserGtw->valuestring);
        save_PasswordGTW(JsonPassword->valuestring);
        save_idGtw(JsonUserId->valueint);
        save_idUnidadeGtw(JsonUserId->valueint);
        strlcpy(userNameHTTPs, JsonUserGtw->valuestring, sizeof(userNameHTTPs));
        strlcpy(passwordHTTPs, JsonPassword->valuestring, sizeof(passwordHTTPs));
        ID_GATEWAY = JsonUserId->valueint;
        DEVICE_ID = JsonUserId->valueint;
        bluetooth_send_message("{\"ok\":true,\"status\":\"gtw_config_saved\",\"restart_required\":true}");

        cJSON_Delete(jsonBluetooth); // Libera a memória alocada para o JSON
        return;
    }

    if (cmd && strcmp(cmd, "GtwWifi") == 0) {
        cJSON *JsonSSIDWifi = cJSON_GetObjectItem(jsonBluetooth, "SSID");
        cJSON *JsonPasswordWifi = cJSON_GetObjectItem(jsonBluetooth, "password");

        // Verifica se todos os campos estão presentes e são válidos
        if (!cJSON_IsString(JsonSSIDWifi) || JsonSSIDWifi->valuestring == NULL) {
#if DEBUG_MODE
            printf("Erro: Campo 'ssidValue' ausente ou inválido\n");
#endif
            cJSON_Delete(jsonBluetooth);
            return;
        }
        if (!cJSON_IsString(JsonPasswordWifi) || JsonPasswordWifi->valuestring == NULL) {
#if DEBUG_MODE
            printf("Erro: Campo 'JsonPasswordWifi' ausente ou inválido\n");
#endif
            cJSON_Delete(jsonBluetooth);
            return;
        }

        save_SIIDWifi(JsonSSIDWifi->valuestring);
        save_PasswordWifi(JsonPasswordWifi->valuestring);
        bluetooth_send_message("{\"ok\":true,\"status\":\"wifi_config_saved\",\"restart_required\":true}");

        strlcpy(wifi_ssid, JsonSSIDWifi->valuestring, sizeof(wifi_ssid));
        strlcpy(wifi_password, JsonPasswordWifi->valuestring, sizeof(wifi_password));

        cJSON_Delete(jsonBluetooth);
        return;
    }

    if ((cmd && strcmp(cmd, "Reset") == 0)) {
        bluetooth_send_message("{\"ok\":true,\"status\":\"reset\"}");
        cJSON_Delete(jsonBluetooth);
        ConnectRest(); // Resetar connect
        return;
    }

    bluetooth_send_message("{\"ok\":false,\"error\":\"unknown_command\"}");
    cJSON_Delete(jsonBluetooth);
}

// Task para monitorar memoria
void vTaskImprimirUsoMemoria(void *pvParameters) {
    esp_task_wdt_add(NULL);

    static uint8_t boot_cycles = 0;

    while (1) {
        esp_task_wdt_reset();

        // Captura heap com API oficial
        multi_heap_info_t info;
        heap_caps_get_info(&info, MALLOC_CAP_8BIT);

        size_t heap_livre = info.total_free_bytes;
        size_t heap_total = info.total_allocated_bytes + heap_livre;
        float percentual_uso = ((float)(heap_total - heap_livre) / heap_total) * 100.0f;

#if DEBUG_MODE
        ESP_LOGI("MEMORY", "Heap total: %u | Livre: %u | Uso: %.2f%%", (unsigned)heap_total, (unsigned)heap_livre,
                 percentual_uso);
#endif

        // Low memory check
        if (percentual_uso > 94.0f) {
            if (bluetooth_config_is_active()) {
                ESP_LOGW("MEMORY", "Heap critico com BLE ativo (%.2f%%). Fechando BLE antes de resetar.",
                         percentual_uso);
                bluetooth_config_stop();
                vTaskDelay(pdMS_TO_TICKS(1000));
                continue;
            }

            ESP_LOGW("MEMORY", "Heap crítico (%.2f%%). Sinalizando reset do stack de rede...", percentual_uso);
            ConnectRest();
        }

        // Recria o WebSocket. Um pedido explicito permanece ativo ate xTaskCreate ter sucesso.
#if (GTW_ROLE_RX_ONLY == 0)
        bool websocket_should_start =
            TokenOk == 1 && token_global[0] != '\0' && wifi_sta_connected() && wifi_sta_has_ip();
        if ((boot_cycles > 3 || websocket_restart_requested) && !websocket_task_is_running() &&
            websocket_should_start) {
            ESP_LOGW("MEMORY", "Recriando task WebSocket%s...",
                     websocket_restart_requested ? " apos desconexao prolongada" : "");
            if (!websocket_task_start_if_needed()) {
                ESP_LOGE("MEMORY", "Falha ao recriar task WebSocket; nova tentativa no proximo ciclo");
            }
        }

        if (boot_cycles <= 3) {
            boot_cycles++;
        } else {
            boot_cycles = 4;
        }
#else
        boot_cycles++;
#endif

        vTaskDelay(pdMS_TO_TICKS(15000 + esp_random() % 2000)); // jitter leve
    }
}

/*#########################################  Funçoes de Configuraçao Bluetooth #########################################
 */

// // Função para salvar A SIID do wifi na NVS
void save_SIIDWifi(const char *device_name) {
    nvs_handle_t my_handle;
    esp_err_t err = nvs_open("storage", NVS_READWRITE, &my_handle);
    if (err == ESP_OK) {
        nvs_set_str(my_handle, "SSID", device_name);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    } else {
        ESP_LOGE("NVS", "Erro ao abrir NVS handle!");
    }
}

void save_PasswordWifi(const char *password) {
    nvs_handle_t my_handle;
    esp_err_t err = nvs_open("storage", NVS_READWRITE, &my_handle);
    if (err == ESP_OK) {
        // Armazenando a senha no NVS com a chave "passwordGtw"
        nvs_set_str(my_handle, "passwordWifi", password);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    } else {
        ESP_LOGE("NVS", "Erro ao abrir NVS handle para salvar a senha do WIfi!");
    }
}

// Função para salvar o estado na NVS
void save_UsetGTW(const char *device_name) {
    nvs_handle_t my_handle;
    esp_err_t err = nvs_open("storage", NVS_READWRITE, &my_handle);
    if (err == ESP_OK) {
        nvs_set_str(my_handle, "userGtw", device_name);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    } else {
        ESP_LOGE("NVS", "Erro ao abrir NVS handle!");
    }
}

void save_PasswordGTW(const char *password) {
    nvs_handle_t my_handle;
    esp_err_t err = nvs_open("storage", NVS_READWRITE, &my_handle);
    if (err == ESP_OK) {
        // Armazenando a senha no NVS com a chave "passwordGtw"
        nvs_set_str(my_handle, "passwordGtw", password);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    } else {
        ESP_LOGE("NVS", "Erro ao abrir NVS handle para salvar a senha!");
    }
}

// Função para salvar o estado ID do GTW na NVS
void save_idGtw(int state) {
    nvs_handle_t my_handle;
    esp_err_t err = nvs_open("idGtw", NVS_READWRITE, &my_handle);
    if (err != ESP_OK) {
        ESP_LOGE("NVS", "Falha ao abrir ID gateway: %s", esp_err_to_name(err));
        return;
    }
    err = nvs_set_i32(my_handle, "idGtw", state);
    if (err == ESP_OK)
        err = nvs_commit(my_handle);
    nvs_close(my_handle);
    if (err != ESP_OK)
        ESP_LOGE("NVS", "Falha ao salvar ID gateway: %s", esp_err_to_name(err));
}

// Função para salvar o ID da Unidade do GTW na NVS
void save_idUnidadeGtw(int state) {
    nvs_handle_t my_handle;
    esp_err_t err = nvs_open("idUnidadeGtw", NVS_READWRITE, &my_handle);
    if (err != ESP_OK) {
        ESP_LOGE("NVS", "Falha ao abrir ID unidade: %s", esp_err_to_name(err));
        return;
    }
    err = nvs_set_i32(my_handle, "idUnidadeGtw", state);
    if (err == ESP_OK)
        err = nvs_commit(my_handle);
    nvs_close(my_handle);
    if (err != ESP_OK)
        ESP_LOGE("NVS", "Falha ao salvar ID unidade: %s", esp_err_to_name(err));
}

/*######################################### Tratamento de Erros ############################################*/

// Reinicia o sistema
void ConnectRest() {
    printf("\033[1;36m\n\n========== SISTEMA REINICIANDO: Connect ==========\n\n\033[0m");

    // Resetar GPIOs (já presente e recomendado)
    // gpio_reset_pin(I2C_SDA);
    // gpio_reset_pin(I2C_SCL);

    vTaskDelay(pdMS_TO_TICKS(500));
    esp_restart(); // Reinicia o ESP
}
