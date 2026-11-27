#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <vector>
 
enum class Politica { Lectores, Escritores, Fifo };
class ControlRW {
public:
    virtual ~ControlRW() = default;
    virtual void entrar_lectura() = 0;
    virtual void salir_lectura() = 0;
    virtual void entrar_escritura() = 0;
    virtual void salir_escritura() = 0;
};
 
// R3, R4 y R5
class MonitorRW : public ControlRW {
public:
    explicit MonitorRW(Politica p) : politica(p) {}

    void entrar_lectura() override {
        std::unique_lock<std::mutex> lk(m);

        // R5: politica FIFO
        if (politica == Politica::Fifo) {
            const long turno = siguiente_turno++;

            cv_fifo.wait(lk, [this, turno] {
                return turno == turno_actual &&
                       !escritora_activa;
            });

            lectoras_activas++;
            turno_actual++;

            // Permitir el ingreso de lectoras consecutivas
            cv_fifo.notify_all();
            return;
        }

        // R3 y R4
        lectoras_esperando++;

        cv_lectoras.wait(lk, [this] {
            return !escritora_activa &&
                   (politica != Politica::Escritores ||
                    escritoras_esperando == 0);
        });

        lectoras_esperando--;
        lectoras_activas++;
    }

    void salir_lectura() override {
        std::unique_lock<std::mutex> lk(m);

        lectoras_activas--;

        if (lectoras_activas == 0) {
            if (politica == Politica::Fifo)
                cv_fifo.notify_all();
            else
                cv_escritoras.notify_one();
        }
    }

    void entrar_escritura() override {
        std::unique_lock<std::mutex> lk(m);

        // R5: politica FIFO
        if (politica == Politica::Fifo) {
            const long turno = siguiente_turno++;

            cv_fifo.wait(lk, [this, turno] {
                return turno == turno_actual &&
                       !escritora_activa &&
                       lectoras_activas == 0;
            });

            escritora_activa = true;
            turno_actual++;
            return;
        }

        // R3 y R4
        escritoras_esperando++;

        cv_escritoras.wait(lk, [this] {
            return !escritora_activa &&
                   lectoras_activas == 0 &&
                   (politica != Politica::Lectores ||
                    lectoras_esperando == 0);
        });

        escritoras_esperando--;
        escritora_activa = true;
    }

    void salir_escritura() override {
        std::unique_lock<std::mutex> lk(m);

        escritora_activa = false;

        if (politica == Politica::Fifo) {
            // R5: despertar solicitudes pendientes
            cv_fifo.notify_all();

        } else if (politica == Politica::Escritores &&
                   escritoras_esperando > 0) {
            // R4: prioridad de escritores
            cv_escritoras.notify_one();

        } else if (lectoras_esperando > 0) {
            // R3: prioridad de lectores, o R4 sin escritoras pendientes
            cv_lectoras.notify_all();

        } else {
            cv_escritoras.notify_one();
        }
    }

private:
    Politica politica;
    std::mutex m;

    std::condition_variable cv_lectoras, cv_escritoras, cv_fifo;

    int lectoras_activas = 0;
    int lectoras_esperando = 0;
    int escritoras_esperando = 0;

    bool escritora_activa = false;

    // R5: control de turnos FIFO
    long siguiente_turno = 0;
    long turno_actual = 0;
};
 
// R2
struct Registro {
    long version = 0;
    long doble_version = 0;
};
 
Registro reg;
long K, dur_lec, dur_esc;
 
// contadores de diagnostico, separados del estado del monitor
std::atomic<int> lectoras_dentro{0}, escritoras_dentro{0}, max_lectoras{0};
std::atomic<long> inconsistencias{0}, violaciones{0};
 
void lectora(ControlRW* c) {
    for (long i = 0; i < K; i++) {
        c->entrar_lectura();
        int dentro = ++lectoras_dentro;
        if (escritoras_dentro > 0) violaciones++;
        int max = max_lectoras;
        while (dentro > max && !max_lectoras.compare_exchange_weak(max, dentro)) {}
 
        if (reg.doble_version != 2 * reg.version) inconsistencias++;
        if (dur_lec > 0) std::this_thread::sleep_for(std::chrono::microseconds(dur_lec));
 
        --lectoras_dentro;
        c->salir_lectura();
    }
}
 
void escritora(ControlRW* c) {
    for (long i = 0; i < K; i++) {
        c->entrar_escritura();
        if (++escritoras_dentro != 1 || lectoras_dentro > 0) violaciones++;
 
        reg.version++;
        reg.doble_version = 2 * reg.version;
        if (dur_esc > 0) std::this_thread::sleep_for(std::chrono::microseconds(dur_esc));
 
        --escritoras_dentro;
        c->salir_escritura();
    }
}
 
// R1
bool leer(const char* s, long& v) {
    errno = 0;
    char* fin;
    v = std::strtol(s, &fin, 10);
    return errno == 0 && fin != s && *fin == '\0';
}
 
int error(const std::string& msg) {
    std::cerr << "Error: " << msg << "\n";
    return 1;
}
 
int main(int argc, char* argv[]) {
    if (argc != 8 && argc != 9)
        return error("uso: ./rw <modo> <politica> <L> <E> <K> <dur_lec_us> <dur_esc_us> [notraza]");
 
    std::string modo = argv[1], pol = argv[2];
    if (modo != "cond" && modo != "atomic") return error("modo invalido (cond | atomic)");
    if (pol != "lectores" && pol != "escritores" && pol != "fifo") return error("politica invalida");
    if (modo == "atomic" && pol != "escritores") return error("atomic solo implementa escritores");
 
    long L, E;
    if (!leer(argv[3], L) || !leer(argv[4], E) || !leer(argv[5], K) ||
        !leer(argv[6], dur_lec) || !leer(argv[7], dur_esc))
        return error("los parametros numericos deben ser enteros");
    if (L < 1 || E < 1 || L + E > 1000 || K < 1 || dur_lec < 0 || dur_esc < 0)
        return error("se requiere L,E >= 1, L+E <= 1000, K >= 1 y duraciones >= 0");
    if (K > LONG_MAX / (2 * E) || K > LONG_MAX / (L + E))
        return error("2EK o (L+E)K no cabe en un long");
 
    // ultimo parametro opcional
    if (argc == 9 && std::string(argv[8]) != "notraza")
        return error("el ultimo parametro solo puede ser notraza");
 
    if (modo == "atomic")
    return error("aun no implementado");
    Politica politica = Politica::Lectores;
    if (pol == "escritores")
    politica = Politica::Escritores;
    else if (pol == "fifo")
    politica = Politica::Fifo;

    MonitorRW monitor(politica);
 
    std::vector<std::thread> hebras;
    bool fallo = false;
    try {
        for (int i = 0; i < L; i++) hebras.emplace_back(lectora, &monitor);
        for (int i = 0; i < E; i++) hebras.emplace_back(escritora, &monitor);
    } catch (const std::system_error& e) {
        std::cerr << "Error al crear hebras: " << e.what() << "\n";
        fallo = true;
    }
    for (auto& h : hebras) h.join();
    if (fallo) return 1;
 
    bool correcto = reg.version == E * K && reg.doble_version == 2 * E * K &&
                    inconsistencias == 0 && violaciones == 0;
 
    std::cout << "modo=" << modo << " politica=" << pol << " operaciones=" << (L + E) * K << "\n"
              << "version_final=" << reg.version << " doble_version_final=" << reg.doble_version << "\n"
              << "inconsistencias=" << inconsistencias << " violaciones=" << violaciones << "\n"
              << "max_lectoras_simultaneas=" << max_lectoras << "\n"
              << "correcto=" << (correcto ? "SI" : "NO") << "\n";
    return 0;
}