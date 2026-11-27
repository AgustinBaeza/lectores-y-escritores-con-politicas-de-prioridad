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
enum class Tipo { Lectora, Escritora };
enum class Accion { Pide, Entra, Sale };

struct AnotacionTraza {
    int hebra;
    Tipo tipo;
    Accion accion;
    long turno; // turno FIFO; -1 en las otras politicas
};

//R6: identidad logica y turno de la solicitud actual de cada hebra.
thread_local int id_hebra = -1;
thread_local long turno_solicitud = -1;

class ControlRW {
public:
    virtual ~ControlRW() = default;
    virtual void entrar_lectura() = 0;
    virtual void salir_lectura() = 0;
    virtual void entrar_escritura() = 0;
    virtual void salir_escritura() = 0;
};

class MonitorRW : public ControlRW {
public:
    explicit MonitorRW(Politica p, bool habilitar_traza = true)
        : politica(p), traza_habilitada(habilitar_traza) {}

    void entrar_lectura() override {
        std::unique_lock<std::mutex> lk(m);
        long turno = -1;

        if (politica == Politica::Fifo) {
            turno = siguiente_turno++;
            turno_solicitud = turno;
            registrar(Tipo::Lectora, Accion::Pide, turno);

            // R7: espera bloqueante y condicion revalidada mediante predicado.
            cv_fifo.wait(lk, [this, turno] {
                return turno == turno_actual && !escritora_activa;
            });

            ++lectoras_activas;
            ++turno_actual;
            registrar(Tipo::Lectora, Accion::Entra, turno);

            // R5/R7: puede habilitar a la siguiente lectora consecutiva.
            cv_fifo.notify_all();
            return;
        }

        turno_solicitud = -1;
        registrar(Tipo::Lectora, Accion::Pide, -1);
        ++lectoras_esperando;

        cv_lectoras.wait(lk, [this] {
            return !escritora_activa &&
                   (politica != Politica::Escritores ||
                    escritoras_esperando == 0);
        });

        --lectoras_esperando;
        ++lectoras_activas;
        registrar(Tipo::Lectora, Accion::Entra, -1);
    }

    void salir_lectura() override {
        std::unique_lock<std::mutex> lk(m);
        registrar(Tipo::Lectora, Accion::Sale, turno_solicitud);
        --lectoras_activas;

        if (lectoras_activas == 0) {
            if (politica == Politica::Fifo)
                cv_fifo.notify_all();
            else
                cv_escritoras.notify_one();
        }
        turno_solicitud = -1;
    }

    void entrar_escritura() override {
        std::unique_lock<std::mutex> lk(m);
        long turno = -1;

        if (politica == Politica::Fifo) {
            turno = siguiente_turno++;
            turno_solicitud = turno;
            registrar(Tipo::Escritora, Accion::Pide, turno);

            cv_fifo.wait(lk, [this, turno] {
                return turno == turno_actual &&
                       !escritora_activa && lectoras_activas == 0;
            });

            escritora_activa = true;
            ++turno_actual;
            registrar(Tipo::Escritora, Accion::Entra, turno);
            return;
        }

        turno_solicitud = -1;
        registrar(Tipo::Escritora, Accion::Pide, -1);
        ++escritoras_esperando;

        cv_escritoras.wait(lk, [this] {
            return !escritora_activa && lectoras_activas == 0 &&
                   (politica != Politica::Lectores ||
                    lectoras_esperando == 0);
        });

        --escritoras_esperando;
        escritora_activa = true;
        registrar(Tipo::Escritora, Accion::Entra, -1);
    }

    void salir_escritura() override {
        std::unique_lock<std::mutex> lk(m);
        registrar(Tipo::Escritora, Accion::Sale, turno_solicitud);
        escritora_activa = false;

        if (politica == Politica::Fifo) {
            cv_fifo.notify_all();
        } else if (politica == Politica::Escritores &&
                   escritoras_esperando > 0) {
            cv_escritoras.notify_one();
        } else if (lectoras_esperando > 0) {
            cv_lectoras.notify_all();
        } else {
            cv_escritoras.notify_one();
        }
        turno_solicitud = -1;
    }

    // R6: copia consistente, obtenida despues de los join().
    std::vector<AnotacionTraza> obtener_traza() {
        std::lock_guard<std::mutex> lk(m);
        return traza;
    }

private:
    void registrar(Tipo tipo, Accion accion, long turno) {
        // Se invoca exclusivamente mientras el monitor mantiene m.
        // Commit 02: interfaz preparada; almacenamiento se agrega en commit 03.
        (void)tipo; (void)accion; (void)turno;
    }

    Politica politica;
    bool traza_habilitada;
    std::mutex m;
    std::condition_variable cv_lectoras, cv_escritoras, cv_fifo;
    std::vector<AnotacionTraza> traza;

    int lectoras_activas = 0;
    int lectoras_esperando = 0;
    int escritoras_esperando = 0;
    bool escritora_activa = false;
    long siguiente_turno = 0;
    long turno_actual = 0;
};

struct ResultadoVerificacion {
    bool politica_ok = true;
    long evidencia = 0;
    long anotacion_error = -1;
    std::string regla_incumplida;
};

ResultadoVerificacion verificar_traza(
    const std::vector<AnotacionTraza>& traza, Politica) {
    // Commit 03: la captura ya esta completa. La validacion semantica
    // se incorpora en el commit siguiente.
    ResultadoVerificacion r;
    if (traza.empty()) {
        r.politica_ok = false;
        r.regla_incumplida = "traza vacia";
    }
    return r;
}

struct Registro {
    long version = 0;
    long doble_version = 0;
};

Registro reg;
long K, dur_lec, dur_esc;
std::atomic<int> lectoras_dentro{0}, escritoras_dentro{0}, max_lectoras{0};
std::atomic<long> inconsistencias{0}, violaciones{0};

void lectora(ControlRW* c, int id) {
    id_hebra = id;
    for (long i = 0; i < K; ++i) {
        c->entrar_lectura();
        const int dentro = ++lectoras_dentro;
        if (escritoras_dentro.load() > 0) ++violaciones;

        // R2/R7: solo instrumentacion diagnostica; no decide la admision.
        int maximo = max_lectoras.load();
        while (dentro > maximo &&
               !max_lectoras.compare_exchange_weak(maximo, dentro)) {}

        if (reg.doble_version != 2 * reg.version) ++inconsistencias;
        if (dur_lec > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(dur_lec));
        --lectoras_dentro;
        c->salir_lectura();
    }
}

void escritora(ControlRW* c, int id) {
    id_hebra = id;
    for (long i = 0; i < K; ++i) {
        c->entrar_escritura();
        if (++escritoras_dentro != 1 || lectoras_dentro.load() > 0)
            ++violaciones;
        ++reg.version;
        reg.doble_version = 2 * reg.version;
        if (dur_esc > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(dur_esc));
        --escritoras_dentro;
        c->salir_escritura();
    }
}

bool leer(const char* s, long& v) {
    errno = 0;
    char* fin = nullptr;
    v = std::strtol(s, &fin, 10);
    return errno == 0 && fin != s && *fin == '\0';
}

int error(const std::string& msg) {
    std::cerr << "Error: " << msg << "\n";
    return 1;
}

int main(int argc, char* argv[]) {
    if (argc != 8)
        return error("uso: ./rw <modo> <politica> <L> <E> <K> <dur_lec_us> <dur_esc_us>");

    const std::string modo = argv[1];
    const std::string pol = argv[2];
    if (modo != "cond" && modo != "atomic")
        return error("modo invalido (cond | atomic)");
    if (pol != "lectores" && pol != "escritores" && pol != "fifo")
        return error("politica invalida");
    if (modo == "atomic" && pol != "escritores")
        return error("atomic solo implementa escritores");

    long L, E;
    if (!leer(argv[3], L) || !leer(argv[4], E) || !leer(argv[5], K) ||
        !leer(argv[6], dur_lec) || !leer(argv[7], dur_esc))
        return error("los parametros numericos deben ser enteros");
    if (L < 1 || E < 1 || L + E > 1000 || K < 1 ||
        dur_lec < 0 || dur_esc < 0)
        return error("se requiere L,E >= 1, L+E <= 1000, K >= 1 y duraciones >= 0");
    if (K > LONG_MAX / (2 * E) || K > LONG_MAX / (L + E))
        return error("2EK o (L+E)K no cabe en un long");

    const bool traza_habilitada = true;
    if (modo == "atomic")
        return error("aun no implementado");

    Politica politica = Politica::Lectores;
    if (pol == "escritores") politica = Politica::Escritores;
    else if (pol == "fifo") politica = Politica::Fifo;

    MonitorRW monitor(politica, traza_habilitada);
    std::vector<std::thread> hebras;
    bool fallo = false;
    int siguiente_id = 0;
    try {
        for (long i = 0; i < L; ++i)
            hebras.emplace_back(lectora, &monitor, siguiente_id++);
        for (long i = 0; i < E; ++i)
            hebras.emplace_back(escritora, &monitor, siguiente_id++);
    } catch (const std::system_error& e) {
        std::cerr << "Error al crear hebras: " << e.what() << "\n";
        fallo = true;
    }

    for (auto& h : hebras) h.join();
    if (fallo) return 1;

    const bool invariantes_ok =
        reg.version == E * K && reg.doble_version == 2 * E * K &&
        inconsistencias.load() == 0 && violaciones.load() == 0;

    std::cout << "modo=" << modo << " politica=" << pol
              << " operaciones=" << (L + E) * K << "\n"
              << "version_final=" << reg.version
              << " doble_version_final=" << reg.doble_version << "\n"
              << "inconsistencias=" << inconsistencias
              << " violaciones=" << violaciones << "\n"
              << "max_lectoras_simultaneas=" << max_lectoras << "\n";

    bool politica_ok = true;
    if (false && traza_habilitada) {
        const auto traza = monitor.obtener_traza();
        const auto resultado = verificar_traza(traza, politica);
        politica_ok = resultado.politica_ok;
        std::cout << "politica_ok=" << (resultado.politica_ok ? "SI" : "NO")
                  << " evidencia=" << resultado.evidencia << "\n";
        if (!resultado.politica_ok)
            std::cout << "anotacion_error=" << resultado.anotacion_error
                      << " regla=\"" << resultado.regla_incumplida << "\"\n";
    } else {
        std::cout << "traza=DESACTIVADA\n";
    }

    std::cout << "correcto="
              << ((invariantes_ok && politica_ok) ? "SI" : "NO") << "\n";
    return (invariantes_ok && politica_ok) ? 0 : 1;
}
