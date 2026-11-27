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

// R6: identidad logica y turno de la solicitud actual de cada hebra.
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
        if (traza_habilitada)
            traza.push_back({id_hebra, tipo, accion, turno});
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
    const std::vector<AnotacionTraza>& traza, Politica politica) {

    ResultadoVerificacion r;
    long entradas_fifo = 0;
    int lectoras_activas = 0;
    int escritoras_activas = 0;
    int lectoras_pendientes = 0;
    int escritoras_pendientes = 0;

    auto fallar = [&](long i, const std::string& regla) {
        if (r.politica_ok) {
            r.politica_ok = false;
            r.anotacion_error = i;
            r.regla_incumplida = regla;
        }
    };

    for (long i = 0; i < static_cast<long>(traza.size()); ++i) {
        const AnotacionTraza& a = traza[static_cast<std::size_t>(i)];

        if (a.hebra < 0)
            fallar(i, "identificador logico de hebra invalido");
        if (politica == Politica::Fifo && a.turno < 0)
            fallar(i, "FIFO exige turno no negativo en toda anotacion");
        if (politica != Politica::Fifo && a.turno != -1)
            fallar(i, "las politicas no FIFO exigen turno -1");

        if (a.accion == Accion::Pide) {
            if (a.tipo == Tipo::Lectora) {
                // Evidencia R6 para prioridad de escritores: la regla se puso a prueba.
                if (politica == Politica::Escritores &&
                    escritoras_pendientes > 0)
                    ++r.evidencia;

                // R6 Lectores: si no hay escritora activa, debe entrar inmediatamente;
                // al conservarse el mutex, Entra debe ser la anotacion siguiente.
                if (politica == Politica::Lectores &&
                    escritoras_activas == 0) {
                    const bool siguiente_es_su_entrada =
                        i + 1 < static_cast<long>(traza.size()) &&
                        traza[static_cast<std::size_t>(i + 1)].accion == Accion::Entra &&
                        traza[static_cast<std::size_t>(i + 1)].tipo == Tipo::Lectora &&
                        traza[static_cast<std::size_t>(i + 1)].hebra == a.hebra;
                    if (!siguiente_es_su_entrada)
                        fallar(i, "Lectores: una lectora que pide sin escritora activa debe entrar en la anotacion siguiente");
                }
                ++lectoras_pendientes;
            } else {
                ++escritoras_pendientes;
            }
            continue;
        }

        if (a.accion == Accion::Entra) {
            if (politica == Politica::Fifo) {
                if (a.turno != entradas_fifo)
                    fallar(i, "FIFO: el k-esimo Entra debe corresponder al turno k");
                ++entradas_fifo;
            }

            if (a.tipo == Tipo::Lectora) {
                if (escritoras_activas != 0)
                    fallar(i, "Todas: una lectora entra sin escritora activa");
                if (lectoras_pendientes <= 0)
                    fallar(i, "Entra de lectora sin solicitud pendiente");
                if (politica == Politica::Escritores &&
                    escritoras_pendientes > 0)
                    fallar(i, "Escritores: una lectora entra sin escritoras pendientes");
                if (politica == Politica::Lectores &&
                    escritoras_pendientes > 0)
                    ++r.evidencia;
                if (politica == Politica::Fifo && lectoras_activas > 0)
                    ++r.evidencia;
                --lectoras_pendientes;
                ++lectoras_activas;
            } else {
                if (lectoras_activas != 0 || escritoras_activas != 0)
                    fallar(i, "Todas: una escritora entra sin lectoras ni escritora activas");
                if (escritoras_pendientes <= 0)
                    fallar(i, "Entra de escritora sin solicitud pendiente");
                if (politica == Politica::Lectores && lectoras_pendientes > 0)
                    fallar(i, "Lectores: una escritora entra sin lectoras pendientes");
                --escritoras_pendientes;
                ++escritoras_activas;
            }
            continue;
        }

        // Accion::Sale
        if (a.tipo == Tipo::Lectora) {
            if (lectoras_activas <= 0)
                fallar(i, "Sale de lectora sin lectora activa");
            else
                --lectoras_activas;
        } else {
            if (escritoras_activas != 1)
                fallar(i, "Sale de escritora sin exactamente una escritora activa");
            else
                --escritoras_activas;
        }
    }

    if (lectoras_activas != 0 || escritoras_activas != 0 ||
        lectoras_pendientes != 0 || escritoras_pendientes != 0)
        fallar(static_cast<long>(traza.size()),
               "la traza termina con solicitudes activas o pendientes");

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
    if (traza_habilitada) {
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

