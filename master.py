import socket
import threading
import time
import sys


# Tablas compartidas: último latido y conexión TCP de cada Worker.
nodos = {}
conexiones = {}
candado = threading.Lock()
candado_consola = threading.Lock()
detener = threading.Event()
hilos = []


def mostrar(mensaje):
    # Los hilos comparten la consola: imprimimos cada mensaje completo.
    with candado_consola:
        print(mensaje, flush=True)


def recibir_latidos(canal_udp):
    while not detener.is_set():
        try:
            datos, direccion = canal_udp.recvfrom(1024)
            mensaje = datos.decode("utf-8")
            if not mensaje.startswith("PING:"):
                continue
            identificador, uso_cpu, estado = mensaje[5:].split("|")
            float(uso_cpu)
            if not identificador or estado not in ("ACTIVO", "OCUPADO"):
                continue
            with candado:
                nuevo = identificador not in nodos
                if not nuevo:
                    nuevo = nodos[identificador]["estado"] == "CAIDO"
                nodos[identificador] = {
                    "ultimo_latido": time.monotonic(),
                    "cpu": uso_cpu,
                    "estado": estado,
                }
            canal_udp.sendto(("ACK:" + identificador).encode("utf-8"), direccion)
            if nuevo:
                mostrar("[UDP] Registrado: " + identificador)
        except socket.timeout:
            continue
        except (ValueError, UnicodeError):
            mostrar("[UDP] Latido con formato incorrecto.")
        except OSError:
            break


def vigilar_nodos():
    while not detener.wait(0.5):
        with candado:
            for identificador in nodos:
                nodo = nodos[identificador]
                if nodo["estado"] != "CAIDO":
                    if time.monotonic() - nodo["ultimo_latido"] >= 6:
                        nodo["estado"] = "CAIDO"
                        mostrar("[UDP] " + identificador + ": Caído / Desconectado")


def atender_worker(conexion):
    identificador = ""
    lector = None
    try:
        # Cada línea es un mensaje completo: TCP puede entregarlo por partes.
        conexion.settimeout(5)
        lector = conexion.makefile("r", encoding="utf-8")
        registro = lector.readline(1024).strip()
        if not registro.startswith("REGISTRO:"):
            return
        identificador = registro[9:]
        with candado:
            if identificador not in nodos or identificador in conexiones:
                conexion.sendall(b"ERROR:registro_invalido_o_duplicado\n")
                return
            if nodos[identificador]["estado"] == "CAIDO":
                return
            conexiones[identificador] = conexion
        conexion.sendall(("REGISTRADO:" + identificador + "\n").encode("utf-8"))
        conexion.settimeout(None)
        mostrar("[TCP] Conectado: " + identificador)
        for linea in lector:
            mostrar("[INFORME] " + linea.strip())
    except (OSError, UnicodeError) as error:
        if not detener.is_set():
            mostrar("[TCP] Desconexión: " + identificador + " " + str(error))
    finally:
        with candado:
            if conexiones.get(identificador) is conexion:
                del conexiones[identificador]
        if lector is not None:
            lector.close()
        conexion.close()
        if identificador:
            mostrar("[TCP] Sesión cerrada: " + identificador)


def aceptar_workers(canal_tcp):
    while not detener.is_set():
        try:
            conexion, direccion = canal_tcp.accept()
            hilo = threading.Thread(target=atender_worker, args=(conexion,), daemon=True)
            hilo.start()
            hilos.append(hilo)
        except socket.timeout:
            continue
        except OSError:
            break


def atender_objetivo(canal_objetivo):
    # Receptor UDP para la simulación de carga de la actividad 3.
    while not detener.is_set():
        try:
            datos, direccion = canal_objetivo.recvfrom(1024)
            if datos == b"PRUEBA":
                canal_objetivo.sendto(b"RECIBIDO", direccion)
        except socket.timeout:
            continue
        except OSError:
            break


def enviar_comando(identificador, comando):
    # Solo la CLI escribe comandos en las conexiones TCP.
    with candado:
        conexion = conexiones.get(identificador)
        activo = identificador in nodos and nodos[identificador]["estado"] != "CAIDO"
    if conexion is None or not activo:
        mostrar("Worker sin conexión activa: " + identificador)
        return
    try:
        conexion.sendall((comando + "\n").encode("utf-8"))
        mostrar("[ORDEN] " + identificador + " <- " + comando)
    except OSError:
        mostrar("No se pudo enviar a " + identificador)


def interfaz(direccion_maestro):
    mostrar("Comandos: nodos | enviar <nodo> <comando> | BROADCAST_STRESS:<ip>:<segundos> | salir")
    mostrar("Ejemplos: enviar nodo_1 EXEC_TASK:scan_ports | enviar nodo_1 FETCH_LOGS")
    while not detener.is_set():
        orden = input("master> ").strip()
        if orden == "salir":
            break
        elif orden == "nodos":
            with candado:
                for identificador in nodos:
                    nodo = nodos[identificador]
                    conectado = identificador in conexiones
                    mostrar(identificador + " CPU: " + nodo["cpu"] + " Estado: " + nodo["estado"] + " TCP: " + str(conectado))
        elif orden.startswith("enviar "):
            partes = orden.split(" ", 2)
            if len(partes) == 3:
                enviar_comando(partes[1], partes[2])
            else:
                mostrar("Usa: enviar nodo_1 FETCH_LOGS")
        elif orden.startswith("BROADCAST_STRESS:"):
            try:
                comando, objetivo, texto_duracion = orden.split(":")
                duracion = int(texto_duracion)
                if objetivo != direccion_maestro or duracion < 1 or duracion > 10:
                    mostrar("Usa la IP del Master y una duración entre 1 y 10 segundos.")
                    continue
                # Misma hora de inicio para todos: dos segundos en el futuro.
                inicio = time.time() + 2
                mensaje = orden + "|" + str(inicio)
                with candado:
                    identificadores = []
                    for identificador in conexiones:
                        if nodos[identificador]["estado"] != "CAIDO":
                            identificadores.append(identificador)
                for identificador in identificadores:
                    enviar_comando(identificador, mensaje)
                mostrar("[CARGA] Workers convocados: " + str(len(identificadores)))
            except ValueError:
                mostrar("Usa: BROADCAST_STRESS:127.0.0.1:3")
        else:
            mostrar("Comando no reconocido.")


def iniciar():
    direccion_maestro = "127.0.0.1"
    if len(sys.argv) > 1:
        direccion_maestro = sys.argv[1]
    canal_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    canal_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    canal_objetivo = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        canal_udp.bind((direccion_maestro, 5000))
        canal_tcp.bind((direccion_maestro, 5001))
        canal_tcp.listen(10)
        canal_objetivo.bind((direccion_maestro, 5002))
        canal_udp.settimeout(0.5)
        canal_tcp.settimeout(0.5)
        canal_objetivo.settimeout(0.5)
        for funcion, argumentos in ((recibir_latidos, (canal_udp,)), (vigilar_nodos, ()),
                                    (aceptar_workers, (canal_tcp,)), (atender_objetivo, (canal_objetivo,))):
            hilo = threading.Thread(target=funcion, args=argumentos, daemon=True)
            hilo.start()
            hilos.append(hilo)
        mostrar("Master listo en " + direccion_maestro + " (UDP 5000, TCP 5001, objetivo UDP 5002)")
        interfaz(direccion_maestro)
    except (KeyboardInterrupt, EOFError):
        mostrar("\nCerrando Master...")
    except OSError as error:
        mostrar("Error de red: " + str(error))
    finally:
        detener.set()
        with candado:
            conexiones_abiertas = list(conexiones.values())
        for conexion in conexiones_abiertas:
            try:
                conexion.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conexion.close()
        canal_udp.close()
        canal_tcp.close()
        canal_objetivo.close()
        for hilo in hilos:
            hilo.join(timeout=1)


if __name__ == "__main__":
    iniciar()
