import socket
import threading
import time
import sys
import os


detener = threading.Event()
candado_envio = threading.Lock()
candado_tarea = threading.Lock()
registros = []


def enviar_resultado(conexion, mensaje):
    # El candado evita mezclar respuestas de dos hilos en el mismo socket.
    with candado_envio:
        try:
            conexion.sendall((mensaje + "\n").encode("utf-8"))
            # Las consultas no se guardan para evitar registros que se repiten.
            if "|FETCH_LOGS|" not in mensaje:
                registros.append(mensaje)
                if len(registros) > 10:
                    del registros[0]
            print(mensaje, flush=True)
        except OSError:
            detener.set()


def enviar_latido(canal_udp, direccion_maestro, identificador, uso_cpu):
    estado = "ACTIVO"
    if candado_tarea.locked():
        estado = "OCUPADO"
    mensaje = "PING:" + identificador + "|" + str(uso_cpu) + "|" + estado
    canal_udp.sendto(mensaje.encode("utf-8"), (direccion_maestro, 5000))
    datos, direccion = canal_udp.recvfrom(1024)
    return datos.decode("utf-8") == "ACK:" + identificador and direccion == (direccion_maestro, 5000)


def mantener_latidos(canal_udp, direccion_maestro, identificador):
    tiempo_anterior = time.monotonic()
    cpu_anterior = time.process_time()
    while not detener.wait(2):
        tiempo_actual = time.monotonic()
        cpu_actual = time.process_time()
        # CPU del proceso Worker como porcentaje de la capacidad del equipo.
        uso_cpu = 100 * (cpu_actual - cpu_anterior)
        uso_cpu = uso_cpu / ((tiempo_actual - tiempo_anterior) * (os.cpu_count() or 1))
        tiempo_anterior = tiempo_actual
        cpu_anterior = cpu_actual
        try:
            if not enviar_latido(canal_udp, direccion_maestro, identificador, round(uso_cpu, 2)):
                print("ACK no válido.", flush=True)
        except OSError:
            if not detener.is_set():
                print("No llegó el ACK del latido.", flush=True)


def generar_peticiones(objetivo, fin, resultado):
    canal = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    canal.settimeout(0.3)
    try:
        # Cada hilo envía como máximo unas 50 peticiones por segundo.
        while time.monotonic() < fin and not detener.is_set():
            inicio_peticion = time.monotonic()
            try:
                canal.sendto(b"PRUEBA", (objetivo, 5002))
                respuesta, direccion = canal.recvfrom(1024)
                if respuesta == b"RECIBIDO" and direccion == (objetivo, 5002):
                    resultado[0] += 1
                    resultado[2] += (time.monotonic() - inicio_peticion) * 1000
                else:
                    resultado[1] += 1
            except OSError:
                resultado[1] += 1
            detener.wait(0.02)
    finally:
        canal.close()


def ejecutar_carga(conexion, identificador, orden, direccion_maestro):
    comando, texto_inicio = orden.split("|")
    nombre_comando, objetivo, texto_duracion = comando.split(":")
    duracion = int(texto_duracion)
    inicio = float(texto_inicio)
    if objetivo != direccion_maestro or duracion < 1 or duracion > 10:
        raise ValueError("El objetivo debe ser el Master y la duración de 1 a 10 segundos.")
    if not time.time() - 1 <= inicio <= time.time() + 10:
        raise ValueError("La hora de inicio está fuera de plazo.")
    print("[CARGA] Preparado para iniciar en " + str(inicio), flush=True)
    if detener.wait(max(0, inicio - time.time())):
        return
    inicio_real = time.time()
    fin = time.monotonic() + max(0, inicio + duracion - time.time())
    hilos = []
    resultados = []
    for numero in range(4):
        # Cada hilo modifica su propia lista: recibidos, errores y latencia.
        resultado = [0, 0, 0.0]
        resultados.append(resultado)
        hilo = threading.Thread(target=generar_peticiones, args=(objetivo, fin, resultado), daemon=True)
        hilos.append(hilo)
        hilo.start()
    for hilo in hilos:
        hilo.join()
    recibidos = 0
    errores = 0
    latencia_total = 0.0
    for resultado in resultados:
        recibidos += resultado[0]
        errores += resultado[1]
        latencia_total += resultado[2]
    latencia_media = 0.0
    if recibidos > 0:
        latencia_media = latencia_total / recibidos
    duracion_real = max(0.001, time.time() - inicio_real)
    informe = "RESULTADO:" + identificador + "|STRESS|OK"
    informe += "|inicio=" + str(round(inicio_real, 3))
    informe += "|recibidos=" + str(recibidos) + "|errores=" + str(errores)
    informe += "|duracion_s=" + str(round(duracion_real, 3))
    informe += "|peticiones_segundo=" + str(round(recibidos / duracion_real, 2))
    informe += "|latencia_ms=" + str(round(latencia_media, 3))
    enviar_resultado(conexion, informe)


def ejecutar_tarea(conexion, identificador, orden, direccion_maestro):
    # Una tarea de trabajo por Worker; su recepción TCP sigue disponible.
    if not candado_tarea.acquire(blocking=False):
        enviar_resultado(conexion, "RESULTADO:" + identificador + "|ERROR|Worker ocupado")
        return
    try:
        if orden == "EXEC_TASK:scan_ports":
            abiertos = []
            for puerto in (5001, 5002, 5003):
                canal = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                canal.settimeout(0.3)
                try:
                    if canal.connect_ex((direccion_maestro, puerto)) == 0:
                        abiertos.append(str(puerto))
                finally:
                    canal.close()
            enviar_resultado(conexion, "RESULTADO:" + identificador + "|scan_ports|OK|abiertos=" + ",".join(abiertos))
        elif orden.startswith("BROADCAST_STRESS:"):
            ejecutar_carga(conexion, identificador, orden, direccion_maestro)
        else:
            enviar_resultado(conexion, "RESULTADO:" + identificador + "|ERROR|Comando desconocido")
    except (OSError, ValueError) as error:
        enviar_resultado(conexion, "RESULTADO:" + identificador + "|ERROR|" + str(error))
    finally:
        candado_tarea.release()


def iniciar():
    if len(sys.argv) < 2:
        print("Uso: python worker.py nodo_1 [direccion_del_master]")
        return
    identificador = sys.argv[1]
    if len(identificador) > 30 or not identificador.replace("_", "").replace("-", "").isalnum():
        print("Usa un identificador de 1 a 30 letras, números, guiones o guiones bajos.")
        return
    direccion_maestro = "127.0.0.1"
    if len(sys.argv) > 2:
        direccion_maestro = sys.argv[2]
    canal_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    conexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lector = None
    hilos = []
    try:
        direccion_maestro = socket.gethostbyname(direccion_maestro)
        canal_udp.settimeout(1)
        if not enviar_latido(canal_udp, direccion_maestro, identificador, 0):
            print("El Master no confirmó el registro UDP.")
            return
        conexion.settimeout(5)
        conexion.connect((direccion_maestro, 5001))
        conexion.sendall(("REGISTRO:" + identificador + "\n").encode("utf-8"))
        lector = conexion.makefile("r", encoding="utf-8")
        confirmacion = lector.readline().strip()
        if confirmacion != "REGISTRADO:" + identificador:
            print("Registro TCP rechazado: " + confirmacion)
            return
        conexion.settimeout(None)
        print("[TCP] Registrado: " + identificador, flush=True)
        hilo = threading.Thread(target=mantener_latidos, args=(canal_udp, direccion_maestro, identificador), daemon=True)
        hilo.start()
        hilos.append(hilo)
        for linea in lector:
            orden = linea.strip()
            if orden == "FETCH_LOGS":
                with candado_envio:
                    detalle = "; ".join(registros)
                enviar_resultado(conexion, "RESULTADO:" + identificador + "|FETCH_LOGS|OK|" + detalle)
            else:
                hilo = threading.Thread(target=ejecutar_tarea, args=(conexion, identificador, orden, direccion_maestro), daemon=True)
                hilo.start()
                hilos.append(hilo)
        print("El Master cerró la conexión.", flush=True)
    except (KeyboardInterrupt, EOFError):
        print("\nCerrando Worker...")
    except (OSError, UnicodeError) as error:
        print("Error de conexión: " + str(error), flush=True)
    finally:
        detener.set()
        try:
            conexion.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        for hilo in hilos:
            hilo.join(timeout=1)
        if lector is not None:
            lector.close()
        conexion.close()
        canal_udp.close()


if __name__ == "__main__":
    iniciar()
