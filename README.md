# Asunto
**Información de técnica y manual de usuario del script de sincronización entre Bitrix y Glyms**
## Contenido
### Documentación del Script de Sincronización: Glyms ↔ Bitrix24
Versión del Documento: 1.0 Fecha: 11 de Junio de 2025 Nombre del Script: sync_glyms_b24.py

## 1. Documentación para el Usuario
Esta sección está destinada a la persona encargada de ejecutar y supervisar el script de sincronización en el día a día.

### 1.1. Propósito del Script
Este script automatiza la sincronización de datos entre el sistema Glyms (base de datos PostgreSQL) y el portal de Bitrix24. Realiza dos tareas principales de forma independiente:

Sincronización de Servicios: Sincroniza la lista de servicios y sus precios desde Glyms hacia el catálogo de productos de Bitrix24.
Sincronización de Empresas: Sincroniza los clientes (creación, actualización y eliminación) desde Glyms hacia el módulo de Empresas (Compañías) en el CRM de Bitrix24, utilizando un sistema de registro de cambios para mayor eficiencia.
### 1.2. Requisitos Previos
Antes de ejecutar el script, asegúrate de que el sistema donde se va a ejecutar cumpla con lo siguiente:

Python 3.8 o superior instalado.
Acceso a la línea de comandos o terminal.
Librerías de Python necesarias instaladas. Puedes instalarlas ejecutando el siguiente comando en la terminal: pip install requests psycopg2-binary
Acceso de red desde la máquina que ejecuta el script tanto al servidor de la base de datos PostgreSQL de Glyms como a internet para acceder a la API de Bitrix24.
### 1.3. Configuración del Script
Antes de la primera ejecución, es crucial configurar correctamente las constantes al principio del archivo del script (sync_glyms_b24.py).

Credenciales de PostgreSQL (PG_...): Asegúrate de que PG_HOST, PG_DATABASE, PG_USER y PG_PASSWORD sean correctos.
Credenciales de Bitrix24 (B24_...): Rellena B24_CLIENT_ID y B24_CLIENT_SECRET con los valores de tu aplicación local creada en Bitrix24.
IDs de Bitrix24: Verifica que los IDs como B24_IBLOCK_ID_SERVICES, B24_PRICE_TYPE_ID_SERVICES y los códigos de campos personalizados (UF_CRM_...) sean correctos.
Mapeos: Revisa los diccionarios de mapeo (B24_SECTION_MAPPING_SERVICES, GLYMS_IDCLIENTETIPO_TO_B24_INDUSTRY_ID, GLYMS_IDTIPOT_TO_B24_COMPANY_TYPE_ID) para asegurar que los IDs de Glyms se correspondan con los IDs correctos de tu instancia de Bitrix24.
### 1.4. Guía de Ejecución
El script se ejecuta desde la línea de comandos.

**Paso 1: Autorización Inicial (Primera Vez)**
La primera vez que uses el script, o si el archivo bitrix24_tokens.json se borra, necesitas autorizarlo.

Abre la terminal y ejecuta el script sin argumentos: python sync_glyms_b24.py
El script te mostrará un mensaje indicando que no hay tokens y te proporcionará una URL.
Copia la URL proporcionada y pégala en un navegador web.
Inicia sesión en Bitrix24 y autoriza los permisos que la aplicación solicita.
El navegador será redirigido a una página (probablemente http://localhost) que mostrará un error de conexión. ¡Es normal! La URL en la barra de direcciones contendrá un código: http://localhost/?code=UN_CODIGO_LARGO_Y_SECRETO&...
Copia solamente el valor del código.
Vuelve a la terminal y ejecuta el script con el argumento --auth_code: python sync_glyms_b24.py --auth_code=EL_CODIGO_QUE_COPIASTE
Esto creará el archivo bitrix24_tokens.json y el script estará listo.
**Paso 2: Ejecución Normal (Sincronización)**
Para ejecutar las sincronizaciones, usa el argumento --sync_type:

Sincronizar todo (servicios y luego empresas): python sync_glyms_b24.py o python sync_glyms_b24.py --sync_type all

Sincronizar solo los Servicios: python sync_glyms_b24.py --sync_type services

Sincronizar solo las Empresas: python sync_glyms_b24.py --sync_type companies

**Paso 3: Modo de Depuración**
Si necesitas ver un log muy detallado, añade el argumento --debug a cualquiera de los comandos anteriores: python sync_glyms_b24.py --sync_type companies --debug

**Paso 4: Funciones de Utilidad (Listar Directorios y Precios)**
Para listar los Tipos de Precio del Catálogo: python sync_glyms_b24.py --price_types La salida te mostrará el ID que necesitas para la variable B24_PRICE_TYPE_ID_SERVICES.

Para listar clasificadores del CRM (Tipos de Empresa, Sectores, etc.): Usa --directory_items seguido por el ENTITY_ID que quieres consultar.

Listar Tipos de Empresa: python sync_glyms_b24.py --directory_items COMPANY_TYPE
Listar Sectores/Industrias: python sync_glyms_b24.py --directory_items INDUSTRY La salida te mostrará el STATUS_ID, que es el valor que necesitas usar en los diccionarios de mapeo.
### 1.5. Programación de Tareas Automáticas
Para que el script se ejecute diariamente, puedes programarlo usando el "Programador de Tareas" de Windows o cron en Linux/macOS.

## 2. Documentación Técnica
Esta sección está destinada a desarrolladores o administradores de sistemas que necesiten entender, mantener o extender la funcionalidad del script.

### 2.1. Arquitectura General
El script es una aplicación de consola en Python 3 que actúa como un cliente de integración entre una base de datos PostgreSQL (Glyms) y la API REST de Bitrix24. Utiliza la autenticación OAuth 2.0 para una aplicación local.

### 2.2. Requisitos de la Base de Datos (Glyms)
Para que la sincronización de empresas funcione, se requiere la siguiente configuración en PostgreSQL:

Tabla de Log: Una tabla para registrar los cambios en public.cliente.
SQL
```
CREATE TABLE f_cliente.sync_log_bitrix24 (
    log_id SERIAL PRIMARY KEY,
    id_cliente INTEGER NOT NULL,
    operation_type VARCHAR(10) NOT NULL,
    change_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    sync_status VARCHAR(20) DEFAULT 'PENDING',
    processed_timestamp TIMESTAMP WITH TIME ZONE NULL,
    error_message TEXT NULL
);
Función de Trigger:
SQL

CREATE OR REPLACE FUNCTION f_cliente.fn_log_btrix24_cliente_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO f_cliente.sync_log_bitrix24 (id_cliente, operation_type) VALUES (NEW.id_cliente, 'INSERT');
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO f_cliente.sync_log_bitrix24 (id_cliente, operation_type) VALUES (NEW.id_cliente, 'UPDATE');
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO f_cliente.sync_log_bitrix24 (id_cliente, operation_type) VALUES (OLD.id_cliente, 'DELETE');
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
Triggers: Tres triggers en la tabla public.cliente que llaman a la función anterior (AFTER INSERT, AFTER UPDATE, AFTER DELETE).
```
### 2.3. Estructura del Código
CONFIGURACIÓN: Sección superior para todas las constantes y credenciales.
Funciones Auxiliares: Utilidades generales (is_valid_email, log_message, log_debug).
Funciones de API de Bitrix24: Gestionan el ciclo de vida de los tokens OAuth 2.0 y realizan las llamadas a la API REST, con lógica de reintento para tokens expirados.
Función de PostgreSQL: get_glyms_data() para ejecutar consultas y devolver los resultados.
Funciones de Utilidad de Bitrix24: display_directory_items() y display_price_types().
Funciones de Sincronización:
sync_services(): Sigue un modelo de "batch completo". Lee todos los registros activos de Glyms, los compara con los existentes en Bitrix24 y realiza las operaciones correspondientes.
sync_companies(): Sigue un modelo de "Captura de Datos de Cambios" (CDC), procesando una tabla de log (f_cliente.sync_log_bitrix24) que es alimentada por triggers.
Bloque Principal (if __name__ == "__main__":): Utiliza argparse para procesar los argumentos de la línea de comandos y orquestar qué funciones principales se deben llamar.
