import json
import requests
import psycopg2
import datetime
import os
import time
import urllib3
import argparse
import re

# Silenciar advertencias de InsecureRequestWarning si verify=False se usa globalmente
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURACIÓN ---
# PostgreSQL (Glyms)
PG_HOST = "192.168.1.224"
PG_PORT = "5432"
PG_DATABASE = "glymsfl10"
PG_USER = "postgres" 
PG_PASSWORD = "afA*g$$eIDz"
PG_VIEW_NAME_SERVICES = "lims.v_prestacion_max_version"
PG_TABLE_NAME_COMPANIES_MAIN = "public.cliente" 
PG_LOG_TABLE_COMPANIES = "f_cliente.sync_log_bitrix24" 
PG_ID_CONTRATO_CLINICA = 14
PG_ID_CONTRATO_VETERINARIA = 371
PG_ID_CONTRATO_INDUSTRIA = 259

# Bitrix24
B24_PORTAL_URL = "https://rapela.bitrix24.es"
B24_CLIENT_ID = "local.6834eb05ed7532.57084891"
B24_CLIENT_SECRET = "CbH8UjmJ26E4bnb1xV84047Lnxta4RTbcno5KuSmV0yVP9Yo1V"
B24_TOKEN_FILE = "bitrix24_tokens.json"
B24_PRICE_TYPE_ID_SERVICES = 2 

# Configuración específica para Servicios del Catálogo
B24_IBLOCK_ID_SERVICES = 24 
B24_CURRENCY_ID_SERVICES = "ARS" 
B24_SECTION_MAPPING_SERVICES = { 
    1: 48,
    2: 46,
    3: 44,
}

# Configuración específica para Empresas (CRM)
B24_COMPANY_CUSTOM_FIELD_GLYMS_CODIGO = "UF_CRM_1748636158002"
B24_COMPANY_CUSTOM_FIELD_GLYMS_NOMBRE_FANTASIA = "UF_CRM_1748637070"
B24_COMPANY_CUSTOM_FIELD_BLOQUEADO = "UF_CRM_1748888310536" 

# Mapeo de Glyms cli.id_cliente_tipo a Bitrix24 INDUSTRY_ID
GLYMS_IDCLIENTETIPO_TO_B24_INDUSTRY_ID = {
    7: "UC_FV5RTZ", 9: "OTHER", 10: "1", 11: "UC_5KOWIB", 12: "UC_6IPFTB",
    13: "UC_PJRWUH", 14: "UC_QMEHSY", 15: "UC_G2UX05", 16: "UC_8B7GEU",
    17: "UC_D0S7HJ", 18: "UC_FWPBRZ", 19: "UC_281LBU", 20: "UC_N97ZBK",
    21: "UC_SGFFD2", 22: "UC_Z8QQ65", 23: "UC_5HKBY1", 24: "UC_8DMZTD", 
    25: "UC_APU0VJ", 
    26: "3", 27:"4", 28: "5", 29: "6", 30: "7", 31: "8", 32: "9", 33: "10", 34: "11",
    35: "12", 36: "13", 37: "14", 38: "15", 39: "16", 40: "17"    
}

# Mapeo de Glyms cli.id_tipo_ot a Bitrix24 COMPANY_TYPE_ID
GLYMS_IDTIPOT_TO_B24_COMPANY_TYPE_ID = {
    1: "UC_HMSG7D",
    2: "4",
    3: "UC_EYGC4I",
}

# Variable global para el modo debug
DEBUG_MODE = False

# --- Funciones Auxiliares ---
def is_valid_email(email):
    if not email or not isinstance(email, str): return False
    regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(regex, email) is not None

def log_message(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def log_debug(message):
    """Imprime un mensaje de depuración solo si el modo debug está activo."""
    if DEBUG_MODE:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[DEBUG {timestamp}] {message}")

# --- Funciones de API de Bitrix24 y Tokens ---
def save_tokens(tokens):
    with open(B24_TOKEN_FILE, 'w') as f: json.dump(tokens, f)
    log_message(f"Tokens guardados en {B24_TOKEN_FILE}")

def load_tokens():
    if os.path.exists(B24_TOKEN_FILE):
        with open(B24_TOKEN_FILE, 'r') as f: return json.load(f)
    return None

def get_bitrix_access_token(auth_code=None):
    tokens = load_tokens()
    if tokens and 'access_token' in tokens and 'refresh_token' in tokens: return tokens
    if auth_code:
        log_message("Intentando obtener tokens con nuevo código de autorización...")
        token_url = f"{B24_PORTAL_URL}/oauth/token/"
        params = {'grant_type': 'authorization_code', 'client_id': B24_CLIENT_ID, 'client_secret': B24_CLIENT_SECRET, 'code': auth_code}
        try:
            response = requests.get(token_url, params=params, verify=False)
            response.raise_for_status(); new_tokens = response.json()
            if 'access_token' in new_tokens:
                save_tokens(new_tokens); log_message("Nuevos tokens obtenidos y guardados.")
                return new_tokens
            else: log_message(f"Error al obtener tokens con auth_code (JSON): {new_tokens}"); return None
        except requests.exceptions.RequestException as e:
            log_message(f"Excepción al obtener tokens con auth_code: {e}")
            if hasattr(e, 'response') and e.response is not None: log_message(f"Respuesta (error): {e.response.text}")
            return None
    else:
        log_message("No hay tokens guardados ni código de autorización.")
        auth_url = f"{B24_PORTAL_URL}/oauth/authorize/?response_type=code&client_id={B24_CLIENT_ID}&redirect_uri=http://localhost"
        log_message(f"1. Abre: {auth_url}\n2. Autoriza\n3. Copia 'code' de la URL\n4. Ejecuta: python {os.path.basename(__file__)} --auth_code=TU_CODIGO")
        return None

def refresh_bitrix_token(refresh_token):
    log_message("Intentando refrescar el token de acceso...")
    token_url = f"{B24_PORTAL_URL}/oauth/token/"
    params = {'grant_type': 'refresh_token', 'client_id': B24_CLIENT_ID, 'client_secret': B24_CLIENT_SECRET, 'refresh_token': refresh_token}
    try:
        response = requests.get(token_url, params=params, verify=False)
        response.raise_for_status(); new_tokens = response.json()
        if 'access_token' in new_tokens:
            save_tokens(new_tokens); log_message("Token refrescado y guardado.")
            return new_tokens
        else: log_message(f"Error al refrescar token: {new_tokens}"); return None
    except requests.exceptions.RequestException as e:
        log_message(f"Excepción al refrescar token: {e}")
        if hasattr(e, 'response') and e.response is not None: log_message(f"Respuesta servidor: {e.response.text}")
        return None

class TokenExpiredError(Exception): pass
class InvalidTokenError(Exception): pass
current_b24_tokens = None

def ensure_valid_token():
    global current_b24_tokens
    if not current_b24_tokens or not current_b24_tokens.get('access_token'):
        log_message("Tokens no en memoria, intentando cargar/obtener...")
        current_b24_tokens = get_bitrix_access_token()
        if not current_b24_tokens: raise Exception("CRÍTICO: No se pudieron obtener los tokens de Bitrix24.")
    return current_b24_tokens['access_token']

def call_bitrix_api(method, api_params, access_token_val):
    api_url = f"{B24_PORTAL_URL}/rest/{method}.json"
    headers = {'Accept': 'application/json'}
    payload_with_auth = api_params.copy()
    payload_with_auth['auth'] = access_token_val
    try:
        log_debug(f"API Call: {method} | Payload: {json.dumps(payload_with_auth, indent=2)}")
        response = requests.post(api_url, json=payload_with_auth, headers=headers, verify=False)
        response.raise_for_status()
        log_debug(f"API Response: {method} | {response.text[:1000]}...")
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            error_json = e.response.json()
            if error_json.get("error") == "expired_token": raise TokenExpiredError("Token expirado")
            elif error_json.get("error") == "invalid_token": raise InvalidTokenError("Token inválido")
        log_message(f"Error HTTP en API {method}: {e} | Respuesta: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        log_message(f"Error de red en API {method}: {e}"); return None

def call_bitrix_api_with_retry(method, params_for_api):
    global current_b24_tokens
    try:
        access_token = ensure_valid_token()
        return call_bitrix_api(method, params_for_api, access_token)
    except TokenExpiredError:
        log_message("Token expiró, refrescando y reintentando...")
        if not current_b24_tokens or not current_b24_tokens.get('refresh_token'):
            raise Exception("Fallo al refrescar: no hay refresh_token.")
        refreshed = refresh_bitrix_token(current_b24_tokens.get('refresh_token'))
        if refreshed and refreshed.get('access_token'):
            current_b24_tokens = refreshed
            return call_bitrix_api(method, params_for_api, current_b24_tokens['access_token'])
        else: raise Exception("Fallo al refrescar el token.")
    except InvalidTokenError:
        log_message("Token inválido. Borrando tokens. Se requerirá --auth_code en la próxima ejecución.")
        if os.path.exists(B24_TOKEN_FILE):
            try: os.remove(B24_TOKEN_FILE); log_message(f"{B24_TOKEN_FILE} eliminado.")
            except OSError as e_rm: log_message(f"Error al eliminar {B24_TOKEN_FILE}: {e_rm}")
        current_b24_tokens = None
        raise Exception("Token inválido. Re-autentica el script con --auth_code.")

# --- Funciones de PostgreSQL ---
def get_glyms_data(query, type_name="datos", params_tuple=None):
    conn = None; data_list = []
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()
        cur.execute(query, params_tuple if params_tuple else None)
        if cur.description:
            column_names = [desc[0] for desc in cur.description]
            for row in cur.fetchall(): data_list.append(dict(zip(column_names, row)))
        cur.close(); log_message(f"Obtenidos {len(data_list)} {type_name} de Glyms.")
    except psycopg2.Error as e:
        log_message(f"Error PostgreSQL para {type_name}: {e}"); return None
    finally:
        if conn: conn.close()
    return data_list

# --- Funciones de Directorio y Precios de Bitrix24 ---
def display_directory_items(entity_id_to_list):
    log_message(f"Solicitando ítems del directorio para ENTITY_ID: {entity_id_to_list}")
    api_params = {"order": {"SORT": "ASC"}, "filter": {"ENTITY_ID": entity_id_to_list}}
    log_message(f"Llamando a crm.status.list con filtro ENTITY_ID: {entity_id_to_list}")
    response_data = call_bitrix_api_with_retry("crm.status.list", api_params)
    if response_data and response_data.get("result"):
        items = response_data["result"]
        if isinstance(items, list) and items:
            log_message(f"Ítems para ENTITY_ID '{entity_id_to_list}':")
            for item in items:
                log_message(f"  ID: {str(item.get('ID','N/A')):<6} | STATUS_ID: {str(item.get('STATUS_ID','N/A')):<20} | NAME: {str(item.get('NAME','N/A')):<40}")
            log_message(f"Total: {len(items)}")
        else: log_message(f"No ítems para '{entity_id_to_list}'.")
    else: log_message(f"No se pudo obtener respuesta para '{entity_id_to_list}'.")

# Reemplaza tu función display_price_types con esta versión:
def display_price_types():
    log_message("Solicitando los Tipos de Precio del Catálogo...")
    api_params = {"order": {"id": "ASC"}, "select": ["id", "name", "xmlId", "base", "sort"]}
    log_message(f"Llamando a catalog.priceType.list...")
    response_data = call_bitrix_api_with_retry("catalog.priceType.list", api_params)

    # --- LÍNEA DE DEPURACIÓN AÑADIDA ---
    # Imprimimos la respuesta completa del servidor para ver exactamente qué recibimos.
    log_message(f"Respuesta COMPLETA del servidor para 'catalog.priceType.list': {json.dumps(response_data, indent=2)}")
    # --- FIN DE LA LÍNEA DE DEPURACIÓN ---

    if response_data and "result" in response_data:
        price_types = response_data["result"].get("priceTypes", [])
        if isinstance(price_types, list) and price_types:
            log_message("Tipos de Precio encontrados:")
            for pt in price_types:
                log_message(f"  ID: {str(pt.get('id','N/A')):<6} | NAME: {str(pt.get('name','N/A')):<30} | Es Base?: {'Sí' if pt.get('base') == 'Y' else 'No'}")
            log_message(f"Total de Tipos de Precio: {len(price_types)}")
        else:
            log_message("No se encontraron Tipos de Precio en la estructura esperada 'result.priceType'.")
    else:
        log_message(f"No se pudo obtener una respuesta válida o la clave 'result' no está presente.")

# ACTUALIZACION DE PRECIOS
def set_product_price(product_id, price_value, currency, price_type_id):
    """
    Establece o actualiza el precio de un producto para un tipo de precio específico.
    Primero busca un precio existente. Si lo encuentra, lo actualiza (update).
    Si no lo encuentra, lo crea (add).
    """
    if price_value is None:
        log_debug(f"No hay precio que establecer para el producto B24 ID {product_id}.")
        return

    try:
        price_float = float(price_value)
    except (ValueError, TypeError):
        log_message(f"ADVERTENCIA: Precio inválido '{price_value}' para producto B24 ID {product_id}. No se establecerá el precio.")
        return

    # 1. Buscar si ya existe un registro de precio para este producto y tipo de precio
    log_debug(f"Buscando precio existente para producto B24 ID {product_id} y PriceType ID {price_type_id}...")
    list_params = {
        'filter': {
            'productId': product_id,
            'catalogGroupId': price_type_id
        },
        'select': ['id'] # Solo necesitamos el ID del registro de precio
    }
    response_list = call_bitrix_api_with_retry("catalog.price.list", list_params)

    existing_price_record_id = None
    if response_list and response_list.get("result") and response_list["result"].get("prices"):
        # Asegurarse de que la respuesta no esté vacía
        prices = response_list["result"]["prices"]
        if prices and isinstance(prices, list) and len(prices) > 0:
            existing_price_record = prices[0]
            existing_price_record_id = existing_price_record.get("id")
            log_debug(f"Precio existente encontrado con ID de registro de precio: {existing_price_record_id}")

    # 2. Decidir si actualizar (update) o añadir (add)
    if existing_price_record_id:
        # --- Actualizar Precio Existente ---
        api_method = "catalog.price.update"
        api_params = {
            "id": existing_price_record_id, # ID del registro de precio, no del producto
            "fields": {
                "price": price_float,
                "currency": currency
            }
        }
        log_message(f"Actualizando precio para producto B24 ID {product_id} (Registro de Precio ID: {existing_price_record_id})...")
    else:
        # --- Añadir Nuevo Precio ---
        api_method = "catalog.price.add"
        api_params = {
            "fields": {
                "productId": product_id,
                "catalogGroupId": price_type_id,
                "price": price_float,
                "currency": currency
            }
        }
        log_message(f"Añadiendo nuevo precio para producto B24 ID {product_id} (Tipo Precio ID: {price_type_id})...")

    # 3. Ejecutar la llamada a la API
    response = call_bitrix_api_with_retry(api_method, api_params)
    
    if response and response.get("result"):
        log_message(f"Precio para producto {product_id} establecido/actualizado exitosamente.")
    else:
        log_message(f"Error al establecer/actualizar precio para producto {product_id}: {response}")
        
        
# --- Funciones de Sincronización ---
def sync_services():
    log_message("Iniciando proceso de sincronización de SERVICIOS.")
    #services_query = ("SELECT p.id_prestacion, p.codigo, p.descripcion, p.comentario, p.activa, p.id_tipo_ot, "
    #                  "(SELECT valor_unico FROM public.contrato_presta WHERE id_contrato = con.id_contrato AND p.codigo = codigo_presta "
    #                  "ORDER BY fecha_version DESC LIMIT 1) as precio "
    #                  f"FROM {PG_VIEW_NAME_SERVICES} p "
    #                  "LEFT JOIN public.contrato con ON (con.id_tipo_ot = p.id_tipo_ot AND contrato_base = true) "
    #                  "WHERE p.activa = 1")
    #AMR 14/07/2025: Cambio el query para que en vez de traer los precios del contrato base se puedan elegir el contrato para cada departamento
    services_query = ("SELECT p.id_prestacion, p.codigo, p.descripcion, p.comentario, p.activa, p.id_tipo_ot, "
                      "CASE WHEN id_tipo_ot = 1 "
                      f"     THEN (SELECT valor_unico FROM public.contrato_presta WHERE id_contrato = {PG_ID_CONTRATO_CLINICA} AND p.codigo = codigo_presta ORDER BY fecha_version DESC LIMIT 1) "
                      "     WHEN id_tipo_ot = 2 "
                      f"     THEN (SELECT valor_unico FROM public.contrato_presta WHERE id_contrato = {PG_ID_CONTRATO_INDUSTRIA} AND p.codigo = codigo_presta ORDER BY fecha_version DESC LIMIT 1) "
                      "     WHEN id_tipo_ot = 3 "
                      f"     THEN (SELECT valor_unico FROM public.contrato_presta WHERE id_contrato = {PG_ID_CONTRATO_VETERINARIA} AND p.codigo = codigo_presta ORDER BY fecha_version DESC LIMIT 1) "
                      "END as precio "
                      f"FROM {PG_VIEW_NAME_SERVICES} p "
                      "WHERE p.activa = 1")
    glyms_services_list = get_glyms_data(services_query, "servicios")
    if glyms_services_list is None: log_message("Fallo al obtener servicios de Glyms."); return
    if not glyms_services_list: log_message("No hay servicios activos en Glyms.");
    glyms_services_map = {str(s["id_prestacion"]): s for s in glyms_services_list}
    log_message(f"Mapeados {len(glyms_services_map)} servicios de Glyms.")

    b24_managed_services = {}
    start = 0; batch_size = 50
    while True:
        select_fields_svc = ["id", "xmlId", "active", "name", "detailText", "code", "iblockId"]
        api_params_svc_list = {'order': {"id": "ASC"}, 'filter': {"iblockId": B24_IBLOCK_ID_SERVICES, "!xmlId": ""},
                               'select': select_fields_svc, 'start': start}
        log_message(f"Listando servicios B24 (lote desde {start})")
        response_data_svc = call_bitrix_api_with_retry("catalog.product.service.list", api_params_svc_list)
        if response_data_svc and "result" in response_data_svc:
            products_batch_svc = response_data_svc.get("result", {}).get("services", [])
            if not isinstance(products_batch_svc, list): products_batch_svc = []
            for product_svc in products_batch_svc:
                xml_id_val_svc = product_svc.get("xmlId")
                if xml_id_val_svc: b24_managed_services[str(xml_id_val_svc)] = product_svc
            if "next" not in response_data_svc or not products_batch_svc or len(products_batch_svc) < batch_size: break
            start = response_data_svc.get("next", start + len(products_batch_svc)); time.sleep(0.3)
        else: log_message(f"Error al obtener servicios de B24. Respuesta: {response_data_svc}"); break
    log_message(f"Obtenidos {len(b24_managed_services)} servicios gestionados de B24.")
    
    for current_xml_id_svc, glyms_service_data in glyms_services_map.items():
        fields_to_send = {
            "name": glyms_service_data.get("descripcion"),
            "active": 'Y' if glyms_service_data.get("activa") == 1 else 'N',
            "detailText": glyms_service_data.get("comentario", ""),
            "code": glyms_service_data.get("codigo"),
        }
        glyms_price = glyms_service_data.get("precio")
        if current_xml_id_svc in b24_managed_services:
            b24_service = b24_managed_services[current_xml_id_svc]
            b24_service_id = b24_service["id"]
            needs_update = any([(b24_service.get(k) or "") != (v or "") for k, v in fields_to_send.items()])
            if needs_update:
                update_api_params = {"id": b24_service_id, "fields": fields_to_send}
                log_message(f"Actualizando datos de servicio B24 ID {b24_service_id}...")
                response_update = call_bitrix_api_with_retry("catalog.product.service.update", update_api_params)
                if not (response_update and response_update.get("result")):
                     log_message(f"Error al actualizar servicio {current_xml_id_svc}: {response_update}"); continue
            else: log_message(f"Servicio {current_xml_id_svc} no requiere actualización.")
            set_product_price(b24_service_id, glyms_price, B24_CURRENCY_ID_SERVICES, B24_PRICE_TYPE_ID_SERVICES)
        else:
            log_message(f"Creando servicio en B24 para Glyms id_prestacion {current_xml_id_svc}")
            create_fields = {"iblockId": B24_IBLOCK_ID_SERVICES, "xmlId": current_xml_id_svc, **fields_to_send}
            glyms_id_tipo_ot_raw = glyms_service_data.get("id_tipo_ot")
            if glyms_id_tipo_ot_raw is not None:
                try:
                    glyms_id_tipo_ot = int(str(glyms_id_tipo_ot_raw))
                    if glyms_id_tipo_ot in B24_SECTION_MAPPING_SERVICES:
                        create_fields["iblockSectionId"] = B24_SECTION_MAPPING_SERVICES[glyms_id_tipo_ot]
                except ValueError: log_message(f"ADVERTENCIA (Servicios): id_tipo_ot '{glyms_id_tipo_ot_raw}' inválido.")
            
            create_api_params = {"fields": create_fields}
            response_add = call_bitrix_api_with_retry("catalog.product.service.add", create_api_params)
            if response_add and response_add.get("result"):
                new_id_data = response_add["result"]
                new_b24_id = new_id_data.get("element", {}).get("id") or new_id_data.get("id") or (int(str(new_id_data)) if str(new_id_data).isdigit() else None)
                if new_b24_id:
                    log_message(f"Servicio {current_xml_id_svc} CREADO con ID B24 {new_b24_id}.")
                    set_product_price(new_b24_id, glyms_price, B24_CURRENCY_ID_SERVICES, B24_PRICE_TYPE_ID_SERVICES)
                else: log_message(f"Servicio {current_xml_id_svc} creado, ID no encontrado. Respuesta: {response_add}")
            else: log_message(f"Error al crear servicio {current_xml_id_svc}: {response_add}")
        time.sleep(0.3)

    log_message("Verificando servicios a desactivar en B24...")
    for b24_xml_id, b24_data in b24_managed_services.items():
        if b24_xml_id not in glyms_services_map and b24_data.get("active") == 'Y':
            log_message(f"Desactivando servicio B24 ID {b24_data.get('id')} (XML_ID {b24_xml_id})")
            call_bitrix_api_with_retry("catalog.product.service.update", {"id": b24_data.get('id'), "fields": {"active": "N"}})
            time.sleep(0.3)
    log_message("Sincronización de SERVICIOS completada.")

def update_company_sync_log_status(log_id, status, error_msg=None):
    log_debug(f"Actualizando DB LOG para log_id: {log_id} a estado: '{status}'")
    sql_update = f"UPDATE {PG_LOG_TABLE_COMPANIES} SET sync_status = %s, processed_timestamp = CURRENT_TIMESTAMP"
    params_list = [status]
    if error_msg: sql_update += ", error_message = %s"; params_list.append(str(error_msg))
    elif status in ["PROCESSED", "TARGET_MISSING"]: sql_update += ", error_message = NULL"
    sql_update += " WHERE log_id = %s;"; params_list.append(log_id)
    execute_glyms_command(sql_update, params_tuple=tuple(params_list), type_name="actualización de log") 

def prepare_b24_company_fields(glyms_company_data, origin_id):
    if glyms_company_data is None: return {"ORIGIN_ID": str(origin_id)}
    fields = {"TITLE": glyms_company_data.get("razon_social"), "ORIGIN_ID": str(origin_id), "ADDRESS": glyms_company_data.get("direccion"),
              "COMMENTS": glyms_company_data.get("observaciones"), B24_COMPANY_CUSTOM_FIELD_GLYMS_CODIGO: glyms_company_data.get("codigo"),
              B24_COMPANY_CUSTOM_FIELD_GLYMS_NOMBRE_FANTASIA: glyms_company_data.get("nombre_fantasia")}
    glyms_email_str = glyms_company_data.get("mail")
    if glyms_email_str and isinstance(glyms_email_str, str):
        glyms_email_str = glyms_email_str.strip()
        if glyms_email_str and is_valid_email(glyms_email_str): fields["EMAIL"] = [{"VALUE": glyms_email_str, "VALUE_TYPE": "WORK"}]
        elif glyms_email_str: log_message(f"ADVERTENCIA: Email inválido '{glyms_email_str}' para ORIGIN_ID {origin_id}.")
    
    glyms_phone_str = glyms_company_data.get("telefono")
    if glyms_phone_str and isinstance(glyms_phone_str, str):
        glyms_phone_str = glyms_phone_str.strip()
        if glyms_phone_str: fields["PHONE"] = [{"VALUE": glyms_phone_str, "VALUE_TYPE": "WORK"}]
    
    glyms_comp_type_key = glyms_company_data.get("id_tipo_ot")
    if glyms_comp_type_key is not None:
        try:
            b24_id = GLYMS_IDTIPOT_TO_B24_COMPANY_TYPE_ID.get(int(glyms_comp_type_key))
            if b24_id: fields["COMPANY_TYPE"] = b24_id
            else: log_message(f"ADVERTENCIA: No mapeo COMPANY_TYPE para Glyms id_tipo_ot '{glyms_comp_type_key}'.")
        except (ValueError, TypeError): log_message(f"ADVERTENCIA: id_tipo_ot '{glyms_comp_type_key}' inválido.")

    glyms_industry_key = glyms_company_data.get("id_cliente_tipo")
    if glyms_industry_key is not None:
        try:
            b24_id = GLYMS_IDCLIENTETIPO_TO_B24_INDUSTRY_ID.get(int(glyms_industry_key))
            if b24_id: fields["INDUSTRY"] = b24_id
            else: log_message(f"ADVERTENCIA: No mapeo INDUSTRY para Glyms id_cliente_tipo '{glyms_industry_key}'.")
        except (ValueError, TypeError): log_message(f"ADVERTENCIA: id_cliente_tipo '{glyms_industry_key}' inválido.")
    
    observaciones_glyms = glyms_company_data.get("observaciones", "") or ""
    fields[B24_COMPANY_CUSTOM_FIELD_BLOQUEADO] = "1" if "bloqueado" in observaciones_glyms.lower() else "0"
    return {k: v for k, v in fields.items() if v is not None}

def sync_companies():
    log_message("Iniciando proceso de sincronización de EMPRESAS (basado en log de cambios).")
    log_query = f"SELECT log_id, id_cliente, operation_type FROM {PG_LOG_TABLE_COMPANIES} WHERE sync_status = 'PENDING' ORDER BY change_timestamp ASC, log_id ASC LIMIT 200;"
    pending_changes = get_glyms_data(log_query, "cambios pendientes de empresas")

    if pending_changes is None: log_message("Fallo al obtener cambios pendientes de empresas."); return
    if not pending_changes: log_message("No hay cambios pendientes de empresas para procesar."); return
    log_message(f"Procesando {len(pending_changes)} cambios de empresas desde la tabla de log.")

    for change_entry in pending_changes:
        log_id, id_cliente_glyms, operation = change_entry.get("log_id"), change_entry.get("id_cliente"), change_entry.get("operation_type")
        current_origin_id = str(id_cliente_glyms)
        log_message(f"--- PROCESANDO LOG ID: {log_id} | ID_CLIENTE: {id_cliente_glyms} | OPERACIÓN: {operation} ---")

        glyms_company_data = None
        if operation in ['INSERT', 'UPDATE']:
            company_detail_query = (f"SELECT cli.id_cliente, cli.codigo, cli.razon_social, cli.nombre_fantasia, "
                                    f"cli.direccion, cli.telefono, cli.mail, cli.observaciones, cli.id_tipo_ot, cli.id_cliente_tipo "
                                    f"FROM {PG_TABLE_NAME_COMPANIES_MAIN} cli WHERE cli.id_cliente = %s;")
            company_details_list = get_glyms_data(company_detail_query, f"detalle empresa {id_cliente_glyms}", params_tuple=(id_cliente_glyms,))
            if not company_details_list:
                err_msg = f"Detalles no encontrados en {PG_TABLE_NAME_COMPANIES_MAIN} para id_cliente {id_cliente_glyms} (op {operation})."
                log_message(f"ERROR: {err_msg}"); update_company_sync_log_status(log_id, "ERROR", err_msg); continue
            glyms_company_data = company_details_list[0]
        
        b24_company_id_found, b24_existing_company_data_map = None, None
        select_fields_comp = ["ID", "TITLE", "EMAIL", "PHONE", "ADDRESS", "COMMENTS", "INDUSTRY", "COMPANY_TYPE", 
                              "ORIGIN_ID", B24_COMPANY_CUSTOM_FIELD_GLYMS_CODIGO, 
                              B24_COMPANY_CUSTOM_FIELD_GLYMS_NOMBRE_FANTASIA, B24_COMPANY_CUSTOM_FIELD_BLOQUEADO]
        company_list_api_params = {'filter': {"ORIGIN_ID": current_origin_id, "CHECK_PERMISSIONS": "N"}, 'select': select_fields_comp}
        response_list = call_bitrix_api_with_retry("crm.company.list", company_list_api_params)
        if response_list and response_list.get("result") and len(response_list["result"]) > 0:
            b24_existing_company_data_map = response_list["result"][0]
            b24_company_id_found = b24_existing_company_data_map.get("ID")
            log_message(f"Empresa encontrada en B24 ID: {b24_company_id_found} para ORIGIN_ID: {current_origin_id}")
        else:
            log_message(f"Empresa con ORIGIN_ID: {current_origin_id} no encontrada en Bitrix24.")

        processed_successfully, api_error_message = False, None
        
        if operation == 'INSERT' and not b24_company_id_found:
            log_message(f"Creando nueva empresa en B24 para Glyms id_cliente {id_cliente_glyms}...")
            fields_for_add = prepare_b24_company_fields(glyms_company_data, current_origin_id)
            if not fields_for_add.get("TITLE"):
                api_error_message = f"Título (razon_social) vacío para crear empresa {id_cliente_glyms}."
            else:
                response_add = call_bitrix_api_with_retry("crm.company.add", {"fields": fields_for_add})
                if response_add and response_add.get("result") and int(str(response_add.get("result",0))) > 0:
                    log_message(f"Empresa {id_cliente_glyms} CREADA con ID B24 {response_add.get('result')}."); processed_successfully = True
                else: api_error_message = f"Error al crear empresa {id_cliente_glyms}: {response_add}"
        elif operation == 'UPDATE' or (operation == 'INSERT' and b24_company_id_found):
            if not b24_company_id_found:
                log_message(f"INFO: Evento UPDATE para Glyms id {id_cliente_glyms}, no en B24. Marcando TARGET_MISSING."); update_company_sync_log_status(log_id, "TARGET_MISSING", "No encontrada en B24 para UPDATE."); time.sleep(0.3); continue
            else:
                log_debug(f"--- COMPARANDO DATOS PARA UPDATE: B24 ID {b24_company_id_found} ---")
                fields_for_update = prepare_b24_company_fields(glyms_company_data, current_origin_id)
                log_debug(f"  > Datos de Bitrix24 (actuales): {json.dumps(b24_existing_company_data_map, indent=2, ensure_ascii=False)}")
                log_debug(f"  > Datos de Glyms (a sincronizar): {json.dumps(fields_for_update, indent=2, ensure_ascii=False)}")
                # Comprobar si hay cambios
                # Esta parte de la comparación puede ser compleja y depende de los detalles de la respuesta de la API
                # Se simplifica a una actualización incondicional para robustez, asumiendo que el trigger de UPDATE
                # en Glyms ya implica un cambio que vale la pena sincronizar.
                # Si se desea optimizar, se debe implementar una comparación campo a campo aquí.
                log_message(f"Actualizando empresa en B24 ID {b24_company_id_found}...")
                response_update = call_bitrix_api_with_retry("crm.company.update", {"ID": b24_company_id_found, "fields": fields_for_update})
                if response_update and response_update.get("result") == True:
                    log_message(f"Empresa B24 ID {b24_company_id_found} actualizada."); processed_successfully = True
                else: api_error_message = f"Error al actualizar B24 ID {b24_company_id_found}: {response_update}"
        elif operation == 'DELETE':
            if not b24_company_id_found:
                log_message(f"INFO: Evento DELETE para Glyms id {id_cliente_glyms}, no en B24. Marcando TARGET_MISSING."); update_company_sync_log_status(log_id, "TARGET_MISSING", "No encontrada en B24 para DELETE."); time.sleep(0.3); continue
            else:
                log_message(f"Procesando DELETE para B24 ID {b24_company_id_found}...")
                comments_to_check = b24_existing_company_data_map.get("COMMENTS", "") or ""
                delete_note_prefix = "[SISTEMA] Empresa eliminada o inactiva en Glyms el "
                if delete_note_prefix not in comments_to_check:
                    delete_note = f"\n{delete_note_prefix}{datetime.date.today().strftime('%Y-%m-%d')}."
                    new_comments = (comments_to_check.strip() + delete_note).strip()
                    response_delete = call_bitrix_api_with_retry("crm.company.update", {"ID": b24_company_id_found, "fields": {"COMMENTS": new_comments}})
                    if response_delete and response_delete.get("result") == True:
                        log_message(f"Comentarios actualizados para B24 ID {b24_company_id_found}."); processed_successfully = True
                    else: api_error_message = f"Error al act. comentarios por DELETE: {response_delete}"
                else:
                    log_message(f"Empresa B24 ID {b24_company_id_found} ya tiene nota de eliminación."); processed_successfully = True

        if processed_successfully: update_company_sync_log_status(log_id, "PROCESSED")
        elif api_error_message: log_message(str(api_error_message)); update_company_sync_log_status(log_id, "ERROR", str(api_error_message))
        
        time.sleep(0.3)
    log_message("Proceso de sincronización de EMPRESAS (basado en log) completado para este lote.")

# Coloca esta nueva función debajo de get_glyms_data
def execute_glyms_command(sql_command, params_tuple=None, type_name="comando"):
    """
    Ejecuta un comando SQL (como UPDATE, INSERT, DELETE) y guarda los cambios.
    No devuelve filas de datos.
    """
    conn = None
    success = False
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()
        cur.execute(sql_command, params_tuple if params_tuple else None)
        
        # --- LA LÍNEA MÁGICA ---
        # Confirma la transacción para guardar los cambios permanentemente en la base de datos.
        conn.commit()
        # ---------------------

        cur.close()
        log_debug(f"Comando '{type_name}' ejecutado exitosamente.")
        success = True
    except psycopg2.Error as e:
        log_message(f"Error PostgreSQL al ejecutar {type_name}: {e}")
        if conn:
            conn.rollback() # Revierte los cambios si hubo un error
    finally:
        if conn:
            conn.close()
    return success
    
    
# --- INICIO DEL SCRIPT ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sincroniza datos de Glyms a Bitrix24 y utilidades de directorio.")
    parser.add_argument('--auth_code', type=str, help="Código de autorización de Bitrix24 para la obtención inicial de tokens.")
    parser.add_argument('--sync_type', type=str, choices=['services', 'companies', 'all'], default='all', help="Tipo de sincronización a ejecutar.")
    parser.add_argument('--directory_items', type=str, metavar='ENTITY_ID', help="Muestra ítems del directorio para el ENTITY_ID (ej: STATUS, COMPANY_TYPE, INDUSTRY) y termina.")
    parser.add_argument('--price_types', action='store_true', help="Muestra los Tipos de Precio del catálogo y termina.")
    parser.add_argument('--debug', action='store_true', help="Activa los mensajes de depuración detallados.")
    args = parser.parse_args()

    if args.debug:
        DEBUG_MODE = True
        log_message("MODO DEBUG ACTIVADO.")

    if not args.auth_code and not args.directory_items and not args.price_types:
        current_b24_tokens = load_tokens()

    if args.auth_code:
        log_message(f"Intentando obtener tokens con auth_code...")
        current_b24_tokens = get_bitrix_access_token(auth_code=args.auth_code)
        if not current_b24_tokens: log_message("No se pudieron obtener tokens. Saliendo."); exit()
        else: log_message("Tokens obtenidos con auth_code. Para sincronizar o listar, ejecuta el script de nuevo sin --auth_code."); exit()
    
    try:
        if args.directory_items:
            ensure_valid_token(); display_directory_items(args.directory_items.upper())
        elif args.price_types:
            ensure_valid_token(); display_price_types()
        else:
            ensure_valid_token()
            if args.sync_type in ['all', 'services']: sync_services()
            if args.sync_type == 'all': log_message("Pausa de 1 segundo..."); time.sleep(1)
            if args.sync_type in ['all', 'companies']: sync_companies()
    except Exception as e:
        log_message(f"CRÍTICO: Excepción no controlada en ejecución principal: {e}")
        import traceback
        log_message(traceback.format_exc())