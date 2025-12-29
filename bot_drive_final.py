import pandas as pd
import requests
import time
import os
import json
from datetime import datetime, timedelta
import urllib3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import pytz # <--- NUEVA LIBRERÍA NECESARIA

# Desactivar advertencias SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= CONFIGURACIÓN SEGURA =================
# AHORA TOMAMOS TODO DE LAS VARIABLES DE ENTORNO DE GITHUB
TICKET_API = os.environ["TICKET_API"] 
SHEET_ID = os.environ["SHEET_ID"]
CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS"] 

DIAS_ATRAS_NUEVAS = 3
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
TIEMPO_ESPERA_DINAMICO = 0.2
# =================================================

def conectar_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Cargamos el JSON desde la variable de entorno
    creds_dict = json.loads(CREDENTIALS_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).get_worksheet(0)
    return sheet

def dates_iso(fecha):
    if not fecha: return ""
    return str(fecha).replace("T", " ")

def limpiar_fecha_para_filtro(fecha_str):
    if pd.isna(fecha_str) or fecha_str == "": return None
    str_f = str(fecha_str).strip()
    formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"]
    for fmt in formatos:
        try:
            return datetime.strptime(str_f[:19], fmt)
        except:
            continue
    return None

def obtener_detalle_api_smart(codigo):
    global TIEMPO_ESPERA_DINAMICO
    codigo_limpio = str(codigo).strip().upper()
    url = f"https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?codigo={codigo_limpio}&ticket={TICKET_API}"
    
    intentos = 0
    tiempos_castigo = [2, 5, 10] 
    
    while intentos < 3:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20, verify=False)
            if r.status_code == 200:
                TIEMPO_ESPERA_DINAMICO = max(0.2, TIEMPO_ESPERA_DINAMICO * 0.95)
                data = r.json()
                if 'Listado' in data and len(data['Listado']) > 0:
                    item = data['Listado'][0]
                    comprador = item.get('Comprador', {})
                    fechas = item.get('Fechas', {})
                    
                    items_raw = item.get('Items', {}).get('Listado', [])
                    lista_prod = []
                    rubro = "General"
                    for i, p in enumerate(items_raw):
                        nom = p.get('NombreProducto', '') or p.get('Descripcion', '')
                        cant = p.get('Cantidad', 0)
                        lista_prod.append(f"({cant}) {nom}")
                        if i == 0: rubro = p.get('Rubro', 'General')
                    
                    glosa_prod = " || ".join(lista_prod[:15]) or "Ver en Web"
                    link_nuevo = f"http://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?idlicitacion={codigo_limpio}"

                    return {
                        'Link': link_nuevo,
                        'Número': item['CodigoExterno'],
                        'Estado': 'Publicada',
                        'Rubro': rubro,
                        'Nombre de la Licitación': item['Nombre'],
                        'Productos': glosa_prod,
                        'Monto Estimado': item.get('MontoEstimado', 0),
                        'Moneda': item.get('Moneda', ''),
                        'Fecha Cierre': dates_iso(fechas.get('FechaCierre', '')),
                        'Comprador': comprador.get('NombreOrganismo', 'N/A'),
                        'Region': comprador.get('RegionUnidad', ''),
                        'Contacto Email': comprador.get('MailUsuario', ''),
                        'Descripcion': item.get('Descripcion', 'Sin descripción'),
                        'Fecha Publicacion': dates_iso(fechas.get('FechaPublicacion', ''))
                    }
                else:
                    return "VACIO"
            elif r.status_code == 429:
                time.sleep(tiempos_castigo[intentos])
                TIEMPO_ESPERA_DINAMICO = min(3.0, TIEMPO_ESPERA_DINAMICO + 0.5)
                intentos += 1
            else:
                time.sleep(2)
                intentos += 1
        except:
            time.sleep(1)
            intentos += 1
    return None

def main_nube():
    print("☁️ BOT INICIADO EN GITHUB ACTIONS")
    
    try:
        sheet = conectar_sheet()
        data_raw = sheet.get_all_records()
        df_actual = pd.DataFrame(data_raw)
        print(f"✅ Conectado. Registros actuales: {len(df_actual)}")
    except Exception as e:
        print(f"❌ Error Crítico conectando a Drive: {e}")
        # Si falla la conexión inicial, matamos el proceso para que GitHub avise del error
        exit(1) 

    if df_actual.empty:
        df_actual = pd.DataFrame(columns=['Número', 'Productos'])

    # FASE 1: REPARACIÓN
    if 'Productos' not in df_actual.columns: df_actual['Productos'] = ""
    df_actual.replace("", float("NaN"), inplace=True)
    mask_incompleto = df_actual['Productos'].isna()
    indices_a_reparar = df_actual[mask_incompleto].index.tolist()
    
    if indices_a_reparar:
        print(f"🔧 Reparando {len(indices_a_reparar)} filas...")
        for idx in indices_a_reparar:
            cod = df_actual.at[idx, 'Número']
            res = obtener_detalle_api_smart(cod)
            if isinstance(res, dict):
                for k, v in res.items():
                    if k in df_actual.columns: df_actual.at[idx, k] = v
                print("✅", end="", flush=True)
            time.sleep(TIEMPO_ESPERA_DINAMICO)
        print("\n")

    # FASE 2: NOVEDADES
    ids_existentes = set(df_actual['Número'].astype(str).str.strip())
    nuevas_filas = []
    
    # Usamos pytz para la fecha de hoy también
    tz_chile = pytz.timezone('America/Santiago')
    fecha_hoy = datetime.now(tz_chile)
    
    print(f"📡 Buscando novedades...")
    for i in range(DIAS_ATRAS_NUEVAS):
        str_fecha = (fecha_hoy - timedelta(days=i)).strftime("%d%m%Y")
        try:
            url = f"https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?fecha={str_fecha}&ticket={TICKET_API}"
            r = requests.get(url, headers=HEADERS, timeout=20, verify=False)
            if r.status_code == 200:
                lista = r.json().get('Listado', [])
                candidatos = [l for l in lista if l['CodigoEstado'] == 5 and str(l['CodigoExterno']) not in ids_existentes]
                if candidatos:
                    print(f"   📅 {str_fecha}: {len(candidatos)} nuevas.")
                    for cand in candidatos:
                        cod = cand['CodigoExterno']
                        res = obtener_detalle_api_smart(cod)
                        if isinstance(res, dict):
                            nuevas_filas.append(res)
                            ids_existentes.add(str(cod))
                        time.sleep(TIEMPO_ESPERA_DINAMICO)
        except: pass

    # INTEGRACIÓN
    if nuevas_filas:
        df_nuevos = pd.DataFrame(nuevas_filas)
        df_actual = pd.concat([df_actual, df_nuevos], ignore_index=True)
        print(f"📥 +{len(df_nuevos)} nuevos.")

    if not df_actual.empty:
        df_actual.drop_duplicates(subset=['Número'], keep='last', inplace=True)
        
        # ==============================================================================
        # 🔥 LIMPIEZA DE PRECISIÓN (TIMEZONE AWARE)
        # ==============================================================================
        print("🧹 Analizando fechas de cierre...")
        
        # 1. Obtenemos hora Chile exacta
        ahora_chile = datetime.now(tz_chile)
        # Le quitamos la info de zona horaria para poder comparar con el Excel (que no tiene zona)
        ahora_chile_naive = ahora_chile.replace(tzinfo=None)
        
        print(f"   🕒 Hora de referencia (Chile): {ahora_chile.strftime('%d-%m-%Y %H:%M')}")
        
        df_actual['temp_fecha_cierre'] = df_actual['Fecha Cierre'].apply(limpiar_fecha_para_filtro)
        
        # 2. FILTRO
        # Comparamos "naive" con "naive" para evitar errores de pandas
        mask_vivas = (df_actual['temp_fecha_cierre'] > ahora_chile_naive) | (df_actual['temp_fecha_cierre'].isna())
        
        conteo_antes = len(df_actual)
        df_actual = df_actual[mask_vivas].copy()
        borradas = conteo_antes - len(df_actual)
        
        if borradas > 0:
            print(f"   🗑️ Se eliminaron {borradas} licitaciones expiradas.")
        else:
            print("   ✨ Todo vigente.")
            
        df_actual.drop(columns=['temp_fecha_cierre'], inplace=True)
        # ==============================================================================

        df_actual.fillna("", inplace=True)
        
        cols_orden = [
            'Link', 'Número', 'Estado', 'Rubro', 'Nombre de la Licitación', 
            'Productos', 'Monto Estimado', 'Moneda', 'Fecha Cierre', 
            'Comprador', 'Region', 'Contacto Email', 'Descripcion', 
            'Fecha Publicacion', 'Enlace', 'Prioridad'
        ]
        
        for col in cols_orden:
            if col not in df_actual.columns: df_actual[col] = ""
            
        df_final = df_actual[cols_orden]

        print("☁️ Actualizando Google Sheets...")
        try:
            sheet.clear()
            set_with_dataframe(sheet, df_final)
            print(f"✅ EXITO: Base actualizada.")
        except Exception as e:
            print(f"❌ Error subiendo: {e}")
            exit(1) # Forzamos error en GitHub para recibir alerta

if __name__ == "__main__":
    main_nube()
