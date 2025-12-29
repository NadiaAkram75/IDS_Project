import streamlit as st
import pandas as pd
from minio import Minio
import io
import time

st.set_page_config(page_title="IDS Blocker Status", layout="wide")
st.title("🛡️ Network Intrusion Detection System")

# MinIO Configuration
MINIO_URL = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "ids-bucket"

def get_live_data():
    stats = {"tried": 0, "success": 0, "empty": 0, "errors": 0, "concat_rows": 0, "final_rows": 0}
    try:
        client = Minio(MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
        # Récupérer tous les fichiers parquet (sans tri pour éviter les problèmes)
        objects = list(client.list_objects(BUCKET_NAME, prefix="predictions/", recursive=True))
        
        # Filtrer uniquement les fichiers parquet et prendre les 30 premiers
        parquet_objects = [o for o in objects if o.object_name.endswith(".parquet")][:30]
        
        if not parquet_objects:
            return None, stats
        
        df_list = []
        for obj in parquet_objects:
            stats["tried"] += 1
            try:
                response = client.get_object(BUCKET_NAME, obj.object_name)
                with response:
                    data = response.read()
                    if data:
                        pdf = pd.read_parquet(io.BytesIO(data))
                        if not pdf.empty:
                            df_list.append(pdf)
                            stats["success"] += 1
                        else:
                            stats["empty"] += 1
                    else:
                        stats["empty"] += 1
            except Exception as e:
                stats["errors"] += 1
                continue
        
        if df_list:
            try:
                result = pd.concat(df_list, ignore_index=True)
                stats["concat_rows"] = len(result)
                # Essayer de supprimer les doublons, mais si ça échoue (dict non hashable), garder le DataFrame
                try:
                    result = result.drop_duplicates()
                    stats["final_rows"] = len(result)
                except (TypeError, ValueError) as e:
                    # Si drop_duplicates échoue (ex: colonnes avec dicts), garder le DataFrame original
                    stats["drop_dup_error"] = str(e)[:50]
                    stats["final_rows"] = len(result)
                return result if not result.empty else None, stats
            except Exception as e:
                stats["concat_error"] = str(e)[:50]
                return None, stats
        return None, stats
    except Exception as e:
        stats["errors"] += 1
        return None, stats

# --- UI LOGIC ---
try:
    df, stats = get_live_data()
except Exception:
    df, stats = None, {"tried": 0, "success": 0, "empty": 0, "errors": 0, "concat_rows": 0, "final_rows": 0}

# DEBUG: Show connection status in sidebar
with st.sidebar:
    st.subheader("🔧 Debug Info")
    try:
        client = Minio(MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
        objects = list(client.list_objects(BUCKET_NAME, prefix="predictions/", recursive=True))
        st.success(f"✅ MinIO connecté")
        st.write(f"Fichiers trouvés: {len(objects)}")
        parquet_files = [o.object_name for o in objects if o.object_name.endswith(".parquet")]
        st.write(f"Fichiers parquet: {len(parquet_files)}")
        if parquet_files:
            st.write("Derniers fichiers:")
            for f in parquet_files[:5]:
                st.code(f, language=None)
        
        # Afficher les statistiques de lecture
        st.write("---")
        st.write("**Statistiques de lecture:**")
        if stats:
            st.write(f"Tentatives: {stats.get('tried', 0)}")
            st.write(f"Succès: {stats.get('success', 0)}")
            st.write(f"Vides/Erreurs: {stats.get('empty', 0) + stats.get('errors', 0)}")
            if 'concat_rows' in stats:
                st.write(f"Lignes après concat: {stats.get('concat_rows', 0)}")
                st.write(f"Lignes finales: {stats.get('final_rows', 0)}")
                if 'drop_dup_error' in stats:
                    st.warning(f"⚠️ Déduplication ignorée: {stats.get('drop_dup_error')}")
            if 'concat_error' in stats:
                st.error(f"Erreur concat: {stats.get('concat_error')}")
        if df is None:
            st.warning("⚠️ Aucune donnée chargée")
            if stats and stats.get('success', 0) > 0:
                st.info("💡 Fichiers lus mais DataFrame vide - vérifier les colonnes")
        elif hasattr(df, '__len__'):
            st.success(f"✅ {len(df)} lignes chargées")
    except Exception as e:
        st.error(f"❌ Erreur MinIO: {e}")

if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
    # Filter for attacks only
    attacks_df = df[df['prediction'] == 1.0]
    
    # 1. Main Status Indicator
    if not attacks_df.empty:
        st.error(f"🚨 ATTACK DETECTED: {len(attacks_df)} malicous packets blocked.")
    else:
        st.success("✅ SYSTEM SECURE: Monitoring live traffic...")

    # 2. Top-level Counters
    col1, col2 = st.columns(2)
    col1.metric("Traffic Scanned", len(df))
    col2.metric("Blocked Threats", len(attacks_df), delta_color="inverse")

    # 3. Detailed Incident Log
    st.subheader("🚧 Real-Time Blocked Traffic Log")
    if not attacks_df.empty:
        # Displaying only relevant columns for proof of detection
        display_df = attacks_df[['protocol_type', 'src_bytes', 'dst_bytes', 'prediction']]
        display_df.columns = ['Protocol', 'Source Bytes', 'Dest Bytes', 'Status (1=Blocked)']
        st.dataframe(display_df.tail(10), use_container_width=True)
    else:
        st.info("No malicious packets detected in current stream.")

else:
    st.info("🔎 Connecting to Spark Security Engine...")

# Rapid refresh for live presentation
time.sleep(3)
st.rerun()