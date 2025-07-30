import streamlit as st
from validator import run_validation
import tempfile
import os

st.set_page_config(page_title="🧪 Data Validator", layout="centered")
st.title("📊 Validator ")

# File upload
uploaded_file1 = st.file_uploader("📁 Upload Current Dataset (CSV or TXT)", type=["csv", "txt"], key="file1")
uploaded_file2 = st.file_uploader("📁 Upload Previous Dataset (CSV or TXT)", type=["csv", "txt"], key="file2")

# Country selection
country = st.selectbox("🌍 Select Country", ["USA", "India", "Germany", "UK", "CA", "PH", "AUS", "FRA"])

#--File Type selection--
ftype = st.selectbox("🌍 Select File Type", ["FRG", "RMS", "TRICITY", "POLLO_CAMPERO", "TMAD", "Caseys"])


# UI note
st.caption("Supports large datasets (e.g. 1 million rows × 20 columns).")

# Run validation
if uploaded_file1 and uploaded_file2 and st.button("✅ Run Validation"):
    temp1_path, temp2_path = None, None
    try:
        # Save uploaded files to temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix='.' + uploaded_file1.name.split('.')[-1]) as temp1, \
             tempfile.NamedTemporaryFile(delete=False, suffix='.' + uploaded_file2.name.split('.')[-1]) as temp2:

            temp1.write(uploaded_file1.read())
            temp2.write(uploaded_file2.read())
            temp1_path = temp1.name
            temp2_path = temp2.name

        st.info("🚀 Running validation... Please wait.")

        output_path="output_summary.txt"

        # Run validation and get log path
        log_file_path = run_validation(temp1_path, temp2_path, country, output_path, ftype)

        st.success("✅ Validation completed!")

        try:
            with open(log_file_path, "r", encoding="utf-8") as log_file:
                log_content = log_file.read()

            # Show validation log in readable format
            st.markdown("### 📋 Validation Log")
            st.code(log_content, language="text")

            # Download button
            st.download_button(
                label="📥 Download Log File",
                data=log_content,
                file_name=os.path.basename(log_file_path),
                mime="text/plain"
            )

        except Exception as log_err:
            st.error(f"⚠️ Unable to read log file: {log_err}")

    except Exception as e:
        st.error(f"❌ Error during validation: {e}")

    finally:
        # Clean up temp files
        for f in [temp1_path, temp2_path]:
            if f and os.path.exists(f):
                os.remove(f)
