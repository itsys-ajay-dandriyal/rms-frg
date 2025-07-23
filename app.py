import streamlit as st
from validator import run_validation
import tempfile
import os

st.set_page_config(page_title="Data Validator", layout="centered")
st.title("📊 Dataset Comparison Validator")

# File upload
uploaded_file1 = st.file_uploader("Upload Current Dataset (CSV or TXT)", type=["csv", "txt"], key="file1")
uploaded_file2 = st.file_uploader("Upload Previous Dataset (CSV or TXT)", type=["csv", "txt"], key="file2")

# Country selection
country = st.selectbox("Select Country", ["USA", "India", "Germany", "UK", "CA"])

if uploaded_file1 and uploaded_file2 and st.button("Run Validation"):
    try:
        # Create temporary files to save uploads
        with tempfile.NamedTemporaryFile(delete=False, suffix='.' + uploaded_file1.name.split('.')[-1]) as temp1, \
             tempfile.NamedTemporaryFile(delete=False, suffix='.' + uploaded_file2.name.split('.')[-1]) as temp2:
            
            temp1.write(uploaded_file1.read())
            temp2.write(uploaded_file2.read())
            temp1_path = temp1.name
            temp2_path = temp2.name

        st.info("Running validation... Please wait.")

        # Run validation
        log_file = run_validation(temp1_path, temp2_path, country)

        st.success("Validation completed. Showing log below:")

        # Read log file safely with encoding
        with open(log_file, "r", encoding="utf-8") as lf:
            log_content = lf.read()

        st.text_area("Validation Log", log_content, height=400)

        st.download_button(
            label="📥 Download Log File",
            data=log_content,
            file_name=os.path.basename(log_file),
            mime="text/plain"
        )

    except Exception as e:
        st.error(f"Error during validation: {e}")

    finally:
        # Cleanup temporary files
        for f in [temp1_path, temp2_path]:
            if os.path.exists(f):
                os.remove(f)
